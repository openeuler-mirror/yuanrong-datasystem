/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// This is deliberately a URMA-backed environment test. Keep protocol/state-machine checks in
// urma_send_lane_test.cpp, which must remain independent of the URMA SDK and ut_common.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <sys/mman.h>
#include <sys/wait.h>
#include <tbb/concurrent_hash_map.h>
#include <unistd.h>

#include <gtest/gtest.h>

#ifdef USE_URMA
#define private public
#include "datasystem/common/rdma/urma_manager.h"
#undef private

#include "datasystem/common/inject/inject_point.h"

DS_DECLARE_bool(enable_urma);
DS_DECLARE_uint32(urma_send_jetty_lane_pool_size);
DS_DECLARE_uint32(urma_send_jetty_lane_refill_extra_size);
DS_DECLARE_uint64(urma_max_write_size_mb);

namespace datasystem {
namespace {
constexpr uint32_t kPeerHandshakePort = 18081;
constexpr uint32_t kLocalHandshakePort = 18080;
constexpr uint64_t kPeerBufferSize = 2 * 1024 * 1024;
constexpr uint8_t kPayloadByte = 0x5A;
constexpr char kVerifyCommand = 'v';
constexpr char kStopCommand = 's';
constexpr auto kWaitTimeout = std::chrono::seconds(10);

bool WriteAll(int fd, const void *data, size_t size)
{
    const auto *cursor = static_cast<const uint8_t *>(data);
    while (size > 0) {
        const auto written = write(fd, cursor, size);
        if (written <= 0) {
            return false;
        }
        cursor += written;
        size -= static_cast<size_t>(written);
    }
    return true;
}

bool ReadAll(int fd, void *data, size_t size)
{
    auto *cursor = static_cast<uint8_t *>(data);
    while (size > 0) {
        const auto readSize = read(fd, cursor, size);
        if (readSize <= 0) {
            return false;
        }
        cursor += readSize;
        size -= static_cast<size_t>(readSize);
    }
    return true;
}

bool WaitUntil(const std::function<bool()> &predicate)
{
    const auto deadline = std::chrono::steady_clock::now() + kWaitTimeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return predicate();
}

// The bonding URMA provider registers physical segments. Match UrmaManager::InitMemoryBufferPool's allocation model
// instead of passing a malloc-backed std::vector whose base and length need not be page aligned.
class MmapBuffer {
public:
    explicit MmapBuffer(uint64_t size) : size_(size)
    {
        data_ = static_cast<uint8_t *>(mmap(nullptr, size_, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1,
                                            0));
        if (data_ == MAP_FAILED) {
            data_ = nullptr;
        }
    }

    ~MmapBuffer()
    {
        if (data_ != nullptr) {
            (void)munmap(data_, size_);
        }
    }

    MmapBuffer(const MmapBuffer &) = delete;
    MmapBuffer &operator=(const MmapBuffer &) = delete;

    bool IsValid() const
    {
        return data_ != nullptr;
    }

    uint8_t *Data() const
    {
        return data_;
    }

    uint64_t Size() const
    {
        return size_;
    }

private:
    uint8_t *data_ = nullptr;
    uint64_t size_ = 0;
};

class RemoteUrmaPeer {
public:
    RemoteUrmaPeer() = default;
    ~RemoteUrmaPeer()
    {
        Finish(kStopCommand, 0);
    }

    RemoteUrmaPeer(const RemoteUrmaPeer &) = delete;
    RemoteUrmaPeer &operator=(const RemoteUrmaPeer &) = delete;

    bool Start()
    {
        int handshakePipe[2] = { -1, -1 };
        int commandPipe[2] = { -1, -1 };
        if (pipe(handshakePipe) != 0 || pipe(commandPipe) != 0) {
            return false;
        }
        pid_ = fork();
        if (pid_ < 0) {
            close(handshakePipe[0]);
            close(handshakePipe[1]);
            close(commandPipe[0]);
            close(commandPipe[1]);
            return false;
        }
        if (pid_ == 0) {
            close(handshakePipe[0]);
            close(commandPipe[1]);
            const int result = RunPeer(handshakePipe[1], commandPipe[0]);
            close(handshakePipe[1]);
            close(commandPipe[0]);
            _exit(result);
        }
        close(handshakePipe[1]);
        close(commandPipe[0]);
        handshakeFd_ = handshakePipe[0];
        commandFd_ = commandPipe[1];
        return true;
    }

    bool ReadHandshake(UrmaHandshakeReqPb &handshake)
    {
        uint32_t size = 0;
        if (!ReadAll(handshakeFd_, &size, sizeof(size)) || size == 0 || size > kPeerBufferSize) {
            return false;
        }
        std::string serialized(size, '\0');
        if (!ReadAll(handshakeFd_, serialized.data(), serialized.size())) {
            return false;
        }
        return handshake.ParseFromString(serialized);
    }

    bool VerifyAndFinish(uint32_t expectedSize)
    {
        return Finish(kVerifyCommand, expectedSize);
    }

    bool StopAndFinish()
    {
        return Finish(kStopCommand, 0);
    }

private:
    struct PeerCommand {
        char type = kStopCommand;
        uint32_t expectedSize = 0;
    };

    static int RunPeer(int handshakeFd, int commandFd)
    {
        FLAGS_enable_urma = true;
        auto &manager = UrmaManager::Instance();
        if (manager.Init(HostPort("127.0.0.1", kPeerHandshakePort)).IsError()) {
            const uint32_t unavailable = 0;
            (void)WriteAll(handshakeFd, &unavailable, sizeof(unavailable));
            return 1;
        }

        MmapBuffer remoteBuffer(kPeerBufferSize);
        if (!remoteBuffer.IsValid()
            || manager.RegisterSegment(reinterpret_cast<uint64_t>(remoteBuffer.Data()), remoteBuffer.Size())
                   .IsError()) {
            const uint32_t unavailable = 0;
            (void)WriteAll(handshakeFd, &unavailable, sizeof(unavailable));
            return 1;
        }

        uint32_t peerJettyId = 0;
        if (manager.GetOrCreateLocalJetty("remote-jetty-reuse-parent", peerJettyId, JettyType::RECV).IsError()) {
            const uint32_t unavailable = 0;
            (void)WriteAll(handshakeFd, &unavailable, sizeof(unavailable));
            return 1;
        }
        std::shared_ptr<UrmaJetty> peerJetty;
        if (manager.GetLocalJetty("remote-jetty-reuse-parent", peerJetty, JettyType::RECV).IsError()) {
            const uint32_t unavailable = 0;
            (void)WriteAll(handshakeFd, &unavailable, sizeof(unavailable));
            return 1;
        }

        UrmaHandshakeReqPb handshake;
        auto peerInfo = manager.GetLocalUrmaInfo();
        peerInfo.jfrId = peerJettyId;
        urma_rjetty_t *rjetty = nullptr;
        uint32_t rjettySize = 0;
        if (ds_urma_get_rjetty(peerJetty->Raw(), &rjetty, &rjettySize) == URMA_SUCCESS && rjetty != nullptr
            && rjettySize > 0) {
            peerInfo.rjettyBuf.assign(reinterpret_cast<const char *>(rjetty), rjettySize);
            ds_urma_put_rjetty(rjetty);
        }
        peerInfo.ToProto(handshake);
        if (manager.GetSegmentInfo(handshake).IsError() || handshake.seg_infos_size() != 1) {
            const uint32_t unavailable = 0;
            (void)WriteAll(handshakeFd, &unavailable, sizeof(unavailable));
            return 1;
        }

        std::string serialized;
        if (!handshake.SerializeToString(&serialized)) {
            return 1;
        }
        const auto size = static_cast<uint32_t>(serialized.size());
        if (!WriteAll(handshakeFd, &size, sizeof(size))
            || !WriteAll(handshakeFd, serialized.data(), serialized.size())) {
            return 1;
        }

        PeerCommand command;
        if (!ReadAll(commandFd, &command, sizeof(command))) {
            return 1;
        }
        if (command.type != kVerifyCommand || command.expectedSize > remoteBuffer.Size()) {
            return command.type == kStopCommand ? 0 : 1;
        }
        return std::all_of(remoteBuffer.Data(), remoteBuffer.Data() + command.expectedSize,
                           [](uint8_t value) { return value == kPayloadByte; })
                   ? 0
                   : 1;
    }

    bool Finish(char type, uint32_t expectedSize)
    {
        if (pid_ <= 0) {
            return false;
        }
        const PeerCommand command{ type, expectedSize };
        const bool wroteCommand = commandFd_ >= 0 && WriteAll(commandFd_, &command, sizeof(command));
        if (commandFd_ >= 0) {
            close(commandFd_);
            commandFd_ = -1;
        }
        if (handshakeFd_ >= 0) {
            close(handshakeFd_);
            handshakeFd_ = -1;
        }
        int status = 0;
        const bool waited = waitpid(pid_, &status, 0) == pid_;
        pid_ = -1;
        return wroteCommand && waited && WIFEXITED(status) && WEXITSTATUS(status) == 0;
    }

    pid_t pid_ = -1;
    int handshakeFd_ = -1;
    int commandFd_ = -1;
};

UrmaRemoteAddrPb BuildRemoteAddress(const UrmaHandshakeReqPb &handshake)
{
    UrmaRemoteAddrPb remote;
    remote.set_seg_va(handshake.seg_infos(0).seg().va());
    remote.set_seg_data_offset(0);
    remote.mutable_request_address()->set_host(handshake.address().host());
    remote.mutable_request_address()->set_port(handshake.address().port());
    return remote;
}

Status SubmitMultiChunkWrite(UrmaManager &manager, const UrmaRemoteAddrPb &remote, const MmapBuffer &payload,
                             uint64_t size, std::vector<uint64_t> &eventKeys)
{
    return manager.UrmaWritePayload(remote, reinterpret_cast<uint64_t>(payload.Data()), payload.Size(),
                                    reinterpret_cast<uint64_t>(payload.Data()), 0, size, 0, INVALID_CHIP_ID,
                                    INVALID_CHIP_ID, false, eventKeys);
}

void ConfigureSingleLaneEnvironmentTest()
{
    FLAGS_enable_urma = true;
    FLAGS_urma_max_write_size_mb = 1;
    FLAGS_urma_send_jetty_lane_pool_size = 1;
    FLAGS_urma_send_jetty_lane_refill_extra_size = 0;
}

TEST(UrmaRemoteJettyReuseTest, RepeatedExchangeReusesSharedRecvJetty)
{
    if (std::getenv("DS_URMA_DEV_NAME") == nullptr) {
        GTEST_SKIP() << "URMA environment test requires DS_URMA_DEV_NAME and a usable local URMA device.";
    }

    ConfigureSingleLaneEnvironmentTest();

    // This case only verifies the repeated out-of-band exchange contract. The peer supplies a real registered
    // segment and RECV Jetty; no WR is posted here so a failure localizes to shared RECV Jetty publication.
    RemoteUrmaPeer peer;
    ASSERT_TRUE(peer.Start());
    UrmaHandshakeReqPb peerHandshake;
    if (!peer.ReadHandshake(peerHandshake)) {
        GTEST_SKIP() << "Remote URMA peer could not initialize; check DS_URMA_DEV_NAME and the URMA runtime.";
    }
    ASSERT_EQ(peerHandshake.seg_infos_size(), 1);

    auto &manager = UrmaManager::Instance();
    ASSERT_TRUE(manager.Init(HostPort("127.0.0.1", kLocalHandshakePort)).IsOk());

    // Exercise the server-side out-of-band exchange twice. ExchangeJfr imports the peer Jetty/segment and publishes
    // this process's shared RECV Jetty in its response. A reconnect must not create a second RECV Jetty: both the
    // stable Jetty ID and delegated rjetty context returned to the peer must be identical.
    UrmaHandshakeRspPb firstExchangeRsp;
    ASSERT_TRUE(manager.ExchangeJfr(peerHandshake, firstExchangeRsp).IsOk());
    ASSERT_TRUE(firstExchangeRsp.has_hand_shake());
    const auto &firstLocalInfo = firstExchangeRsp.hand_shake();
    ASSERT_EQ(firstLocalInfo.jfr_ids_size(), 1);
    ASSERT_TRUE(firstLocalInfo.has_rjetty_info());
    ASSERT_FALSE(firstLocalInfo.rjetty_info().rjetty_blob().empty());

    UrmaHandshakeRspPb secondExchangeRsp;
    ASSERT_TRUE(manager.ExchangeJfr(peerHandshake, secondExchangeRsp).IsOk());
    ASSERT_TRUE(secondExchangeRsp.has_hand_shake());
    const auto &secondLocalInfo = secondExchangeRsp.hand_shake();
    ASSERT_EQ(secondLocalInfo.jfr_ids_size(), 1);
    EXPECT_EQ(secondLocalInfo.jfr_ids(0), firstLocalInfo.jfr_ids(0));
    EXPECT_EQ(secondLocalInfo.rjetty_info().rjetty_blob(), firstLocalInfo.rjetty_info().rjetty_blob());
    EXPECT_TRUE(peer.StopAndFinish());
}

TEST(UrmaRemoteJettyReuseTest, MultiChunkWriteSharesAndSettlesOneRemoteSendLane)
{
    if (std::getenv("DS_URMA_DEV_NAME") == nullptr) {
        GTEST_SKIP() << "URMA environment test requires DS_URMA_DEV_NAME and a usable local URMA device.";
    }

    // Keep exactly one send lane. The payload below is larger than max write size, so it must create two chunks:
    // a second acquire would exhaust the pool, while a request-scoped acquire lets both chunks use the same lane.
    ConfigureSingleLaneEnvironmentTest();

    // Use a forked peer with a real registered segment and RECV Jetty. This keeps the assertions on the
    // remote-Jetty/import/post-WR path rather than on a mock of the URMA SDK.
    RemoteUrmaPeer peer;
    ASSERT_TRUE(peer.Start());
    UrmaHandshakeReqPb peerHandshake;
    if (!peer.ReadHandshake(peerHandshake)) {
        GTEST_SKIP() << "Remote URMA peer could not initialize; check DS_URMA_DEV_NAME and the URMA runtime.";
    }
    ASSERT_EQ(peerHandshake.seg_infos_size(), 1);

    auto &manager = UrmaManager::Instance();
    ASSERT_TRUE(manager.Init(HostPort("127.0.0.1", kLocalHandshakePort)).IsOk());
    UrmaJfrInfo peerInfo;
    ASSERT_TRUE(peerInfo.FromProto(peerHandshake).IsOk());
    uint32_t localJettyId = 0;
    ASSERT_TRUE(manager.ImportRemoteJetty(peerInfo, localJettyId).IsOk());
    ASSERT_TRUE(manager.ImportRemoteInfo(peerHandshake).IsOk());

    const uint64_t maxWriteSize = manager.urmaResource_->GetMaxWriteSize();
    ASSERT_GT(maxWriteSize, 0u);
    const uint64_t transferSize = maxWriteSize + 64;
    ASSERT_LE(transferSize, kPeerBufferSize);
    MmapBuffer payload(kPeerBufferSize);
    ASSERT_TRUE(payload.IsValid());
    std::memset(payload.Data(), kPayloadByte, transferSize);
    const auto remote = BuildRemoteAddress(peerHandshake);

    // Pause the writer after its first post. Its CQE can arrive while the writer has not sealed the lease yet;
    // that completion may settle its event, but must not return the lane to the pool before Seal().
    ASSERT_TRUE(inject::Set("UrmaManager.ApplySendLaneAction.Release", "call()").IsOk());
    ASSERT_TRUE(inject::Set("UrmaManager.UrmaWriteAfterPost", "1*pause()").IsOk());
    manager.requestId_.store(1000);
    Status writeStatus = Status::OK();
    std::vector<uint64_t> eventKeys;
    std::thread writer([&] { writeStatus = SubmitMultiChunkWrite(manager, remote, payload, transferSize, eventKeys); });

    const bool postPaused = WaitUntil(
        [] { return inject::GetExecuteCount("UrmaManager.UrmaWriteAfterPost") == 1; });
    std::shared_ptr<UrmaEvent> firstEvent;
    const bool firstEventFound = postPaused && manager.GetEvent(1000, firstEvent).IsOk();
    const bool completionBeforeSeal = firstEventFound && WaitUntil([&] {
        return firstEvent->laneLease_->GetPendingEventCount() == 0 && !firstEvent->laneLease_->IsSettled();
    });
    const auto statsBeforeSeal = manager.urmaResource_->GetSendJettyPoolStats();

    EXPECT_TRUE(inject::Clear("UrmaManager.UrmaWriteAfterPost").IsOk());
    writer.join();
    EXPECT_TRUE(postPaused);
    EXPECT_TRUE(firstEventFound);
    EXPECT_TRUE(completionBeforeSeal);
    EXPECT_EQ(statsBeforeSeal.poolSize, 1u);
    EXPECT_EQ(statsBeforeSeal.idleCount, 0u) << "completion before Seal must not release the send lane";
    ASSERT_TRUE(writeStatus.IsOk()) << writeStatus.ToString();
    ASSERT_EQ(eventKeys.size(), 2u);

    // Both chunks belong to one request, so they must retain both the exact same lease and the same send Jetty.
    std::shared_ptr<UrmaEvent> secondEvent;
    ASSERT_TRUE(manager.GetEvent(eventKeys[0], firstEvent).IsOk());
    ASSERT_TRUE(manager.GetEvent(eventKeys[1], secondEvent).IsOk());
    ASSERT_NE(firstEvent->laneLease_, nullptr);
    EXPECT_EQ(firstEvent->laneLease_.get(), secondEvent->laneLease_.get());
    EXPECT_EQ(firstEvent->GetJetty().lock().get(), secondEvent->GetJetty().lock().get());
    for (const auto key : eventKeys) {
        ASSERT_TRUE(manager.WaitToFinish(key, 10000).IsOk());
    }
    // After both CQEs, the lane returns once. The injection count catches a double release even if the pool's
    // duplicate-release protection masks it in the idle count.
    const auto statsAfterSuccess = manager.urmaResource_->GetSendJettyPoolStats();
    EXPECT_EQ(statsAfterSuccess.poolSize, 1u);
    EXPECT_EQ(statsAfterSuccess.idleCount, 1u);
    EXPECT_EQ(inject::GetExecuteCount("UrmaManager.ApplySendLaneAction.Release"), 1u);

    // Fail the second chunk after the first has been posted. The lease must turn that request-level failure into one
    // retirement, even if the first completion has already arrived. Count both the final lease action and the
    // asynchronous modify-to-error submission to reject double retire paths.
    ASSERT_TRUE(inject::Set("UrmaManager.UrmaWriteError", "1*call()->1*return()").IsOk());
    ASSERT_TRUE(inject::Set("UrmaManager.ApplySendLaneAction.Retire", "call()").IsOk());
    ASSERT_TRUE(inject::Set("urma.ModifyJettyToError", "1*pause()").IsOk());
    manager.requestId_.store(2000);
    std::vector<uint64_t> failedEventKeys;
    const auto failureStatus = SubmitMultiChunkWrite(manager, remote, payload, transferSize, failedEventKeys);
    EXPECT_TRUE(failureStatus.IsError());
    EXPECT_TRUE(WaitUntil([] { return inject::GetExecuteCount("urma.ModifyJettyToError") == 1; }));
    const auto statsAfterFailure = manager.urmaResource_->GetSendJettyPoolStats();
    EXPECT_EQ(statsAfterFailure.poolSize, 0u);
    EXPECT_EQ(statsAfterFailure.idleCount, 0u);
    EXPECT_EQ(inject::GetExecuteCount("UrmaManager.ApplySendLaneAction.Retire"), 1u);
    EXPECT_EQ(inject::GetExecuteCount("urma.ModifyJettyToError"), 1u);
    EXPECT_TRUE(inject::Clear("urma.ModifyJettyToError").IsOk());
    EXPECT_TRUE(inject::Clear("UrmaManager.ApplySendLaneAction.Retire").IsOk());
    EXPECT_TRUE(inject::Clear("UrmaManager.UrmaWriteError").IsOk());
    EXPECT_TRUE(inject::Clear("UrmaManager.ApplySendLaneAction.Release").IsOk());

    EXPECT_TRUE(peer.VerifyAndFinish(static_cast<uint32_t>(transferSize)));
}

}  // namespace
}  // namespace datasystem

#else
TEST(UrmaRemoteJettyReuseTest, RequiresUrmaBuildConfiguration)
{
    GTEST_SKIP() << "Build this target with --config=urma.";
}
#endif
