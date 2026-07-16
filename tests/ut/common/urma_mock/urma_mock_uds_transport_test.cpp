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

/**
 * UDS transport UTs cover listener accept, SCM_RIGHTS memfd transfer,
 * and cross-thread HELLO / HELLO_ACK round trips.
 */
#include <gtest/gtest.h>

#include <dirent.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "datasystem/common/urma_mock/segment/segment_endpoint_registry.h"
#include "datasystem/common/urma_mock/segment/segment_identity.h"
#include "datasystem/common/urma_mock/transport/uds_endpoint_service.h"
#include "datasystem/common/urma_mock/transport/uds_transport.h"

using namespace datasystem::urma_mock;

namespace {

std::string MakeUniqueInstance()
{
    static std::atomic<int> seq{ 0 };
    return "uds_test_" + std::to_string(::getpid()) + "_" + std::to_string(seq.fetch_add(1));
}

int CountOpenFds()
{
    DIR *dir = ::opendir("/proc/self/fd");
    if (dir == nullptr) {
        return -1;
    }
    int count = 0;
    while (::readdir(dir) != nullptr) {
        ++count;
    }
    ::closedir(dir);
    return count;
}

void PackInvalidUdsHeader(uint8_t *header)
{
    constexpr uint32_t invalidMagic = 0x12345678U;
    std::memcpy(header, &invalidMagic, sizeof(invalidMagic));
    std::memset(header + sizeof(invalidMagic), 0, kUdsHeaderSize - sizeof(invalidMagic));
}

class UdsTransportTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        instance_ = MakeUniqueInstance();
        setenv("URMA_MOCK_UDS_INSTANCE", instance_.c_str(), 1);
        setenv("URMA_MOCK_UDS_BASE_DIR", "/tmp", 1);
    }
    void TearDown() override
    {
        UdsEndpointService::Instance().ShutdownListener();
        unsetenv("URMA_MOCK_UDS_INSTANCE");
        unsetenv("URMA_MOCK_UDS_BASE_DIR");
    }
    std::string instance_;
};

}  // namespace

TEST_F(UdsTransportTest, UdsSockSeqPacketAcceptReturnsPeer)
{
    // Bind/listen/accept returns a connected peer fd and the path is
    // resolvable from the env.
    auto path = ResolveUdsPath();
    EXPECT_FALSE(path.empty());
    UdsListener listener;
    ASSERT_TRUE(listener.Bind(path));
    EXPECT_EQ(listener.GetPath(), path);

    // Spin up accept in a thread, then connect from main.
    int peerFd = -1;
    std::thread acceptTh([&] { peerFd = listener.Accept(); });
    // Give the listener a moment to enter accept().
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    UdsConnection client;
    ASSERT_TRUE(client.Connect(path));
    acceptTh.join();
    EXPECT_GE(peerFd, 0);

    // Round-trip a header-only message.
    ASSERT_TRUE(client.Send(UdsMsgType::HELLO, 0, nullptr, 0, {}));
    uint8_t hdrBuf[kUdsHeaderSize];
    // Read 16B header from peer fd.
    ssize_t n = ::recv(peerFd, hdrBuf, kUdsHeaderSize, 0);
    EXPECT_EQ(n, static_cast<ssize_t>(kUdsHeaderSize));
    uint32_t magic = 0;
    UdsMsgType type = UdsMsgType::HELLO;
    uint16_t flags = 0;
    uint32_t payloadLen = 0;
    ASSERT_TRUE(UnpackUdsHeader(hdrBuf, &magic, &type, &flags, &payloadLen));
    EXPECT_EQ(magic, kUdsMagic);
    EXPECT_EQ(type, UdsMsgType::HELLO);
    EXPECT_EQ(payloadLen, 0u);

    ::close(peerFd);
}

TEST(UdsTransportDefaultPathTest, DefaultBaseDirUsesUserPrivateTmpDir)
{
    const char *oldBase = std::getenv("URMA_MOCK_UDS_BASE_DIR");
    const char *oldInstance = std::getenv("URMA_MOCK_UDS_INSTANCE");
    std::string savedBase = oldBase == nullptr ? std::string{} : std::string(oldBase);
    std::string savedInstance = oldInstance == nullptr ? std::string{} : std::string(oldInstance);

    unsetenv("URMA_MOCK_UDS_BASE_DIR");
    unsetenv("URMA_MOCK_UDS_INSTANCE");

    std::string path = ResolveUdsPath();
    std::string expectedPrefix =
        "/tmp/datasystem_urma_mock_" + std::to_string(static_cast<long long>(::getuid())) + "/instance_default/";
    EXPECT_EQ(path.find(expectedPrefix), 0u);
    EXPECT_EQ(path.rfind("/uds.sock"), path.size() - std::string("/uds.sock").size());

    if (oldBase != nullptr) {
        setenv("URMA_MOCK_UDS_BASE_DIR", savedBase.c_str(), 1);
    }
    if (oldInstance != nullptr) {
        setenv("URMA_MOCK_UDS_INSTANCE", savedInstance.c_str(), 1);
    }
}

TEST(UdsImportEndpointRegistryTest, LookupByRemoteVaDisambiguatesSameToken)
{
    constexpr uint64_t token = 44286;
    constexpr uint64_t va1 = 0x7f0010000000ULL;
    constexpr uint64_t va2 = 0x7f0020000000ULL;
    ImportEndpointRegistry::Instance().Clear();

    ImportEndpoint ep1;
    ep1.instanceId = "instance-1";
    ep1.va = va1;
    ImportEndpoint ep2;
    ep2.instanceId = "instance-2";
    ep2.va = va2;
    UdsEndpointService::Instance().RegisterImportEndpoint(token, ep1);
    UdsEndpointService::Instance().RegisterImportEndpoint(token, ep2);

    EXPECT_EQ(UdsEndpointService::Instance().LookupImportEndpoint(token, va1).instanceId, "instance-1");
    EXPECT_EQ(UdsEndpointService::Instance().LookupImportEndpoint(token, va2).instanceId, "instance-2");
    EXPECT_TRUE(UdsEndpointService::Instance().LookupImportEndpoint(token, va2 + 4096).instanceId.empty());
    EXPECT_EQ(ImportEndpointRegistry::Instance().Lookup(token).instanceId, "instance-2");

    ImportEndpointRegistry::Instance().Clear();
}

TEST_F(UdsTransportTest, SegmentEndpointUnregisterRemovesPublishedRecord)
{
    urma_seg_t seg{};
    seg.ubva.uasid = 17;
    seg.ubva.va = 0x7f0060000000ULL;
    seg.len = 4096;
    seg.ubva.eid.raw[0] = 1;
    constexpr uint64_t token = 0x510000;

    UdsEndpointService::Instance().EnsureListener();
    UdsEndpointService::Instance().RegisterSegmentEndpoint(seg, token);
    auto ep = UdsEndpointService::Instance().LookupSegmentEndpoint(seg, token);
    EXPECT_EQ(ep.instanceId, instance_);
    EXPECT_EQ(ep.va, seg.ubva.va);

    UdsEndpointService::Instance().UnregisterSegmentEndpoint(seg, token);
    EXPECT_TRUE(UdsEndpointService::Instance().LookupSegmentEndpoint(seg, token).instanceId.empty());
}

TEST_F(UdsTransportTest, SegmentEndpointLookupByTokenAndVaFindsLiveOwner)
{
    urma_seg_t staleSeg{};
    staleSeg.ubva.uasid = 17;
    staleSeg.ubva.va = 0x7f0061000000ULL;
    staleSeg.len = 4096;
    staleSeg.ubva.eid.raw[0] = 1;
    urma_seg_t liveSeg = staleSeg;
    liveSeg.ubva.eid.raw[0] = 2;
    constexpr uint64_t token = 0x510001;

    UdsEndpointService::Instance().EnsureListener();
    ImportEndpoint staleEp;
    staleEp.instanceId = "stale-instance";
    staleEp.va = staleSeg.ubva.va;
    staleEp.len = staleSeg.len;
    ASSERT_TRUE(SegmentEndpointRegistry::Instance().Register(BuildSegmentEndpointKey(staleSeg, token), staleEp));
    UdsEndpointService::Instance().RegisterSegmentEndpoint(liveSeg, token);

    auto ep = SegmentEndpointRegistry::Instance().Lookup(token, liveSeg.ubva.va);
    EXPECT_EQ(ep.instanceId, instance_);
    EXPECT_EQ(ep.va, liveSeg.ubva.va);

    UdsEndpointService::Instance().UnregisterSegmentEndpoint(liveSeg, token);
}

TEST_F(UdsTransportTest, UdsCmsgScmRightsDeliversMemfd)
{
    // Send a header + payload + memfd fd over SCM_RIGHTS; recv side must
    // get the same memfd (read/write same bytes).
    auto path = ResolveUdsPath();
    UdsListener listener;
    ASSERT_TRUE(listener.Bind(path));

    int peerFd = -1;
    std::thread acceptTh([&] { peerFd = listener.Accept(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    UdsConnection client;
    ASSERT_TRUE(client.Connect(path));
    acceptTh.join();
    ASSERT_GE(peerFd, 0);

    // Build a memfd, write payload, send it.
    int memfd = ::memfd_create("r91_payload", 0);
    ASSERT_GE(memfd, 0);
    const char *payload = "r91-cmsg-memfd-payload";
    const size_t kPayloadLen = std::strlen(payload);
    EXPECT_EQ(::ftruncate(memfd, static_cast<off_t>(kPayloadLen)), 0);
    ssize_t wrote = ::write(memfd, payload, kPayloadLen);
    EXPECT_EQ(wrote, static_cast<ssize_t>(kPayloadLen));
    ::lseek(memfd, 0, SEEK_SET);

    // Send header-only frame carrying the memfd via cmsg.
    ASSERT_TRUE(client.Send(UdsMsgType::POST_SEND, 0, nullptr, 0, { memfd }));

    // Receive: the peer fd should see a 16-byte header and an SCM_RIGHTS
    // cmsg with one fd. We use a UdsConnection adapter: dup peerFd into
    // a new UdsConnection by re-opening? Simpler: read raw msg with
    // recvmsg so we can verify cmsg presence.
    uint8_t hdrBuf[kUdsHeaderSize];
    std::vector<uint8_t> cmsgBuf(256);
    iovec iov;
    iov.iov_base = hdrBuf;
    iov.iov_len = kUdsHeaderSize;
    msghdr msg{};
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsgBuf.data();
    msg.msg_controllen = cmsgBuf.size();
    ssize_t n = ::recvmsg(peerFd, &msg, 0);
    EXPECT_EQ(n, static_cast<ssize_t>(kUdsHeaderSize));

    cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
    ASSERT_NE(cmsg, nullptr);
    EXPECT_EQ(cmsg->cmsg_level, SOL_SOCKET);
    EXPECT_EQ(cmsg->cmsg_type, SCM_RIGHTS);
    size_t fdCount = (cmsg->cmsg_len - CMSG_LEN(0)) / sizeof(int);
    EXPECT_EQ(fdCount, 1u);
    int deliveredFd;
    std::memcpy(&deliveredFd, CMSG_DATA(cmsg), sizeof(int));
    EXPECT_GE(deliveredFd, 0);

    // Read the payload through the delivered fd and verify.
    char readBuf[64] = { 0 };
    ::lseek(deliveredFd, 0, SEEK_SET);
    ssize_t rn = ::read(deliveredFd, readBuf, sizeof(readBuf) - 1);
    EXPECT_EQ(rn, static_cast<ssize_t>(kPayloadLen));
    EXPECT_STREQ(readBuf, payload);

    ::close(deliveredFd);
    ::close(peerFd);
    ::close(memfd);
}

TEST_F(UdsTransportTest, UdsSendRecvRoundTripAcrossThreads)
{
    // Two threads: client Send(HELLO) -> server Recv -> client Send(ACK)
    // -> server Recv. Server adopts the peer fd from accept().
    auto path = ResolveUdsPath();
    UdsListener listener;
    ASSERT_TRUE(listener.Bind(path));

    int peerFd = -1;
    std::thread acceptTh([&] { peerFd = listener.Accept(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    UdsConnection client;
    ASSERT_TRUE(client.Connect(path));
    acceptTh.join();
    ASSERT_GE(peerFd, 0);

    UdsConnection server;
    server.AdoptFd(peerFd);

    // Client sends HELLO first (server.Recv blocks until the datagram arrives).
    ASSERT_TRUE(client.Send(UdsMsgType::HELLO, 0, nullptr, 0, {}));

    UdsMsgType inType = UdsMsgType::HELLO;
    std::vector<uint8_t> inPayload;
    std::vector<int> inFds;
    int err = 0;
    ASSERT_TRUE(server.Recv(&inType, nullptr, &inPayload, &inFds, nullptr, &err)) << "server.Recv errno=" << err;
    EXPECT_EQ(inType, UdsMsgType::HELLO);

    // Client sends HELLO_ACK with payload.
    const char *ackPayload = "ack-payload-9";
    ASSERT_TRUE(client.Send(UdsMsgType::HELLO_ACK, 0, reinterpret_cast<const uint8_t *>(ackPayload),
                            std::strlen(ackPayload), {}));

    UdsMsgType outType = UdsMsgType::HELLO_ACK;
    std::vector<uint8_t> outPayload;
    std::vector<int> outFds;
    ASSERT_TRUE(server.Recv(&outType, nullptr, &outPayload, &outFds, nullptr, &err));
    EXPECT_EQ(outType, UdsMsgType::HELLO_ACK);
    EXPECT_EQ(outPayload.size(), std::strlen(ackPayload));
    EXPECT_EQ(std::memcmp(outPayload.data(), ackPayload, outPayload.size()), 0);
    EXPECT_TRUE(outFds.empty());
}

TEST_F(UdsTransportTest, InvalidFrameClosesReceivedScmRightsFd)
{
    int sockets[2] = { -1, -1 };
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_SEQPACKET, 0, sockets), 0);

    UdsConnection receiver;
    receiver.AdoptFd(sockets[1]);
    int memfd = ::memfd_create("invalid_frame_fd", 0);
    ASSERT_GE(memfd, 0);
    const int fdCountBefore = CountOpenFds();

    uint8_t header[kUdsHeaderSize];
    PackInvalidUdsHeader(header);
    iovec iov{};
    iov.iov_base = header;
    iov.iov_len = sizeof(header);
    std::vector<uint8_t> cmsgBuf(CMSG_SPACE(sizeof(int)));
    msghdr msg{};
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsgBuf.data();
    msg.msg_controllen = cmsgBuf.size();
    cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
    cmsg->cmsg_level = SOL_SOCKET;
    cmsg->cmsg_type = SCM_RIGHTS;
    cmsg->cmsg_len = CMSG_LEN(sizeof(int));
    std::memcpy(CMSG_DATA(cmsg), &memfd, sizeof(memfd));
    ASSERT_EQ(::sendmsg(sockets[0], &msg, 0), static_cast<ssize_t>(kUdsHeaderSize));

    UdsMsgType type = UdsMsgType::HELLO;
    std::vector<int> fds;
    int err = 0;
    EXPECT_FALSE(receiver.Recv(&type, nullptr, nullptr, &fds, nullptr, &err));
    EXPECT_EQ(err, EINVAL);
    EXPECT_TRUE(fds.empty());
    EXPECT_EQ(CountOpenFds(), fdCountBefore);

    ::close(memfd);
    ::close(sockets[0]);
}

TEST_F(UdsTransportTest, ShutdownListenerDoesNotWaitForeverForSilentPeer)
{
    UdsEndpointService::Instance().EnsureListener();
    auto path = ResolveUdsPath();
    UdsConnection client;
    ASSERT_TRUE(client.Connect(path));

    auto start = std::chrono::steady_clock::now();
    UdsEndpointService::Instance().ShutdownListener();
    auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
    EXPECT_LT(elapsedMs.count(), 3000);
}
