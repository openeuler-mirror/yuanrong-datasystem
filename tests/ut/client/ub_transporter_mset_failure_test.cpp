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

/** Description: Focused UB write failure and TCP fallback policy tests. */

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "datasystem/client/object_cache/transport/data_plane/ub_connection.h"
#include "datasystem/client/object_cache/transport/data_plane/ub_transporter.h"
#include "datasystem/client/object_cache/transport/object_buffer_internal.h"
#include "datasystem/client/object_cache/transport/rpc/worker_rpc_client.h"
#include "datasystem/common/rpc/mem_view.h"
#include "datasystem/object/object_buffer.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {
namespace {

HostPort MakeAddress(int port)
{
    return HostPort("127.0.0.1", port);
}

TransportRequestContext MakeRequestContext()
{
    TransportRequestContext context;
    context.clientId = "client-1";
    context.tenantId = "tenant-1";
    return context;
}

TransportSetParam MakeSetParam(bool tcpFallbackDisabled = false)
{
    TransportSetParam param;
    param.requestContext = MakeRequestContext();
    param.tcpFallbackDisabled = tcpFallbackDisabled;
    return param;
}

std::shared_ptr<ObjectBuffer> MakeTransportBuffer(const HostPort &workerAddr, const std::string &key,
                                                  const std::string &data, const std::string &shmId)
{
    auto info = std::make_shared<ObjectBufferInfo>();
    info->objectKey = key;
    info->dataSize = data.size();
    info->metadataSize = 0;
    info->workerAddr = workerAddr;
    info->shmId = ShmKey::Intern(shmId);
    auto storage = std::make_shared<std::vector<uint8_t>>(data.size() + 1);
    info->pointer = storage->data();
    info->ubGetBufferHandle = std::static_pointer_cast<void>(storage);
    info->ubUrmaDataInfo = std::make_shared<UrmaRemoteAddrPb>();
    std::shared_ptr<ObjectBuffer> buffer;
    if (ObjectBufferInternal::Create(info, buffer).IsError()
        || buffer->MemoryCopy(data.data(), data.size()).IsError()) {
        return nullptr;
    }
    return buffer;
}

class FakeWorkerRpcClient : public WorkerRpcClient {
public:
    FakeWorkerRpcClient() : WorkerRpcClient(MakeAddress(9000), nullptr)
    {
    }

    Status InvokeSet(int64_t, PublishReqPb &, const std::vector<MemView> &payloads,
                     PublishRspPb &, uint32_t &) override
    {
        ++setInvokeCount;
        invokedPayloads.clear();
        for (const auto &payload : payloads) {
            invokedPayloads.emplace_back(static_cast<const char *>(payload.Data()), payload.Size());
        }
        return Status::OK();
    }

    Status InvokeMultiSet(int64_t, MultiPublishReqPb &request, const std::vector<MemView> &payloads,
                          MultiPublishRspPb &response, uint32_t &workerVersion) override
    {
        ++multiSetInvokeCount;
        publishedKeys.clear();
        for (const auto &info : request.object_info()) {
            publishedKeys.emplace_back(info.object_key());
        }
        invokedPayloads.clear();
        for (const auto &payload : payloads) {
            invokedPayloads.emplace_back(static_cast<const char *>(payload.Data()), payload.Size());
        }
        response.mutable_last_rc()->set_error_code(K_OK);
        workerVersion = 1;
        return Status::OK();
    }

    bool IsAlive() const override
    {
        return alive.load();
    }

    std::atomic<bool> alive{ true };
    int multiSetInvokeCount = 0;
    int setInvokeCount = 0;
    std::vector<std::string> publishedKeys;
    std::vector<std::string> invokedPayloads;
};

class FakeUbConnection : public UbConnection {
public:
    bool IsAlive() const override
    {
        return alive.load();
    }

    std::atomic<bool> alive{ true };
};

class TestUbTransporter : public UbTransporter {
public:
    TestUbTransporter(std::shared_ptr<WorkerRpcClient> rpcClient, std::shared_ptr<UbConnection> connection)
        : UbTransporter(std::move(rpcClient), std::move(connection))
    {
    }

    std::vector<Status> writeStatuses;
    std::vector<int> writeCqeStatuses;

protected:
    Status SubmitPayload(ObjectBufferInfo &, bool, std::vector<uint64_t> &eventKeys,
                         UrmaWriteFailure *failure) override
    {
        Status rc = writeStatuses.front();
        writeStatuses.erase(writeStatuses.begin());
        if (failure != nullptr && !writeCqeStatuses.empty()) {
            failure->cqeStatus = writeCqeStatuses.front();
            writeCqeStatuses.erase(writeCqeStatuses.begin());
        }
        if (rc.IsOk()) {
            eventKeys.emplace_back(1);
        }
        return rc;
    }

    Status WaitPayloadEvents(std::vector<uint64_t> &, UrmaWriteFailure *) override
    {
        return Status::OK();
    }
};

TEST(UbTransporterMSetFailureReportTest, ReportsHardUbFailureOverEarlierTimeout)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    TestUbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>());
    transporter.writeStatuses = {
        Status(K_URMA_ERROR, "first payload reported wait status"),
        Status(K_URMA_ERROR, "second payload reported error 4"),
    };
    transporter.writeCqeStatuses = { 9, 4 };
    const HostPort workerAddr = MakeAddress(9000);
    auto timeoutBuffer = MakeTransportBuffer(workerAddr, "timeout-key", "slow", "shm-timeout");
    auto hardFailureBuffer = MakeTransportBuffer(workerAddr, "hard-key", "hard", "shm-hard");
    ASSERT_NE(timeoutBuffer, nullptr);
    ASSERT_NE(hardFailureBuffer, nullptr);

    TransportMSetResult result;
    ASSERT_TRUE(transporter.MSet({ timeoutBuffer, hardFailureBuffer }, MakeSetParam(), result).IsOk());

    EXPECT_EQ(rpcClient->multiSetInvokeCount, 1);
    EXPECT_EQ(result.ubFailureReportRc.GetCode(), K_URMA_ERROR);
    EXPECT_EQ(result.ubFailureReportRc.GetMsg(), "second payload reported error 4");
    ASSERT_TRUE(result.ubCqeStatus.has_value());
    EXPECT_EQ(*result.ubCqeStatus, 4);
    ASSERT_EQ(rpcClient->invokedPayloads.size(), 2u);
    EXPECT_EQ(rpcClient->invokedPayloads[0], "slow");
    EXPECT_EQ(rpcClient->invokedPayloads[1], "hard");
}

TEST(UbTransporterFallbackPolicyTest, SetHonorsPolicyAndDoesNotPublishDeniedPayload)
{
    constexpr size_t PAYLOAD_SIZE = 512 * 1024;
    const std::string data(PAYLOAD_SIZE, 'x');
    for (bool disabled : { true, false }) {
        for (const auto &failure : { Status(K_URMA_ERROR, "cqe status: 9"),
                                     Status(K_URMA_ERROR, "post send failed") }) {
            auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
            TestUbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>());
            transporter.writeStatuses = { failure };
            auto buffer = MakeTransportBuffer(MakeAddress(9000), "set-key", data, "set-allocation");
            ASSERT_NE(buffer, nullptr);
            TransportSetResult result;
            auto rc = transporter.Set(*buffer, MakeSetParam(disabled), &result);
            EXPECT_EQ(rc.GetCode(), disabled ? K_URMA_ERROR : K_OK);
            EXPECT_EQ(result.publishAttempted, !disabled);
            EXPECT_EQ(rpcClient->setInvokeCount, disabled ? 0 : 1);
            if (!disabled) {
                ASSERT_EQ(rpcClient->invokedPayloads.size(), 1u);
                EXPECT_EQ(rpcClient->invokedPayloads.front(), data);
            }
        }
    }
}

TEST(UbTransporterFallbackPolicyTest, MSetWithDisabledFallbackPublishesOnlyUbSuccesses)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    TestUbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>());
    transporter.writeStatuses = { Status(K_URMA_ERROR, "cqe status: 9"),
                                   Status(K_URMA_ERROR, "post send failed"), Status::OK() };
    auto denied = MakeTransportBuffer(MakeAddress(9000), "denied", "denied-data", "allocation-a");
    auto alsoDenied = MakeTransportBuffer(MakeAddress(9000), "also-denied", "denied-data", "allocation-b");
    auto success = MakeTransportBuffer(MakeAddress(9000), "success", "success-data", "allocation-c");
    ASSERT_NE(denied, nullptr);
    ASSERT_NE(alsoDenied, nullptr);
    ASSERT_NE(success, nullptr);
    TransportMSetResult result;
    ASSERT_TRUE(transporter.MSet({ denied, alsoDenied, success }, MakeSetParam(true), result).IsOk());
    EXPECT_EQ(result.failedKeys, std::vector<std::string>({ "denied", "also-denied" }));
    EXPECT_EQ(result.lastRc.GetCode(), K_URMA_ERROR);
    EXPECT_EQ(rpcClient->publishedKeys, std::vector<std::string>({ "success" }));
    EXPECT_TRUE(rpcClient->invokedPayloads.empty());
}

TEST(UbTransporterFallbackPolicyTest, AllDeniedMSetNeverPublishes)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    TestUbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>());
    transporter.writeStatuses = { Status(K_URMA_ERROR, "post send failed") };
    auto buffer = MakeTransportBuffer(MakeAddress(9000), "denied", "data", "allocation");
    ASSERT_NE(buffer, nullptr);
    TransportMSetResult result;
    EXPECT_EQ(transporter.MSet({ buffer }, MakeSetParam(true), result).GetCode(), K_URMA_ERROR);
    EXPECT_FALSE(result.publishAttempted);
    EXPECT_EQ(rpcClient->multiSetInvokeCount, 0);
}

}  // namespace
}  // namespace client
}  // namespace datasystem
