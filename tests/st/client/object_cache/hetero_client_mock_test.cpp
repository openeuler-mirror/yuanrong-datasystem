/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Device object put to host test.
 */

#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "common.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/client/object_cache/client_worker_api.h"
#include "datasystem/hetero_client.h"
#include "datasystem/client/hetero_cache/device_buffer.h"
#include "datasystem/protos/p2p_subscribe.pb.h"

namespace datasystem {
namespace st {
using namespace datasystem::object_cache;
class HeteroClientMockTest : public ExternalClusterTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const int workerNum = 2;
        opts.numWorkers = workerNum;
        // -enable_huge_tlb=true -enable_fallocate=false
        opts.workerGflagParams =
            " -v=0  -authorization_enable=true -shared_memory_size_mb=2048 enable_fallocate=false -arena_per_tenant=2 "
            "-enable_fallocate=false";
        opts.enableDistributedMaster = "true";
        opts.numEtcd = 1;
        FLAGS_v = 0;
    }

    std::shared_ptr<ClientWorkerApi> CreateClientWorkerApi(int workerNum)
    {
        HostPort workerAddress;
        cluster_->GetWorkerAddr(workerNum, workerAddress);
        auto workerApi = std::make_shared<ClientWorkerApi>(workerAddress, RpcCredential(), HeartbeatType::RPC_HEARTBEAT,
                                                           signature_.get());
        workerApi->Init(CLIENT_RPC_TIMEOUT);
        return workerApi;
    }

protected:
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    std::unique_ptr<Signature> signature_ = std::make_unique<Signature>(accessKey_, secretKey_);
    static constexpr int CLIENT_RPC_TIMEOUT = 4 * 60 * 1000;
};

TEST_F(HeteroClientMockTest, TestSubscribeReceiveEvent)
{
    const int index1 = 0;
    const int index2 = 1;
    auto workerClient1 = CreateClientWorkerApi(index1);
    auto workerClient2 = CreateClientWorkerApi(index2);
    DS_ASSERT_OK(datasystem::inject::Set("SubscribeReceiveEvent.quicklyTimeout", "call(2000)"));
    // Test Local and remote func
    const int mockDevId = 1;
    SubscribeReceiveEventRspPb resp1;
    workerClient1->SubscribeReceiveEvent(mockDevId, resp1);
    SubscribeReceiveEventRspPb resp2;
    workerClient2->SubscribeReceiveEvent(mockDevId, resp2);
}

TEST_F(HeteroClientMockTest, TestGetP2PMeta)
{
    const int index1 = 0;
    const int index2 = 1;
    const int deviceIdx = 1;
    auto workerClient1 = CreateClientWorkerApi(index1);
    auto workerClient2 = CreateClientWorkerApi(index2);

    auto devObjKey = GetStringUuid();
    auto lifetime = LifetimeType::REFERENCE;
    auto bufferInfo = std::make_shared<DeviceBufferInfo>(devObjKey, deviceIdx, lifetime, true, TransferType::P2P);
    size_t dataSize = 1024;
    Blob info{ nullptr, dataSize };
    DS_ASSERT_OK(workerClient1->PutP2PMeta(bufferInfo, { info }));

    std::vector<std::shared_ptr<DeviceBufferInfo>> bufferInfoList;
    auto getBufferInfo = std::make_shared<DeviceBufferInfo>(devObjKey, deviceIdx, lifetime, true, TransferType::P2P);
    std::vector<DeviceBlobList> devBlobStorageList;
    Blob getInfo{ nullptr, dataSize };

    bufferInfoList.emplace_back(getBufferInfo);
    std::vector<Blob> listData;
    listData.emplace_back(getInfo);
    devBlobStorageList.emplace_back(DeviceBlobList{ listData, deviceIdx });

    GetP2PMetaRspPb resp;
    const int64_t timeoutMs = 1000;
    DS_ASSERT_OK(workerClient2->GetP2PMeta(bufferInfoList, devBlobStorageList, resp, timeoutMs));

    std::vector<Blob> blobs;
    DS_ASSERT_OK(workerClient1->GetBlobsInfo(devObjKey, timeoutMs, blobs));
    ASSERT_EQ(blobs.size(), 1);
    blobs.clear();
    DS_ASSERT_OK(workerClient2->GetBlobsInfo(devObjKey, timeoutMs, blobs));
    ASSERT_EQ(blobs.size(), 1);

    auto notExitId = GetStringUuid();
    auto notExitBufferInfo =
        std::make_shared<DeviceBufferInfo>(notExitId, deviceIdx, lifetime, true, TransferType::P2P);
    bufferInfoList.clear();
    bufferInfoList.emplace_back(notExitBufferInfo);
    DS_ASSERT_NOT_OK(workerClient2->GetP2PMeta(bufferInfoList, devBlobStorageList, resp, timeoutMs));
}

TEST_F(HeteroClientMockTest, TestRecvRootInfo)
{
    const int index1 = 0;

    auto workerClient1 = CreateClientWorkerApi(index1);
    auto workerClient2 = CreateClientWorkerApi(index1);
    auto deviceId = 1;
    auto localClientId1 = workerClient1->GetClientId();
    auto localClientId2 = workerClient2->GetClientId();
    char rootInfoInternal[11] = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', '\0' };

    SendRootInfoReqPb req;
    req.set_root_info(std::string(std::begin(rootInfoInternal), std::end(rootInfoInternal)));
    req.set_dst_client_id(localClientId1);
    req.set_dst_device_id(deviceId);
    req.set_src_client_id(localClientId2);
    req.set_src_device_id(deviceId);
    SendRootInfoRspPb resp;
    DS_ASSERT_OK(workerClient1->SendRootInfo(req, resp));

    RecvRootInfoReqPb rootInfoReq;
    rootInfoReq.set_dst_client_id(localClientId1);
    rootInfoReq.set_dst_device_id(deviceId);
    rootInfoReq.set_src_client_id(localClientId2);
    rootInfoReq.set_src_device_id(deviceId);

    RecvRootInfoRspPb rootInfoResp;
    DS_ASSERT_OK(workerClient2->RecvRootInfo(rootInfoReq, rootInfoResp));

    ASSERT_EQ(rootInfoResp.root_info().length(), sizeof(rootInfoInternal) / sizeof(rootInfoInternal[0]));
}

}  // namespace st
}  // namespace datasystem