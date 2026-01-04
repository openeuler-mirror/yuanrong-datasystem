/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Test interface to master
 */
#include <vector>

#include "common.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"

#include "datasystem/common/util/thread_local.h"
#include "datasystem/master/object_cache/oc_metadata_manager.h"
#include "datasystem/master/object_cache/store/object_meta_store.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/strings_util.h"

using namespace datasystem::worker;
using namespace datasystem::master;
namespace datasystem {
namespace st {
class WorkerOCMasterTest : public ExternalClusterTest {
public:
    void SetUp() override
    {
        akSkManager_ = std::make_shared<AkSkManager>(0);
        akSkManager_->SetClientAkSk(accessKey_, secretKey_);
        ExternalClusterTest::SetUp();
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.query_meta.get_object_remote", "return()"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "master.query_meta.get_object_remote", "return()"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 2, "master.query_meta.get_object_remote", "return()"));
    }

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 3;
        opts.masterIdx = 2;
        opts.numEtcd = 1;
        std::string hostIp = "127.0.0.1";
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        for (auto addr : opts.workerConfigs) {
            workerAddress_.emplace(addr.ToString());
        }
    }

    std::shared_ptr<WorkerMasterOCApi> CreateClient(int workerIndex = 0)
    {
        HostPort metaAddress, localAddress;
        EXPECT_EQ(cluster_->GetMetaServerAddr(metaAddress), Status::OK());
        EXPECT_EQ(cluster_->GetWorkerAddr(workerIndex, localAddress), Status::OK());
        int stubCacheNum = 100;
        RpcStubCacheMgr::Instance().Init(stubCacheNum);
        auto client = WorkerMasterOCApi::CreateWorkerMasterOCApi(metaAddress, localAddress, akSkManager_);
        EXPECT_EQ(client->Init(), Status::OK());
        return client;
    }

    std::string GetWorkerAddr(int workerIndex = 0)
    {
        HostPort workerAddress;
        cluster_->GetWorkerAddr(workerIndex, workerAddress);
        return workerAddress.ToString();
    }

    void MakeObjectMetas(int num, std::unordered_map<std::string, ObjectMeta> &metas, int workerIndex = 0)
    {
        for (int i = 0; i < num; i++) {
            ObjectMeta objectMeta;
            objectMeta.meta.set_object_key(std::to_string(i));
            objectMeta.meta.set_data_size(1000);
            objectMeta.locations[GetWorkerAddr(workerIndex)] = AckState::ACK;
            metas.emplace(objectMeta.meta.object_key(), objectMeta);
        }
    }

    void MakeExistIds(std::list<std::string> &queryIds, const std::unordered_map<std::string, ObjectMeta> &inMetas)
    {
        size_t index = 0;
        for (const auto &meta : inMetas) {
            if (index % 2 == 0) {
                queryIds.emplace_back(meta.first);
            }
            index++;
        }
    }

    static int WorkerCreate(const std::unordered_map<std::string, ObjectMeta> &inMetas,
                            std::shared_ptr<WorkerMasterOCApi> client, bool isCopy = false)
    {
        int succeedCount = 0;
        Status status;
        for (const auto &meta : inMetas) {
            ObjectMeta metaInfo = meta.second;
            if (isCopy) {
                CreateCopyMetaReqPb req;
                CreateCopyMetaRspPb rsp;
                req.set_object_key(metaInfo.meta.object_key());
                req.set_address(metaInfo.locations.begin()->first);
                status = client->CreateCopyMeta(req, rsp);
            } else {
                CreateMetaReqPb req;
                CreateMetaRspPb rsp;
                req.set_address(metaInfo.locations.begin()->first);
                req.mutable_meta()->CopyFrom(metaInfo.meta);
                status = client->CreateMeta(req, rsp);
            }
            if (status.IsOk()) {
                succeedCount++;
            } else {
                LOG(ERROR) << status.ToString();
            }
        }
        return succeedCount;
    }

    Status WorkerQuery(const std::string &address, std::list<std::string> &queryIds,
                       std::unordered_map<std::string, ObjectMeta> &inMetas, std::shared_ptr<WorkerMasterOCApi> client,
                       int resultCount)
    {
        QueryMetaReqPb req;
        QueryMetaRspPb rsp;
        std::vector<RpcMessage> payloads;
        req.set_address(address);
        *req.mutable_ids() = { queryIds.begin(), queryIds.end() };
        RETURN_IF_NOT_OK(client->QueryMeta(req, 0, rsp, payloads));
        std::vector<master::QueryMetaInfoPb> outMetas = { rsp.mutable_query_metas()->begin(),
                                                          rsp.mutable_query_metas()->end() };
        for (const auto &outMeta : outMetas) {
            const auto &meta = outMeta.meta();
            std::string objectKey = meta.object_key();
            EXPECT_EQ(inMetas[objectKey].meta.object_key(), meta.object_key());
            EXPECT_EQ(inMetas[objectKey].meta.data_size(), meta.data_size());
            EXPECT_EQ(workerAddress_.count(outMeta.address()), 1ul);
            LOG(INFO) << "====id: " << objectKey << ", addr: " << VectorToString(workerAddress_)
                      << ", addr size: " << outMeta.address().size();
        }
        EXPECT_EQ(static_cast<size_t>(resultCount), outMetas.size());
        return Status::OK();
    };

    Status WorkerRemove(std::list<std::string> removeIds, std::shared_ptr<WorkerMasterOCApi> client,
                        std::string workerAddress)
    {
        master::DeleteAllCopyMetaReqPb deleteReq;
        master::DeleteAllCopyMetaRspPb resp;
        *deleteReq.mutable_object_keys() = { removeIds.begin(), removeIds.end() };
        deleteReq.set_address(workerAddress);
        RETURN_IF_NOT_OK(client->DeleteAllCopyMeta(deleteReq, resp));

        return Status::OK();
    }

private:
    static RandomData random_;
    std::set<std::string> workerAddress_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    std::shared_ptr<AkSkManager> akSkManager_;
};

RandomData WorkerOCMasterTest::random_;

TEST_F(WorkerOCMasterTest, TestCreateQueryRemoveMeta)
{
    int num = 5;
    std::unordered_map<std::string, ObjectMeta> inMetas;
    MakeObjectMetas(num, inMetas);
    auto client = CreateClient(0);
    std::string workerAddress = GetWorkerAddr(1);
    // Create
    int succeed = WorkerCreate(inMetas, client);
    EXPECT_EQ(succeed, num);
    // create same
    succeed = WorkerCreate(inMetas, client);
    EXPECT_EQ(succeed, num);

    // Query exist
    std::list<std::string> queryIds;
    MakeExistIds(queryIds, inMetas);
    EXPECT_EQ(WorkerQuery(workerAddress, queryIds, inMetas, client, queryIds.size()), Status::OK());
    // Query contain missing object
    queryIds.emplace_back("123456789");
    EXPECT_EQ(WorkerQuery(workerAddress, queryIds, inMetas, client, queryIds.size() - 1), Status::OK());
    // Remove exist
    EXPECT_EQ(WorkerRemove(queryIds, client, workerAddress), Status::OK());
    // Remove not exist
    EXPECT_EQ(WorkerRemove(queryIds, client, workerAddress), Status::OK());
    // Query after remove
    EXPECT_EQ(WorkerQuery(workerAddress, queryIds, inMetas, client, 0), Status::OK());
    // Query not exist object
    queryIds.clear();
    queryIds.emplace_back("123456789");
    EXPECT_EQ(WorkerQuery(workerAddress, queryIds, inMetas, client, 0), Status::OK());
}

TEST_F(WorkerOCMasterTest, TestCreateConcurrency)
{
    int num = 10;
    std::unordered_map<std::string, ObjectMeta> inMetas;
    MakeObjectMetas(num, inMetas);
    int threadNum = 20;
    std::atomic<int> okCount(0);
    std::vector<std::thread> clientThreads(threadNum);
    for (int i = 0; i < threadNum; ++i) {
        clientThreads[i] = std::thread([&okCount, &inMetas, this]() {
            auto client = CreateClient();
            int succeed = WorkerCreate(inMetas, client);
            okCount.fetch_add(succeed);
        });
    }

    for (auto &t : clientThreads) {
        t.join();
    }

    EXPECT_EQ(okCount, num * threadNum);
}

TEST_F(WorkerOCMasterTest, TestQueryConcurrency)
{
    // Prepare metas
    int num = 5;
    std::unordered_map<std::string, ObjectMeta> inMetas;
    MakeObjectMetas(num, inMetas);
    std::string workerAddress = GetWorkerAddr(1);
    auto initClient = CreateClient();
    EXPECT_EQ(WorkerCreate(inMetas, initClient), num);
    initClient.reset();

    // Query
    int threadNum = 10;
    std::vector<std::thread> clientThreads(threadNum);
    for (int i = 0; i < threadNum; ++i) {
        clientThreads[i] = std::thread([i, threadNum, &inMetas, this, workerAddress]() {
            auto client = CreateClient();
            std::list<std::string> queryIds;
            MakeExistIds(queryIds, inMetas);
            if (i < threadNum / 2) {
                EXPECT_EQ(WorkerQuery(workerAddress, queryIds, inMetas, client, queryIds.size()), Status::OK());
            } else {
                queryIds.emplace_back("****************");
                EXPECT_EQ(WorkerQuery(workerAddress, queryIds, inMetas, client, queryIds.size() - 1), Status::OK());
            }
        });
    }

    for (auto &t : clientThreads) {
        t.join();
    }
}

TEST_F(WorkerOCMasterTest, TestRemoveConcurrency)
{
    // Prepare metas
    int num = 5;
    std::unordered_map<std::string, ObjectMeta> inMetas;
    MakeObjectMetas(num, inMetas);
    auto initClient = CreateClient(0);
    auto workerAddress = GetWorkerAddr(0);
    WorkerCreate(inMetas, initClient);
    initClient.reset();
    // Remove
    int threadNum = 20;
    std::vector<std::thread> clientThreads(threadNum);
    for (int i = 0; i < threadNum; ++i) {
        clientThreads[i] = std::thread([&inMetas, this, &workerAddress]() {
            auto client = CreateClient();
            std::list<std::string> queryIds;
            MakeExistIds(queryIds, inMetas);
            EXPECT_EQ(WorkerRemove(queryIds, client, workerAddress), Status::OK());
        });
    }

    for (auto &t : clientThreads) {
        t.join();
    }
}

TEST_F(WorkerOCMasterTest, TestCopyCreateQueryRemoveMeta)
{
    int num = 5;
    std::unordered_map<std::string, ObjectMeta> inMetas;
    std::unordered_map<std::string, ObjectMeta> inMetas1;
    MakeObjectMetas(num, inMetas, 0);
    auto worker0_client = CreateClient(0);
    auto worker0Address = GetWorkerAddr(0);
    MakeObjectMetas(num, inMetas1, 1);
    auto worker1_client = CreateClient(1);
    auto worker1Address = GetWorkerAddr(1);
    auto worker2Address = GetWorkerAddr(2);
    // Worker0 create primary
    int succeed = WorkerCreate(inMetas, worker0_client, false);
    EXPECT_EQ(succeed, num);
    // Worker1 create copy
    int succeed1 = WorkerCreate(inMetas1, worker1_client, true);
    EXPECT_EQ(succeed1, num);
    // Worker0 create same primary
    succeed = WorkerCreate(inMetas, worker0_client, false);
    EXPECT_EQ(succeed, num);
    // Worker1 create same copy
    succeed1 = WorkerCreate(inMetas1, worker1_client, true);
    EXPECT_EQ(succeed1, num);
    // Query exist
    std::list<std::string> queryIds;
    MakeExistIds(queryIds, inMetas);
    EXPECT_EQ(WorkerQuery(worker2Address, queryIds, inMetas, worker0_client, queryIds.size()), Status::OK());
    EXPECT_EQ(WorkerQuery(worker2Address, queryIds, inMetas1, worker1_client, queryIds.size()), Status::OK());
    // Query contain missing object
    queryIds.emplace_back("123456789");
    EXPECT_EQ(WorkerQuery(worker2Address, queryIds, inMetas, worker0_client, queryIds.size() - 1), Status::OK());
    EXPECT_EQ(WorkerQuery(worker2Address, queryIds, inMetas1, worker1_client, queryIds.size() - 1), Status::OK());
    // Remove exist
    EXPECT_EQ(WorkerRemove(queryIds, worker0_client, worker0Address), Status::OK());
    EXPECT_EQ(WorkerRemove(queryIds, worker1_client, worker1Address), Status::OK());
    // Remove not exist
    EXPECT_EQ(WorkerRemove(queryIds, worker0_client, worker0Address), Status::OK());
    EXPECT_EQ(WorkerRemove(queryIds, worker1_client, worker1Address), Status::OK());
    // Query after remove
    EXPECT_EQ(WorkerQuery(worker2Address, queryIds, inMetas, worker0_client, 0), Status::OK());
    EXPECT_EQ(WorkerQuery(worker2Address, queryIds, inMetas1, worker1_client, 0), Status::OK());
    // Query not exist object
    queryIds.clear();
    queryIds.emplace_back("123456789");
    LOG(INFO) << "IDS: " << VectorToString(queryIds);
    EXPECT_EQ(WorkerQuery(worker2Address, queryIds, inMetas, worker0_client, 0), Status::OK());
    EXPECT_EQ(WorkerQuery(worker2Address, queryIds, inMetas1, worker1_client, 0), Status::OK());
}

TEST_F(WorkerOCMasterTest, LEVEL2_TestCreateQueryRemoveRestart)
{
    int num = 2;
    // in case of the first heartbeat send after master restart.
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 0));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 1));
    std::unordered_map<std::string, ObjectMeta> inMetas;
    std::unordered_map<std::string, ObjectMeta> inMetas1;
    MakeObjectMetas(num, inMetas, 0);
    auto worker0_client = CreateClient(0);
    auto worker0Address = GetWorkerAddr(0);
    MakeObjectMetas(num, inMetas1, 1);
    auto worker1_client = CreateClient(1);
    auto worker1Address = GetWorkerAddr(1);
    auto worker2Address = GetWorkerAddr(2);
    // Worker0 create primary
    int succeed = WorkerCreate(inMetas, worker0_client, false);
    EXPECT_EQ(succeed, num);
    // Worker1 create copy
    int succeed1 = WorkerCreate(inMetas1, worker1_client, true);
    EXPECT_EQ(succeed1, num);

    // Restart master
    cluster_->ShutdownNode(WORKER, 2);

    // Need to wait a bit before master hook up with a gcs node.
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 2, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 2));
    // Worker0 create same primary
    succeed = WorkerCreate(inMetas, worker0_client, false);
    EXPECT_EQ(succeed, num);
    // Worker1 create same copy
    succeed1 = WorkerCreate(inMetas1, worker1_client, true);
    EXPECT_EQ(succeed1, num);
    // Query exist
    std::list<std::string> queryIds;
    MakeExistIds(queryIds, inMetas);
    EXPECT_EQ(WorkerQuery(worker2Address, queryIds, inMetas, worker0_client, queryIds.size()), Status::OK());
    EXPECT_EQ(WorkerQuery(worker2Address, queryIds, inMetas1, worker1_client, queryIds.size()), Status::OK());
    // Remove exist
    EXPECT_EQ(WorkerRemove(queryIds, worker0_client, worker0Address), Status::OK());
    EXPECT_EQ(WorkerRemove(queryIds, worker1_client, worker1Address), Status::OK());
    // Restart master
    cluster_->ShutdownNode(WORKER, 2);
    // Need to wait a bit before master hook up with a gcs node.
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 2, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 2));
    // Query failed
    EXPECT_EQ(WorkerQuery(worker2Address, queryIds, inMetas, worker0_client, 0), Status::OK());
    EXPECT_EQ(WorkerQuery(worker2Address, queryIds, inMetas1, worker1_client, 0), Status::OK());
}

TEST_F(WorkerOCMasterTest, TestUpdate)
{
    auto client = CreateClient(0);
    ObjectMeta objectMeta;
    objectMeta.meta.set_object_key("abcd");
    objectMeta.meta.set_data_size(1);
    objectMeta.locations["127.0.0.1"] = AckState::ACK;
    UpdateMetaReqPb updateReq;
    UpdateMetaRspPb updateRsp;
    updateReq.set_object_key(objectMeta.meta.object_key());
    updateReq.set_address(objectMeta.locations.begin()->first);
    ASSERT_EQ(client->UpdateMeta(updateReq, updateRsp).GetCode(), StatusCode::K_NOT_FOUND);

    CreateMetaReqPb createReq;
    createReq.set_address(objectMeta.locations.begin()->first);
    createReq.mutable_meta()->CopyFrom(objectMeta.meta);
    CreateMetaRspPb createRsp;
    DS_ASSERT_OK(client->CreateMeta(createReq, createRsp));
    // master control version, can not control by worker
    DS_ASSERT_OK(client->UpdateMeta(updateReq, updateRsp));
}

TEST_F(WorkerOCMasterTest, TestWorkerTimeoutInterval)
{
    int num = 5;
    std::unordered_map<std::string, ObjectMeta> inMetas;
    std::unordered_map<std::string, ObjectMeta> inMetas1;
    MakeObjectMetas(num, inMetas, 0);
    auto worker0_client = CreateClient(0);
    auto worker0Address = GetWorkerAddr(0);
    MakeObjectMetas(num, inMetas1, 1);
    auto worker1_client = CreateClient(1);
    // Worker0 create primary
    int succeed = WorkerCreate(inMetas, worker0_client, false);
    EXPECT_EQ(succeed, num);

    inject::Set("rpc_util.retry_on_error_before_func", "return(K_RPC_UNAVAILABLE)");
    // 1.Construct master unavailable while CREATE, calculate the timeout interval meets the expectation
    reqTimeoutDuration.Init(5000);
    Timer timer;
    int succeed1 = WorkerCreate(inMetas1, worker1_client, true);
    EXPECT_NE(succeed1, num);
    auto timeCost = static_cast<uint64_t>(timer.ElapsedMilliSecond());
    LOG(INFO) << "time cast: " << timeCost;
    ASSERT_TRUE(timeCost > 4500 && timeCost <= 5000);

    // 2.Construct master unavailable while QUERY, calculate the timeout interval meets the expectation
    reqTimeoutDuration.Init(5000);
    timer.Reset();
    std::list<std::string> queryIds;
    EXPECT_NE(WorkerQuery(worker0Address, queryIds, inMetas, worker0_client, queryIds.size()), Status::OK());
    timeCost = static_cast<uint64_t>(timer.ElapsedMilliSecond());
    LOG(INFO) << "time cast: " << timeCost;
    ASSERT_TRUE(timeCost > 4000 && timeCost <= 4500);
}
}  // namespace st
}  // namespace datasystem
