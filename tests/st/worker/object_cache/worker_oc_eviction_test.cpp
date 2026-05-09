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
 * Description: Test EvictionManager.
 */
#include <fcntl.h>
#include <memory>
#include <vector>

#include "client/object_cache/oc_client_common.h"
#include "common.h"
#include "datasystem/client/mmap/embedded_mmap_table.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/object_cache/lock.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/util/queue/queue.h"
#include "datasystem/common/object_cache/safe_table.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/master/object_cache/store/object_meta_store.h"
#include "datasystem/object/buffer.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"
#include "datasystem/worker/object_cache/async_send_manager.h"
#define private public
#include "datasystem/worker/object_cache/obj_cache_shm_unit.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"
#include "datasystem/worker/object_cache/worker_worker_oc_api.h"
#include "datasystem/worker/object_cache/worker_oc_eviction_manager.h"
#undef private
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_crud_common_api.h"
#include "securec.h"

using namespace datasystem::object_cache;
using namespace datasystem::worker;
using namespace datasystem::master;

DS_DECLARE_string(spill_directory);
DS_DECLARE_uint64(spill_size_limit);
DS_DECLARE_string(master_address);
DS_DECLARE_string(etcd_address);
DS_DECLARE_bool(enable_distributed_master);
DS_DECLARE_string(shared_disk_directory);
DS_DECLARE_string(obs);
DS_DECLARE_string(obs_endpoint);
DS_DECLARE_string(obs_access_key);
DS_DECLARE_string(obs_secret_key);
DS_DECLARE_string(obs_bucket);
DS_DECLARE_uint32(l2_cache_async_write_rate_limit_mb);
DS_DECLARE_uint64(l2_cache_async_write_queue_size);

namespace datasystem {
namespace st {

static Status RetryCreate(std::shared_ptr<ObjectClient> client, const std::string &objectKey, uint64_t dataSize,
                          CreateParam param, std::shared_ptr<Buffer> &buffer)
{
    Status rc;
    do {
        rc = client->Create(objectKey, dataSize, param, buffer);
    } while (rc.GetCode() == K_OUT_OF_MEMORY);
    return rc;
}

static Status RetrySet(std::shared_ptr<KVClient> client, const std::string &objectKey, std::string &data,
                          SetParam param)
{
    Status rc;
    do {
        rc = client->Set(objectKey, data, param);
    } while (rc.GetCode() == K_OUT_OF_MEMORY);
    return rc;
}

static bool ExistsNone(std::vector<Optional<Buffer>> &buffers)
{
    return std::any_of(buffers.cbegin(), buffers.cend(), [](const Optional<Buffer> &buffer) { return !buffer; });
}

static Status RetryGet(std::shared_ptr<ObjectClient> client, const std::vector<std::string> &objectKeys,
                       int64_t timeout, std::vector<Optional<Buffer>> &buffers)
{
    Status rc;
    do {
        buffers.clear();
        rc = client->Get(objectKeys, timeout, buffers);
    } while (rc.IsOk() && ExistsNone(buffers));
    return rc;
}

static Status RetryPublish(Optional<Buffer> &buffer)
{
    Status rc;
    do {
        rc = buffer->Publish();
    } while (rc.GetCode() == K_OUT_OF_MEMORY);
    return rc;
}

static Status RetryPublish(std::shared_ptr<Buffer> &buffer)
{
    Status rc;
    do {
        rc = buffer->Publish();
    } while (rc.GetCode() == K_OUT_OF_MEMORY);
    return rc;
}

constexpr static int OBJECT_NUM = 100;
constexpr static int MOD = 10;
class EvictionManagerCommon {
public:
    using SafeObjType = SafeObject<ObjectInterface>;
    using ObjectTable = SafeTable<ImmutableString, ObjectInterface>;

    uint64_t GetMaxMemorySize()
    {
        return allocator->GetMaxMemorySize();
    }

    uint64_t GetAllocatedSize()
    {
        return allocator->GetTotalPhysicalMemoryUsage(datasystem::memory::CacheType::MEMORY);
    }

    void GetAllObjsFromObjectTable(std::unordered_map<std::string, std::shared_ptr<SafeObjType>> &res)
    {
        for (auto &meta : *objectTable_) {
            res[meta.first] = meta.second;
        }
    }

    uint64_t GetMetaSize(uint64_t dataSize)
    {
        const uint64_t defaultMetaSize = 10;
        return WorkerOcServiceCrudCommonApi::CanTransferByShm(dataSize) ? defaultMetaSize : 0;
    }

    Status CreateObject(const std::string &objectKey, uint64_t dataSize, WriteMode writeMode = WriteMode::NONE_L2_CACHE,
                        bool primaryCopy = true, bool spillState = false, DataFormat dataFormat = DataFormat::BINARY,
                        uint64_t version = 0)
    {
        CHECK_FAIL_RETURN_STATUS(!objectTable_->Contains(objectKey), StatusCode::K_DUPLICATED, "object exist");
        const uint64_t metaSize = GetMetaSize(dataSize);
        uint64_t needSize = dataSize + metaSize;

        // add object to objectsInfo_
        auto ptr = std::make_unique<object_cache::ObjCacheShmUnit>();
        auto shmUnit = std::make_shared<ShmUnit>();
        RETURN_IF_NOT_OK(shmUnit->AllocateMemory("", needSize, false));
        if (metaSize > 0) {
            auto ret = memset_s(shmUnit->GetPointer(), metaSize, 0, metaSize);
            if (ret != EOK) {
                RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR,
                                        FormatString("[ObjectKey %s] Memset failed, errno: %d", objectKey, ret));
            }
        }
        ptr->SetShmUnit(shmUnit);
        ptr->SetDataSize(dataSize);
        ptr->SetMetadataSize(metaSize);
        ptr->SetCreateTime(version);
        ptr->SetLifeState(ObjectLifeState::OBJECT_SEALED);

        ptr->modeInfo.SetWriteMode(writeMode);
        ptr->stateInfo.SetDataFormat(dataFormat);
        ptr->stateInfo.SetPrimaryCopy(primaryCopy);
        ptr->stateInfo.SetSpillState(spillState);

        objectTable_->Insert(objectKey, std::move(ptr));
        return Status::OK();
    }

    Status DeleteObject(const std::string &objectKey)
    {
        CHECK_FAIL_RETURN_STATUS(objectTable_->Contains(objectKey), StatusCode::K_NOT_FOUND, "object not exist");
        objectTable_->Erase(objectKey);
        return Status::OK();
    }

    std::shared_ptr<ObjectTable> &GetObjectTable()
    {
        return objectTable_;
    }

    ~EvictionManagerCommon()
    {
        if (allocator) {
            allocator->Shutdown();
        }
    }

    datasystem::memory::Allocator *allocator;
    std::shared_ptr<ObjectTable> objectTable_;
    const uint64_t maxMemorySize = 1 * 1024 * 1024 * 1024;
};

class EvictionManagerAndMasterTest : public ExternalClusterTest, public EvictionManagerCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 2;
        opts.numEtcd = 1;
        std::string hostIp = "127.0.0.1";
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        for (auto addr : opts.workerConfigs) {
            workerAddress_.emplace(addr.ToString());
        }
        opts.workerGflagParams = " -v=3";
        opts.injectActions = "worker.Spill.Sync:return()";
    }

    void SetUp() override
    {
        LOG_IF_ERROR(inject::Set("worker.Spill.Sync", "return()"), "set inject point failed");
        akSkManager_ = std::make_shared<AkSkManager>(0);
        DS_ASSERT_OK(akSkManager_->SetClientAkSk(accessKey_, secretKey_));
        ExternalClusterTest::SetUp();
        InitTest();
    }

    void TearDown() override
    {
        objectTable_.reset();
        cm_.reset();
        etcdStore_.reset();
    }

    std::shared_ptr<WorkerMasterOCApi> CreateClient(int workerIndex = 0)
    {
        HostPort metaAddress, localAddress;
        cluster_->GetMetaServerAddr(metaAddress);
        cluster_->GetWorkerAddr(workerIndex, localAddress);
        auto client = WorkerMasterOCApi::CreateWorkerMasterOCApi(metaAddress, localAddress, akSkManager_);
        client->Init();
        return client;
    }

    std::string GetWorkerAddr(int workerIndex = 0)
    {
        HostPort workerAddress;
        cluster_->GetWorkerAddr(workerIndex, workerAddress);
        return workerAddress.ToString();
    }

    Status QueryMetadata(std::shared_ptr<WorkerMasterOCApi> api, int workerIndex,
                         const std::vector<std::string> &objectKeys, datasystem::master::QueryMetaRspPb &rsp)
    {
        const std::string queryReqId = GetStringUuid();
        datasystem::master::QueryMetaReqPb req;
        std::vector<RpcMessage> payloads;
        *req.mutable_ids() = { objectKeys.begin(), objectKeys.end() };
        req.set_request_id(queryReqId);
        req.set_address(GetWorkerAddr(workerIndex));
        return api->QueryMeta(req, 0, rsp, payloads);
    }

    Status CreateMeta(const std::string &objectKey, int workerIndex, std::shared_ptr<WorkerMasterOCApi> client,
                      bool isCopy = false, uint64_t *version = nullptr)
    {
        if (isCopy) {
            CreateCopyMetaReqPb req;
            CreateCopyMetaRspPb rsp;
            req.set_object_key(objectKey);
            req.set_address(GetWorkerAddr(workerIndex));
            auto rc = client->CreateCopyMeta(req, rsp);
            if (rc.IsOk() && version != nullptr) {
                *version = rsp.version();
            }
            return rc;
        } else {
            auto metaPb = std::make_unique<ObjectMetaPb>();
            metaPb->set_object_key(objectKey);
            metaPb->set_data_size(1000);
            CreateMetaReqPb req;
            CreateMetaRspPb rsp;
            req.set_address(GetWorkerAddr(workerIndex));
            req.set_allocated_meta(metaPb.release());
            auto rc = client->CreateMeta(req, rsp);
            if (rc.IsOk() && version != nullptr) {
                *version = rsp.version();
            }
            return rc;
        }
    }

    Status GetFromRemote(const std::string &objKey, size_t offset, size_t size, std::shared_ptr<ShmUnit> &shmUnit)
    {
        std::shared_ptr<WorkerRemoteWorkerOCApi> workerStub;
        HostPort workerAddr;
        cluster_->GetWorkerAddr(0, workerAddr);
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CreateRemoteWorkerApi(workerAddr.ToString(), akSkManager_, workerStub),
                                         "Create remote worker api failed.");
        GetObjectRemoteReqPb reqPb;
        GetObjectRemoteRspPb rspPb;
        reqPb.set_object_key(objKey);
        reqPb.set_read_offset(offset);
        reqPb.set_read_size(size);
        std::unique_ptr<ClientUnaryWriterReader<GetObjectRemoteReqPb, GetObjectRemoteRspPb>> clientApi;
        RETURN_IF_NOT_OK(workerStub->GetObjectRemote(&clientApi));
        RETURN_IF_NOT_OK(workerStub->GetObjectRemoteWrite(clientApi, reqPb));
        RETURN_IF_NOT_OK(clientApi->Read(rspPb));
        auto payloadSz = static_cast<size_t>(rspPb.data_size());
        std::vector<RpcMessage> payloads;
        RETURN_IF_NOT_OK(
            AllocateMemoryForObject(objKey, payloadSz, GetMetaSize(payloadSz), false, eviction_, *shmUnit));
        void *dest = reinterpret_cast<uint8_t *>(shmUnit->GetPointer()) + GetMetaSize(payloadSz) + offset;
        if (clientApi->IsV2Client()) {
            RETURN_IF_NOT_OK(clientApi->ReceivePayload(dest, size));
        } else {
            shmUnit->id = ShmKey::Intern(GetStringUuid());

            RETURN_IF_NOT_OK(clientApi->ReceivePayload(payloads));
            size_t payloadLen = 0;
            std::vector<std::pair<const uint8_t *, uint64_t>> payloadData;
            for (const auto &msg : payloads) {
                payloadLen += msg.Size();
                payloadData.emplace_back(reinterpret_cast<const uint8_t *>(msg.Data()), msg.Size());
            }
            CHECK_FAIL_RETURN_STATUS(!(payloads.empty() || payloadLen == 0), K_INVALID,
                                     "Payload is null or no bytes to write.");
            RETURN_IF_NOT_OK(shmUnit->MemoryCopy(payloadData, memCpyThreadPool_, GetMetaSize(payloadSz) + offset));
        }
        return Status::OK();
    }

protected:
    void InitTest()
    {
        int stubCacheNum = 100;
        RpcStubCacheMgr::Instance().Init(stubCacheNum);
        HostPort metaAddr;
        DS_ASSERT_OK(cluster_->GetMetaServerAddr(metaAddr));
        metaAddr_ = metaAddr;
        FLAGS_master_address = metaAddr_.ToString();

        HostPort worker1Addr;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, worker1Addr));
        worker0Addr_ = worker1Addr;

        HostPort worker2Addr;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(1, worker2Addr));
        worker2Addr_ = worker2Addr;

        objectTable_ = std::make_shared<ObjectTable>();
        allocator = datasystem::memory::Allocator::Instance();
        allocator->Init(maxMemorySize);
        memCpyThreadPool_ = std::make_shared<ThreadPool>(1);
    }

    void InitClusterManager(const HostPort &workerId)
    {
        std::pair<HostPort, HostPort> addrs;
        cluster_->GetEtcdAddrs(0, addrs);
        FLAGS_etcd_address = addrs.first.ToString();
        etcdStore_ = std::make_unique<EtcdStore>(FLAGS_etcd_address);
        etcdStore_->Init();
        eviction_ = std::make_shared<object_cache::WorkerOcEvictionManager>(nullptr, workerId, workerId);
        worker_ = std::make_unique<WorkerOCServiceImpl>(workerId, workerId, nullptr, nullptr, eviction_, nullptr,
                                                        etcdStore_.get());
        cm_ = std::make_unique<EtcdClusterManager>(workerId, workerId, etcdStore_.get(), false);
        worker_->SetClusterManager(cm_.get());
        ClusterInfo clusterInfo;
        DS_ASSERT_OK(EtcdClusterManager::ConstructClusterInfoViaEtcd(etcdStore_.get(), clusterInfo));
        DS_ASSERT_OK(cm_->Init(clusterInfo));
        cm_->SetWorkerReady();
    }

    std::set<std::string> workerAddress_;
    HostPort metaAddr_;
    HostPort worker0Addr_;
    HostPort worker2Addr_;
    std::unique_ptr<WorkerOCServiceImpl> worker_;
    std::unique_ptr<EtcdClusterManager> cm_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    std::shared_ptr<AkSkManager> akSkManager_;
    std::shared_ptr<object_cache::WorkerOcEvictionManager> eviction_;
    std::unique_ptr<EtcdStore> etcdStore_;
    std::shared_ptr<ThreadPool> memCpyThreadPool_;
};

class AsyncSendManagerWriteSpeedTest : public EvictionManagerAndMasterTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 2;
        opts.numEtcd = 1;
        opts.numOBS = 1;
        std::string hostIp = "127.0.0.1";
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        for (auto addr : opts.workerConfigs) {
            workerAddress_.emplace(addr.ToString());
        }
        opts.workerGflagParams = " -v=3";
        opts.injectActions = "worker.Spill.Sync:return()";
    }
};

TEST_F(AsyncSendManagerWriteSpeedTest, TestAsyncWriteBigElement)
{
    HostPort obsAddr;
    DS_ASSERT_OK(cluster_->GetOBSAddr(0, obsAddr));
    FLAGS_l2_cache_type="obs";
    FLAGS_obs_endpoint=obsAddr.ToString();
    FLAGS_obs_access_key="3rtJpvkP4zowTDsx6XiE";
    FLAGS_obs_secret_key="SJx5Zecs7SL7I6Au9XpylG9LwPF29kMwIxisI5Xs";
    FLAGS_obs_bucket="test";
    FLAGS_l2_cache_async_write_rate_limit_mb = 1;
    FLAGS_l2_cache_async_write_queue_size = 10000;
    std::shared_ptr<ObjectTable> objectTable = GetObjectTable();
    InitClusterManager(worker0Addr_);
    std::shared_ptr<object_cache::WorkerOcEvictionManager> evictionManager =
        std::make_shared<object_cache::WorkerOcEvictionManager>(objectTable, worker0Addr_, metaAddr_, nullptr);
    evictionManager->SetClusterManager(cm_.get());
    DS_EXPECT_OK(evictionManager->Init(std::make_shared<ObjectGlobalRefTable<ClientKey>>(), akSkManager_));

    std::shared_ptr<PersistenceApi> api = PersistenceApi::CreateShared();
    DS_ASSERT_OK(api->Init());

    // Put
    std::shared_ptr<SafeObjType> entry;
    uint64_t dataSize = 1024 * 1024 * 5;
    std::string objectKey = GetStringUuid();
    DS_EXPECT_OK(CreateObject(objectKey, dataSize, WriteMode::WRITE_BACK_L2_CACHE, true));  // Async send object.
    DS_EXPECT_OK(objectTable_->Get(objectKey, entry));
    AsyncSendManager asyncMgr(api, evictionManager);
    asyncMgr.Init();
    std::future<datasystem::Status> future;
    Timer timer;
    asyncMgr.Add(objectKey, entry, future);
    auto status = future.get();
    ASSERT_GT(timer.ElapsedMilliSecond(), 4000);
    asyncMgr.Stop();
}

TEST_F(EvictionManagerAndMasterTest, TestEndLifeEvictionUsesAsyncDeleteAllCopyMeta)
{
    // END_LIFE eviction should delete the local object without waiting for remote DeleteNotification.
    FLAGS_spill_directory = "";
    std::shared_ptr<ObjectTable> objectTable = GetObjectTable();

    object_cache::WorkerOcEvictionManager evictionManager(objectTable, worker0Addr_, metaAddr_, nullptr);
    DS_ASSERT_OK(evictionManager.Init(std::make_shared<ObjectGlobalRefTable<ClientKey>>(), akSkManager_));

    // Inject to use master address directly for non-distributed master mode
    datasystem::inject::Set("WorkerOcEvictionManager.GetMetaAddressForObject",
                            FormatString("return(%s)", metaAddr_.ToString()));

    auto masterClient = CreateClient(0);
    const std::string objectKey = "end_life_async_delete";
    const uint64_t dataSize = 10 * 1024 * 1024;
    const uint64_t objectVersion = 1;
    DS_ASSERT_OK(CreateObject(objectKey, dataSize, WriteMode::NONE_L2_CACHE_EVICT, true, false, DataFormat::BINARY,
                              objectVersion));
    std::shared_ptr<SafeObjType> entry;
    DS_ASSERT_OK(objectTable_->Get(objectKey, entry));
    evictionManager.Add(objectKey);
    DS_ASSERT_OK(CreateMeta(objectKey, 0, masterClient));
    DS_ASSERT_OK(CreateMeta(objectKey, 1, masterClient, true));

    const int notifyTimeoutMs = 3000;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "MasterWorkerOCServiceImpl.DeleteNotification.retry",
                                           "return(K_RPC_CANCELLED)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "OCMetadataManager.NotifyWorkerDelete.timeoutMs",
                                           FormatString("return(%d)", notifyTimeoutMs)));

    const int fastEvictLimitMs = 1000;
    const int pollIntervalMs = 50;
    Timer timer;
    evictionManager.Evict(maxMemorySize, CacheType::MEMORY);
    bool removedFast = false;
    while (timer.ElapsedMilliSecond() < fastEvictLimitMs) {
        if (!objectTable_->Contains(objectKey)) {
            removedFast = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(pollIntervalMs));
    }
    ASSERT_TRUE(removedFast) << "END_LIFE eviction should not wait for synchronous DeleteNotification timeout.";
    ASSERT_FALSE(objectTable_->Contains(objectKey));
}

TEST_F(EvictionManagerAndMasterTest, TestEvictObjNotExistInObjTable)
{
    std::shared_ptr<ObjectTable> objectTable = GetObjectTable();
    object_cache::WorkerOcEvictionManager evictionManager(objectTable, worker0Addr_, metaAddr_);
    auto globalRefTable = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
    DS_EXPECT_OK(evictionManager.Init(globalRefTable, akSkManager_));

    std::string objectKey1 = "test";
    uint64_t dataSize1 = 800 * 1024 * 1024;
    DS_EXPECT_OK(CreateObject(objectKey1, dataSize1));
    std::shared_ptr<SafeObjType> entry;
    DS_EXPECT_OK(objectTable_->Get(objectKey1, entry));
    evictionManager.Add(objectKey1);

    std::string objectKey2 = "test2";
    uint64_t dataSize2 = 1 * 1024 * 1024;
    DS_EXPECT_OK(CreateObject(objectKey2, dataSize2));
    std::shared_ptr<SafeObjType> entry2;
    DS_EXPECT_OK(objectTable_->Get(objectKey2, entry2));
    evictionManager.Add(objectKey2);
    DS_EXPECT_OK(DeleteObject(objectKey2));  // Object2 will be freed
    // Now object2 only exist in EvictionList, not exist in ObjectTable.

    // Verify
    EvictionList::Node oldest;
    std::vector<EvictionList::Node> objsInList;
    std::unordered_map<std::string, std::shared_ptr<SafeObjType>> objsInTable;
    DS_EXPECT_OK(evictionManager.GetAllObjectsInfo(objsInList, oldest));
    constexpr size_t objNum = 2;
    ASSERT_EQ(objsInList.size(), size_t(objNum));
    GetAllObjsFromObjectTable(objsInTable);
    ASSERT_EQ(objsInTable.size(), size_t(1));

    evictionManager.Evict();
    sleep(5);
    objsInList.clear();
    objsInTable.clear();
    DS_EXPECT_OK(evictionManager.GetAllObjectsInfo(objsInList, oldest));
    ASSERT_EQ(objsInList.size(), size_t(1));
    GetAllObjsFromObjectTable(objsInTable);
    ASSERT_EQ(objsInTable.size(), size_t(1));
}

TEST_F(EvictionManagerAndMasterTest, TestEvictObjWriteThroughHashMap)
{
    std::shared_ptr<ObjectTable> objectTable = GetObjectTable();
    InitClusterManager(worker0Addr_);
    object_cache::WorkerOcEvictionManager evictionManager(objectTable, worker0Addr_, metaAddr_, nullptr);
    evictionManager.SetClusterManager(cm_.get());
    DS_EXPECT_OK(evictionManager.Init(std::make_shared<ObjectGlobalRefTable<ClientKey>>(), akSkManager_));

    // Put HashMap objects
    std::shared_ptr<SafeObjType> entry;
    for (int i = 0; GetAllocatedSize() < GetMaxMemorySize() * HIGH_WATER_FACTOR; i++) {
        HostPort metaAddress;
        cluster_->GetMetaServerAddr(metaAddress);
        uint64_t dataSize = 10 * 1024 * 1024;
        std::string objectKey = "key_" + std::to_string(i);
        DS_EXPECT_OK(
            CreateObject(objectKey, dataSize, WriteMode::WRITE_THROUGH_L2_CACHE, true, false, DataFormat::HASH_MAP));
        DS_EXPECT_OK(objectTable_->Get(objectKey, entry));
        evictionManager.Add(objectKey);
    }

    // Verify
    EvictionList::Node oldest;
    std::vector<EvictionList::Node> objsInList;
    std::unordered_map<std::string, std::shared_ptr<SafeObjType>> objsInTable;
    DS_EXPECT_OK(evictionManager.GetAllObjectsInfo(objsInList, oldest));
    GetAllObjsFromObjectTable(objsInTable);
    ASSERT_EQ(objsInList.size(), objsInTable.size());
    size_t alreadyPut = objsInList.size();

    // Evict, objects will be delete.
    evictionManager.Evict();
    sleep(5);
    ASSERT_TRUE(GetAllocatedSize() < GetMaxMemorySize() * LOW_WATER_FACTOR);

    // Verify
    objsInList.clear();
    objsInTable.clear();
    DS_EXPECT_OK(evictionManager.GetAllObjectsInfo(objsInList, oldest));
    GetAllObjsFromObjectTable(objsInTable);
    ASSERT_LT(objsInList.size(), alreadyPut);
    ASSERT_LT(objsInTable.size(), alreadyPut);
    ASSERT_EQ(objsInList.size(), objsInTable.size());

    // Put again
    for (int i = 0; GetAllocatedSize() < GetMaxMemorySize() * HIGH_WATER_FACTOR; i++) {
        HostPort metaAddress;
        cluster_->GetMetaServerAddr(metaAddress);
        uint64_t dataSize = 10 * 1024 * 1024;
        std::string objectKey = "aga_" + std::to_string(i);
        DS_EXPECT_OK(CreateObject(objectKey, dataSize, WriteMode::WRITE_THROUGH_L2_CACHE));
        DS_EXPECT_OK(objectTable_->Get(objectKey, entry));
        evictionManager.Add(objectKey);
    }

    // Verify
    objsInList.clear();
    objsInTable.clear();
    DS_EXPECT_OK(evictionManager.GetAllObjectsInfo(objsInList, oldest));
    GetAllObjsFromObjectTable(objsInTable);
    ASSERT_EQ(objsInList.size(), objsInTable.size());
    ASSERT_EQ(objsInList.size(), alreadyPut);
}

TEST_F(EvictionManagerAndMasterTest, TestEvictObjNotPrimaryCopy)
{
    std::shared_ptr<ObjectTable> objectTable = GetObjectTable();
    InitClusterManager(worker0Addr_);
    object_cache::WorkerOcEvictionManager evictionManager(objectTable, worker0Addr_, metaAddr_, nullptr);
    evictionManager.SetClusterManager(cm_.get());
    auto globalRefTable = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
    DS_EXPECT_OK(evictionManager.Init(globalRefTable, akSkManager_));

    // Put
    auto masterClient0 = CreateClient(0);
    auto masterClient1 = CreateClient(1);
    std::shared_ptr<SafeObjType> entry;
    for (int i = 0; GetAllocatedSize() < GetMaxMemorySize() * HIGH_WATER_FACTOR; i++) {
        HostPort metaAddress;
        cluster_->GetMetaServerAddr(metaAddress);
        uint64_t dataSize = 10 * 1024 * 1024;
        std::string objectKey = "key_" + std::to_string(i);
        DS_EXPECT_OK(CreateObject(objectKey, dataSize, WriteMode::NONE_L2_CACHE, false));
        DS_EXPECT_OK(objectTable_->Get(objectKey, entry));
        evictionManager.Add(objectKey);

        // Create meta from worker0
        DS_EXPECT_OK(CreateMeta(objectKey, 0, masterClient0));

        // Create copy meta from worker1
        DS_EXPECT_OK(CreateMeta(objectKey, 1, masterClient1, true));
    }

    // Verify
    EvictionList::Node oldest;
    std::vector<EvictionList::Node> objsInList;
    std::unordered_map<std::string, std::shared_ptr<SafeObjType>> objsInTable;
    DS_EXPECT_OK(evictionManager.GetAllObjectsInfo(objsInList, oldest));
    GetAllObjsFromObjectTable(objsInTable);
    ASSERT_EQ(objsInList.size(), objsInTable.size());
    size_t alreadyPut = objsInList.size();

    // Evict, objects will be delete.
    evictionManager.Evict();
    sleep(5);
    ASSERT_LE(GetAllocatedSize(), GetMaxMemorySize() * LOW_WATER_FACTOR);

    objsInList.clear();
    objsInTable.clear();
    DS_EXPECT_OK(evictionManager.GetAllObjectsInfo(objsInList, oldest));
    GetAllObjsFromObjectTable(objsInTable);
    ASSERT_LT(objsInList.size(), alreadyPut);   // Delete will erase objects from EvictionList
    ASSERT_LT(objsInTable.size(), alreadyPut);  // Delete will erase objects from ObjectTable
}

TEST_F(EvictionManagerAndMasterTest, TestBatchRemoveMetaForEviction)
{
    std::shared_ptr<ObjectTable> objectTable = GetObjectTable();
    const bool oldEnableDistributedMaster = FLAGS_enable_distributed_master;
    FLAGS_enable_distributed_master = false;
    Raii resetDistributedMaster([oldEnableDistributedMaster]() {
        FLAGS_enable_distributed_master = oldEnableDistributedMaster;
    });
    DS_ASSERT_OK(inject::Set("EtcdClusterManager.checkConnection", "return(K_OK)"));
    Raii clearCheckConnection([]() {
        (void)inject::Clear("EtcdClusterManager.checkConnection");
    });
    InitClusterManager(worker0Addr_);
    object_cache::WorkerOcEvictionManager evictionManager(objectTable, worker0Addr_, metaAddr_, nullptr);
    evictionManager.SetClusterManager(cm_.get());
    auto globalRefTable = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
    DS_ASSERT_OK(evictionManager.Init(globalRefTable, akSkManager_));

    auto masterClient0 = CreateClient(0);
    auto masterClient1 = CreateClient(1);
    const std::string objectKeySuffix = GetStringUuid();
    const std::string staleKey = "batch_remove_meta_stale_" + objectKeySuffix;
    const std::string currentKey = "batch_remove_meta_current_" + objectKeySuffix;
    constexpr uint64_t staleRemoveVersion = 1;
    uint64_t staleVersion = 0;
    uint64_t currentVersion = 0;
    DS_ASSERT_OK(CreateMeta(staleKey, 0, masterClient0, false, &staleVersion));
    DS_ASSERT_OK(CreateMeta(currentKey, 0, masterClient0, false, &currentVersion));
    DS_ASSERT_OK(CreateMeta(staleKey, 1, masterClient1, true));
    DS_ASSERT_OK(CreateMeta(currentKey, 1, masterClient1, true));
    ASSERT_GT(staleVersion, staleRemoveVersion);
    ASSERT_GT(currentVersion, 0ul);

    WorkerOcEvictionManager::EvictDeletedObjects deletedObjects = {
        { staleKey, staleRemoveVersion },
        { currentKey, currentVersion },
    };
    DS_ASSERT_OK(evictionManager.RemoveMetaFromMasterForEviction(deletedObjects));

    QueryMetaRspPb rsp;
    DS_ASSERT_OK(QueryMetadata(masterClient1, 1, { staleKey, currentKey }, rsp));
    ASSERT_EQ(rsp.query_metas_size(), 2);
    std::unordered_map<std::string, std::string> selectedAddresses;
    for (const auto &meta : rsp.query_metas()) {
        selectedAddresses.emplace(meta.meta().object_key(), meta.address());
    }
    ASSERT_EQ(selectedAddresses[staleKey], GetWorkerAddr(0));
    ASSERT_TRUE(selectedAddresses[currentKey].empty());
}

TEST_F(EvictionManagerAndMasterTest, TestBatchRemoveMetaForEvictionWithUnroutableKey)
{
    std::shared_ptr<ObjectTable> objectTable = GetObjectTable();
    const bool oldEnableDistributedMaster = FLAGS_enable_distributed_master;
    FLAGS_enable_distributed_master = false;
    Raii resetDistributedMaster([oldEnableDistributedMaster]() {
        FLAGS_enable_distributed_master = oldEnableDistributedMaster;
    });
    DS_ASSERT_OK(inject::Set("EtcdClusterManager.checkConnection", "return(K_OK)"));
    Raii clearCheckConnection([]() {
        (void)inject::Clear("EtcdClusterManager.checkConnection");
    });
    InitClusterManager(worker0Addr_);
    object_cache::WorkerOcEvictionManager evictionManager(objectTable, worker0Addr_, metaAddr_, nullptr);
    evictionManager.SetClusterManager(cm_.get());
    auto globalRefTable = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
    DS_ASSERT_OK(evictionManager.Init(globalRefTable, akSkManager_));

    auto masterClient0 = CreateClient(0);
    auto masterClient1 = CreateClient(1);
    const std::string objectKeySuffix = GetStringUuid();
    const std::string currentKey = "batch_remove_meta_mixed_current_" + objectKeySuffix;
    const std::string unroutableKey = "batch_remove_meta_mixed_unroutable_" + objectKeySuffix;
    uint64_t currentVersion = 0;
    constexpr uint64_t unroutableVersion = 1;
    DS_ASSERT_OK(CreateMeta(currentKey, 0, masterClient0, false, &currentVersion));
    DS_ASSERT_OK(CreateMeta(currentKey, 1, masterClient1, true));
    DS_ASSERT_OK(inject::Set("WorkerOcEvictionManager.RemoveMetaFromMasterForEviction.moveToEmptyMaster",
                             FormatString("call(%s)", unroutableKey)));
    Raii clearMoveToEmptyMaster([]() {
        (void)inject::Clear("WorkerOcEvictionManager.RemoveMetaFromMasterForEviction.moveToEmptyMaster");
    });

    WorkerOcEvictionManager::EvictDeletedObjects deletedObjects = {
        { currentKey, currentVersion },
        { unroutableKey, unroutableVersion },
    };
    DS_ASSERT_NOT_OK(evictionManager.RemoveMetaFromMasterForEviction(deletedObjects));
    ASSERT_EQ(deletedObjects.size(), 1ul);
    ASSERT_EQ(deletedObjects.count(unroutableKey), 1ul);
    ASSERT_EQ(deletedObjects[unroutableKey], unroutableVersion);

    QueryMetaRspPb rsp;
    DS_ASSERT_OK(QueryMetadata(masterClient1, 1, { currentKey }, rsp));
    ASSERT_EQ(rsp.query_metas_size(), 1);
    ASSERT_TRUE(rsp.query_metas(0).address().empty());
}

TEST_F(EvictionManagerAndMasterTest, TestEvictObjPrimaryCopy)
{
    FLAGS_spill_directory = "./spill_TestEvictObjPrimaryCopy";
    std::shared_ptr<ObjectTable> objectTable = GetObjectTable();
    object_cache::WorkerOcEvictionManager evictionManager(objectTable, worker0Addr_, metaAddr_);
    auto globalRefTable = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
    DS_EXPECT_OK(evictionManager.Init(globalRefTable, akSkManager_));

    // Put
    auto masterClient = CreateClient(0);
    std::shared_ptr<SafeObjType> entry;
    for (int i = 0; GetAllocatedSize() < GetMaxMemorySize() * HIGH_WATER_FACTOR; i++) {
        uint64_t dataSize = 10 * 1024 * 1024;
        HostPort metaAddress;
        cluster_->GetMetaServerAddr(metaAddress);
        std::string objectKey = "key_" + std::to_string(i);
        DS_EXPECT_OK(CreateObject(objectKey, dataSize, WriteMode::NONE_L2_CACHE, true));
        DS_EXPECT_OK(objectTable_->Get(objectKey, entry));
        evictionManager.Add(objectKey);

        // Create meta
        DS_EXPECT_OK(CreateMeta(objectKey, 0, masterClient));
    }

    // Verify
    EvictionList::Node oldest;
    std::vector<EvictionList::Node> objsInList;
    std::unordered_map<std::string, std::shared_ptr<SafeObjType>> objsInTable;
    DS_EXPECT_OK(evictionManager.GetAllObjectsInfo(objsInList, oldest));
    GetAllObjsFromObjectTable(objsInTable);
    ASSERT_EQ(objsInList.size(), objsInTable.size());
    size_t alreadyPut = objsInList.size();

    // Evict, objects will be spilled to disk.
    evictionManager.Evict();
    sleep(5);
    evictionManager.Evict();
    sleep(5);
    Timer timer;
    int timeoutS = 40;
    bool success = false;
    while (timer.ElapsedSecond() < timeoutS) {
        if (GetAllocatedSize() < GetMaxMemorySize() * LOW_WATER_FACTOR) {
            success = true;
            break;
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));  // sleep 1000 ms
        }
    }
    ASSERT_TRUE(success);

    objsInList.clear();
    objsInTable.clear();
    DS_EXPECT_OK(evictionManager.GetAllObjectsInfo(objsInList, oldest));
    GetAllObjsFromObjectTable(objsInTable);
    ASSERT_LT(objsInList.size(), alreadyPut);   // Spill will erase objects from EvictionList
    ASSERT_EQ(objsInTable.size(), alreadyPut);  // Spill will not erase objects from ObjectTable
}

TEST_F(EvictionManagerAndMasterTest, DISABLED_LEVEL1_TestEvictObjWithLock)
{
    FLAGS_spill_directory = "./spill_TestEvictObjWithLock";
    std::shared_ptr<ObjectTable> objectTable = GetObjectTable();
    object_cache::WorkerOcEvictionManager evictionManager(objectTable, worker0Addr_, metaAddr_);
    auto globalRefTable = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
    DS_EXPECT_OK(evictionManager.Init(globalRefTable, akSkManager_));
    // Put
    auto masterClient = CreateClient(0);
    std::vector<std::shared_ptr<SafeObjType>> entryList;
    std::shared_ptr<SafeObjType> entry;
    uint64_t dataSize = 10 * 1024 * 1024;
    for (int i = 0; GetAllocatedSize() < GetMaxMemorySize() * HIGH_WATER_FACTOR; i++) {
        HostPort metaAddress;
        cluster_->GetMetaServerAddr(metaAddress);
        std::string objectKey = "key_" + std::to_string(i);
        DS_EXPECT_OK(CreateObject(objectKey, dataSize, WriteMode::NONE_L2_CACHE, true));
        DS_EXPECT_OK(objectTable_->Get(objectKey, entry));
        evictionManager.Add(objectKey);

        // Create meta
        DS_EXPECT_OK(CreateMeta(objectKey, 0, masterClient));
        entryList.emplace_back(entry);
    }

    DS_ASSERT_OK(inject::Set("worker.SubmitSpillTask", "8*sleep(2000)"));
    DS_ASSERT_OK(inject::Set("worker.Evict", "call(0)"));
    DS_ASSERT_OK(inject::Set("worker.Spill", "return(50)"));

    auto metaSize = GetMetaSize(dataSize);
    std::vector<std::pair<std::shared_ptr<object_cache::ShmLock>, std::thread::id>> locks;
    const size_t lockTypeCount = 3;
    const uint64_t timeoutOneSec = 1000;
    std::thread t([&entryList, &locks, metaSize, timeoutOneSec] {
        // Lock before evict.
        std::this_thread::sleep_for(std::chrono::milliseconds(timeoutOneSec));
        for (size_t i = 0; i < entryList.size(); i++) {
            auto entry = entryList[i];
            auto lockType = i % lockTypeCount;
            if (lockType == 0) {
                DS_EXPECT_OK(entry->RLock());
            } else if (lockType == 1) {
                DS_EXPECT_OK(entry->WLock());
            } else {
                auto lockFrame = reinterpret_cast<uint32_t *>((*entry)->GetShmUnit()->GetPointer());
                auto lock = std::make_shared<object_cache::ShmLock>(lockFrame, metaSize, 0);
                DS_EXPECT_OK(lock->Init());
                DS_EXPECT_OK(lock->WLatch(timeoutOneSec));
                locks.emplace_back(lock, std::this_thread::get_id());
            }
        }
    });

    // Evict, objects will be spilled to disk.
    evictionManager.Evict();
    t.join();
    ASSERT_GE(GetAllocatedSize(), GetMaxMemorySize() * LOW_WATER_FACTOR);

    const int sleepTimeout = 5000;
    std::this_thread::sleep_for(std::chrono::milliseconds(sleepTimeout));
    for (size_t i = 0; i < entryList.size(); i++) {
        auto entry = entryList[i];
        auto lockType = i % lockTypeCount;
        if (lockType == 0) {
            entry->RUnlock();
        } else if (lockType == 1) {
            entry->WUnlock();
        }
    }
    for (auto &item : locks) {
        item.first->UnWLatch(item.second);
    }
    DS_ASSERT_OK(inject::Set("worker.SubmitSpillTask", "8*sleep(2000)"));
    const int64_t maxWaitTime = 30000;
    Timer timer;
    bool result = false;
    while (timer.ElapsedMilliSecond() < maxWaitTime && evictionManager.IsRunning()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(timeoutOneSec));
    }
    ASSERT_TRUE(!evictionManager.IsRunning());
    evictionManager.Evict();
    timer.Reset();
    while (timer.ElapsedMilliSecond() < maxWaitTime) {
        std::this_thread::sleep_for(std::chrono::milliseconds(timeoutOneSec));
        if (GetAllocatedSize() < GetMaxMemorySize() * LOW_WATER_FACTOR) {
            result = true;
            break;
        }
    }
    ASSERT_TRUE(result);
}

TEST_F(EvictionManagerAndMasterTest, TestSpillDisableWithoutL2Cache)
{
    FLAGS_spill_directory = "";
    std::shared_ptr<ObjectTable> objectTable = GetObjectTable();
    object_cache::WorkerOcEvictionManager evictionManager(objectTable, worker0Addr_, metaAddr_);
    auto globalRefTable = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
    DS_EXPECT_OK(evictionManager.Init(globalRefTable, akSkManager_));

    // Put
    std::shared_ptr<SafeObjType> entry;
    auto masterClient = CreateClient(0);
    for (int i = 0; GetAllocatedSize() < GetMaxMemorySize() * HIGH_WATER_FACTOR; i++) {
        uint64_t dataSize = 10 * 1024 * 1024;
        HostPort metaAddress;
        cluster_->GetMetaServerAddr(metaAddress);
        std::string objectKey = "key_" + std::to_string(i);
        DS_EXPECT_OK(CreateObject(objectKey, dataSize, WriteMode::NONE_L2_CACHE, true));
        DS_EXPECT_OK(objectTable_->Get(objectKey, entry));
        evictionManager.Add(objectKey);

        // Create meta
        DS_EXPECT_OK(CreateMeta(objectKey, 0, masterClient));
    }

    // Verify
    EvictionList::Node oldest;
    std::vector<EvictionList::Node> objsInList;
    std::unordered_map<std::string, std::shared_ptr<SafeObjType>> objsInTable;
    DS_EXPECT_OK(evictionManager.GetAllObjectsInfo(objsInList, oldest));
    GetAllObjsFromObjectTable(objsInTable);
    ASSERT_EQ(objsInList.size(), objsInTable.size());
    size_t alreadyPut = objsInList.size();

    // Evict, objects will be spilled to disk.
    evictionManager.Evict();
    sleep(5);
    ASSERT_GE(GetAllocatedSize(), GetMaxMemorySize() * HIGH_WATER_FACTOR);

    objsInList.clear();
    objsInTable.clear();
    DS_EXPECT_OK(evictionManager.GetAllObjectsInfo(objsInList, oldest));
    GetAllObjsFromObjectTable(objsInTable);
    ASSERT_EQ(objsInList.size(), alreadyPut);
    ASSERT_EQ(objsInTable.size(), alreadyPut);
}

TEST_F(EvictionManagerAndMasterTest, TestSpillDisableWithL2Cache)
{
    FLAGS_spill_directory = "";
    std::shared_ptr<ObjectTable> objectTable = GetObjectTable();
    InitClusterManager(worker0Addr_);
    object_cache::WorkerOcEvictionManager evictionManager(objectTable, worker0Addr_, metaAddr_, nullptr);
    evictionManager.SetClusterManager(cm_.get());
    auto globalRefTable = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
    DS_EXPECT_OK(evictionManager.Init(globalRefTable, akSkManager_));

    // Put
    std::shared_ptr<SafeObjType> entry;
    auto masterClient = CreateClient(0);
    for (int i = 0; GetAllocatedSize() < GetMaxMemorySize() * HIGH_WATER_FACTOR; i++) {
        uint64_t dataSize = 10 * 1024 * 1024;
        HostPort metaAddress;
        cluster_->GetMetaServerAddr(metaAddress);
        std::string objectKey = "key_" + std::to_string(i);
        DS_EXPECT_OK(CreateObject(objectKey, dataSize, WriteMode::WRITE_THROUGH_L2_CACHE, true));
        DS_EXPECT_OK(objectTable_->Get(objectKey, entry));
        evictionManager.Add(objectKey);

        // Create meta
        DS_EXPECT_OK(CreateMeta(objectKey, 0, masterClient));
    }

    // Verify
    EvictionList::Node oldest;
    std::vector<EvictionList::Node> objsInList;
    std::unordered_map<std::string, std::shared_ptr<SafeObjType>> objsInTable;
    DS_EXPECT_OK(evictionManager.GetAllObjectsInfo(objsInList, oldest));
    GetAllObjsFromObjectTable(objsInTable);
    ASSERT_EQ(objsInList.size(), objsInTable.size());
    size_t alreadyPut = objsInList.size();

    // Evict, objects will be spilled to disk.
    evictionManager.Evict();
    sleep(5);
    ASSERT_LE(GetAllocatedSize(), GetMaxMemorySize() * LOW_WATER_FACTOR);

    objsInList.clear();
    objsInTable.clear();
    DS_EXPECT_OK(evictionManager.GetAllObjectsInfo(objsInList, oldest));
    GetAllObjsFromObjectTable(objsInTable);
    ASSERT_LT(objsInList.size(), alreadyPut);
    ASSERT_LT(objsInTable.size(), alreadyPut);
    ASSERT_EQ(objsInList.size(), objsInTable.size());
}

TEST_F(EvictionManagerAndMasterTest, DISABLED_WriteBackDelayTest)
{
    FLAGS_spill_directory = "./spill_WriteBackDelayTest";

    constexpr size_t limit = 100;
    FLAGS_spill_size_limit = limit;
    std::shared_ptr<ObjectTable> objectTable = GetObjectTable();
    InitClusterManager(worker0Addr_);
    std::shared_ptr<WorkerOcEvictionManager> evictionManager =
        std::make_shared<WorkerOcEvictionManager>(objectTable, worker0Addr_, metaAddr_, nullptr);
    evictionManager->SetClusterManager(cm_.get());
    auto globalRefTable = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
    DS_EXPECT_OK(evictionManager->Init(globalRefTable, akSkManager_));

    std::shared_ptr<PersistenceApi> api = PersistenceApi::CreateShared();
    DS_ASSERT_OK(api->Init());
    AsyncSendManager asyncMgr(api, evictionManager);
    // Stop async send thread.
    datasystem::inject::Set("worker.before_pop_from_queue", "1000*return(K_OK)");

    // Put
    std::shared_ptr<SafeObjType> entry;
    auto masterClient = CreateClient(0);
    for (int i = 0; GetAllocatedSize() < GetMaxMemorySize() * HIGH_WATER_FACTOR; i++) {
        uint64_t dataSize = 10 * 1024 * 1024;
        HostPort metaAddress;
        cluster_->GetMetaServerAddr(metaAddress);
        std::string objectKey = "key_" + std::to_string(i);
        DS_EXPECT_OK(CreateObject(objectKey, dataSize, WriteMode::WRITE_BACK_L2_CACHE, true));  // Async send object.
        DS_EXPECT_OK(objectTable_->Get(objectKey, entry));
        evictionManager->Add(objectKey);

        // Create meta
        DS_EXPECT_OK(CreateMeta(objectKey, 0, masterClient));
    }

    // Verify
    EvictionList::Node oldest;
    std::vector<EvictionList::Node> objsInList;
    std::unordered_map<std::string, std::shared_ptr<SafeObjType>> objsInTable;
    DS_EXPECT_OK(evictionManager->GetAllObjectsInfo(objsInList, oldest));
    GetAllObjsFromObjectTable(objsInTable);
    ASSERT_EQ(objsInList.size(), objsInTable.size());
    size_t alreadyPut = objsInList.size();

    evictionManager->Evict();
    sleep(5);
    ASSERT_GE(GetAllocatedSize(), GetMaxMemorySize() * HIGH_WATER_FACTOR);  // No objects evicted.

    objsInList.clear();
    objsInTable.clear();
    DS_EXPECT_OK(evictionManager->GetAllObjectsInfo(objsInList, oldest));
    GetAllObjsFromObjectTable(objsInTable);
    ASSERT_EQ(objsInList.size(), alreadyPut);
    ASSERT_EQ(objsInTable.size(), alreadyPut);
    ASSERT_EQ(objsInList.size(), objsInTable.size());
}

TEST_F(EvictionManagerAndMasterTest, TestSpillSizeLimit)
{
    FLAGS_spill_directory = "./spill_TestSpillSizeLimit";
    constexpr size_t limit = 100 * 1024 * 1024;
    FLAGS_spill_size_limit = limit;
    std::shared_ptr<ObjectTable> objectTable = GetObjectTable();
    object_cache::WorkerOcEvictionManager evictionManager(objectTable, worker0Addr_, metaAddr_);
    auto globalRefTable = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
    DS_EXPECT_OK(evictionManager.Init(globalRefTable, akSkManager_));

    // Put
    auto masterClient = CreateClient(0);
    std::shared_ptr<SafeObjType> entry;
    for (int i = 0; GetAllocatedSize() < GetMaxMemorySize() * HIGH_WATER_FACTOR; i++) {
        uint64_t dataSize = 10 * 1024 * 1024;
        HostPort metaAddress;
        cluster_->GetMetaServerAddr(metaAddress);
        std::string objectKey = "key_" + std::to_string(i);
        DS_EXPECT_OK(CreateObject(objectKey, dataSize, WriteMode::NONE_L2_CACHE, true));
        DS_EXPECT_OK(objectTable_->Get(objectKey, entry));
        evictionManager.Add(objectKey);

        // Create meta
        DS_EXPECT_OK(CreateMeta(objectKey, 0, masterClient));
    }

    // Verify
    EvictionList::Node oldest;
    std::vector<EvictionList::Node> objsInList;
    std::unordered_map<std::string, std::shared_ptr<SafeObjType>> objsInTable;
    DS_EXPECT_OK(evictionManager.GetAllObjectsInfo(objsInList, oldest));
    GetAllObjsFromObjectTable(objsInTable);
    ASSERT_EQ(objsInList.size(), objsInTable.size());
    size_t alreadyPut = objsInList.size();

    // Evict, objects will be spilled to disk.
    evictionManager.Evict();
    auto sleepTime = 20;
    while (GetAllocatedSize() > GetMaxMemorySize() * LOW_WATER_FACTOR && sleepTime > 0) {
        sleepTime--;
        sleep(1);
    }
    ASSERT_LT(GetAllocatedSize(), GetMaxMemorySize() * LOW_WATER_FACTOR);
    objsInList.clear();
    objsInTable.clear();
    DS_EXPECT_OK(evictionManager.GetAllObjectsInfo(objsInList, oldest));
    GetAllObjsFromObjectTable(objsInTable);
    ASSERT_LT(objsInList.size(), alreadyPut);   // Spill will erase objects from EvictionList
    ASSERT_EQ(objsInTable.size(), alreadyPut);  // Spill will not erase objects from ObjectTable
}

TEST_F(EvictionManagerAndMasterTest, TestEvictObjPrimaryCopyAlreadySpilled)
{
    std::shared_ptr<ObjectTable> objectTable = GetObjectTable();
    object_cache::WorkerOcEvictionManager evictionManager(objectTable, worker0Addr_, metaAddr_);
    auto globalRefTable = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
    DS_EXPECT_OK(evictionManager.Init(globalRefTable, akSkManager_));

    // Put
    auto masterClient = CreateClient(0);
    std::shared_ptr<SafeObjType> entry;
    for (int i = 0; GetAllocatedSize() < GetMaxMemorySize() * HIGH_WATER_FACTOR; i++) {
        uint64_t dataSize = 10 * 1024 * 1024;
        HostPort metaAddress;
        cluster_->GetMetaServerAddr(metaAddress);
        std::string objectKey = "key_" + std::to_string(i);
        DS_EXPECT_OK(CreateObject(objectKey, dataSize, WriteMode::NONE_L2_CACHE, true, true));
        DS_EXPECT_OK(objectTable_->Get(objectKey, entry));
        evictionManager.Add(objectKey);

        // Create meta
        DS_EXPECT_OK(CreateMeta(objectKey, 0, masterClient));
    }

    // Verify
    EvictionList::Node oldest;
    std::vector<EvictionList::Node> objsInList;
    std::unordered_map<std::string, std::shared_ptr<SafeObjType>> objsInTable;
    DS_EXPECT_OK(evictionManager.GetAllObjectsInfo(objsInList, oldest));
    GetAllObjsFromObjectTable(objsInTable);
    ASSERT_EQ(objsInList.size(), objsInTable.size());
    size_t alreadyPut = objsInList.size();

    // Evict, objects will be free.
    evictionManager.Evict();
    sleep(5);
    ASSERT_LE(GetAllocatedSize(), GetMaxMemorySize() * LOW_WATER_FACTOR);

    objsInList.clear();
    objsInTable.clear();
    DS_EXPECT_OK(evictionManager.GetAllObjectsInfo(objsInList, oldest));
    GetAllObjsFromObjectTable(objsInTable);
    ASSERT_LT(objsInList.size(), alreadyPut);
    ASSERT_EQ(objsInTable.size(), alreadyPut);
}

TEST_F(EvictionManagerAndMasterTest, TestEvictObjConcurrently1111)
{
    FLAGS_spill_directory = "spill_TestEvictObjConcurrently";
    std::shared_ptr<ObjectTable> objectTable = GetObjectTable();
    object_cache::WorkerOcEvictionManager evictionManager(objectTable, worker0Addr_, metaAddr_);
    auto globalRefTable = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
    DS_EXPECT_OK(evictionManager.Init(globalRefTable, akSkManager_));

    // Put
    auto masterClient = CreateClient(0);
    std::shared_ptr<SafeObjType> entry;
    for (int i = 0; GetAllocatedSize() < GetMaxMemorySize() * HIGH_WATER_FACTOR; i++) {
        uint64_t dataSize = 10 * 1024 * 1024;
        HostPort metaAddress;
        cluster_->GetMetaServerAddr(metaAddress);
        std::string objectKey = "key_" + std::to_string(i);
        DS_EXPECT_OK(CreateObject(objectKey, dataSize, WriteMode::NONE_L2_CACHE, true));
        DS_EXPECT_OK(objectTable_->Get(objectKey, entry));
        evictionManager.Add(objectKey);

        // Create meta
        DS_EXPECT_OK(CreateMeta(objectKey, 0, masterClient));
    }

    // Verify
    EvictionList::Node oldest;
    std::vector<EvictionList::Node> objsInList;
    std::unordered_map<std::string, std::shared_ptr<SafeObjType>> objsInTable;
    DS_EXPECT_OK(evictionManager.GetAllObjectsInfo(objsInList, oldest));
    GetAllObjsFromObjectTable(objsInTable);
    ASSERT_EQ(objsInList.size(), objsInTable.size());
    size_t alreadyPut = objsInList.size();

    // Evict, objects will be spilled to disk.
    constexpr size_t poolNum = 2;
    ThreadPool threadPool(poolNum);
    std::vector<std::future<void>> futures;
    for (int i = 0; i < 2; i++) {
        futures.emplace_back(threadPool.Submit([&evictionManager]() {
            evictionManager.Evict();
            sleep(5);
            evictionManager.Evict();
            sleep(5);
        }));
    }
    for (auto &fut : futures) {
        fut.get();
    }
    Timer timer;
    int timeoutS = 40;
    bool success = false;
    while (timer.ElapsedSecond() < timeoutS) {
        if (GetAllocatedSize() < GetMaxMemorySize() * LOW_WATER_FACTOR) {
            success = true;
            break;
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));  // sleep 1000 ms
        }
    }
    ASSERT_TRUE(success);

    objsInList.clear();
    objsInTable.clear();
    DS_EXPECT_OK(evictionManager.GetAllObjectsInfo(objsInList, oldest));
    GetAllObjsFromObjectTable(objsInTable);
    ASSERT_LT(objsInList.size(), alreadyPut);
    ASSERT_EQ(objsInTable.size(), alreadyPut);
}

class EvictionManagerSaveToRedisTest : public OCClientCommon {
public:
    std::vector<std::string> workerAddress_;

    void SetClusterSetupOptions(ExternalClusterOptions &opts)
    {
        const int WORKER_NUMS = 2;
        opts.numWorkers = WORKER_NUMS;
        opts.numEtcd = 1;
        std::string hostIp = "127.0.0.1";
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        for (auto addr : opts.workerConfigs) {
            workerAddress_.emplace_back(addr.ToString());
        }
        opts.workerGflagParams = " -shared_memory_size_mb=10 ";
        opts.numOBS = 1;
    }
};

TEST_F(EvictionManagerSaveToRedisTest, TestEvictWriteThroughObj)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    std::shared_ptr<KVClient> client;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    InitTestKVClient(0, client);
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    HostPort metaAddress;
    cluster_->GetMetaServerAddr(metaAddress);

    // Allocator just remain 1024*1024 bytes for extent allocation.
    int objNum = 15;
    uint64_t totalSize = 1024 * 1024;
    uint64_t metadataSize = 4;
    uint64_t dataSize = totalSize - metadataSize;
    std::string data(dataSize, 'x');
    for (int i = 0; i < objNum; i++) {
        // Put exceed shared_memory_size_mb, will trigger evict
        std::string objectKey = "key_" + std::to_string(i);
        DS_ASSERT_OK(RetrySet(client, objectKey, data, param));

        // Remote get exceed shared_memory_size_mb, will trigger evict
        std::vector<Optional<Buffer>> buffers;
        DS_ASSERT_OK(RetryGet(client2, { objectKey }, 0, buffers));
        ASSERT_TRUE(buffers.size() == 1);
        auto &bufferGet1 = *buffers[0];
        ASSERT_EQ(data, std::string(reinterpret_cast<const char *>(bufferGet1.ImmutableData()), bufferGet1.GetSize()));
    }

    // Get from local , remote disk or redis
    for (int i = 0; i < objNum; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        std::vector<Optional<Buffer>> buffers1;
        DS_ASSERT_OK(RetryGet(client1, { objectKey }, 0, buffers1));
        ASSERT_EQ(buffers1.size(), size_t(1));
        auto &bufferGet1 = *buffers1[0];
        ASSERT_EQ(data, std::string(reinterpret_cast<const char *>(bufferGet1.ImmutableData()), bufferGet1.GetSize()));

        std::vector<Optional<Buffer>> buffers2;
        DS_ASSERT_OK(RetryGet(client2, { objectKey }, 0, buffers2));
        ASSERT_EQ(buffers2.size(), size_t(1));
        auto &bufferGet2 = *buffers2[0];
        ASSERT_EQ(data, std::string(reinterpret_cast<const char *>(bufferGet2.ImmutableData()), bufferGet2.GetSize()));
    }
}

class EvictionManagerEndToEndTest : public OCClientCommon {
public:
    std::vector<std::string> workerAddress_;

    void SetClusterSetupOptions(ExternalClusterOptions &opts)
    {
        opts.numWorkers = 2;
        opts.numEtcd = 1;
        opts.enableSpill = true;
        std::string hostIp = "127.0.0.1";
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        for (auto addr : opts.workerConfigs) {
            workerAddress_.emplace_back(addr.ToString());
        }
        opts.workerGflagParams = " -shared_memory_size_mb=10 -v=1 ";
        opts.numOBS = 1;
        opts.injectActions = "worker.Spill.Sync:return()";
    }

    void MultiClientsGet(int objectNum, std::vector<std::shared_ptr<ObjectClient>> &objectClients)
    {
        size_t objNumPerClient = objectNum / objectClients.size();
        ThreadPool threadPool(objectClients.size());
        std::vector<std::future<void>> futures;
        for (size_t i = 0; i < objectClients.size(); i++) {
            auto &client = objectClients[i];
            std::vector<std::string> getObjList;
            for (size_t idx = 0; idx < objNumPerClient; idx++) {
                getObjList.emplace_back("key_" + std::to_string(idx + i * objNumPerClient));
            }
            auto getBatchObjs = [getObjList, &client, objNumPerClient]() {
                size_t objNumPerGet = 10;
                for (size_t i = 0; i < objNumPerClient / objNumPerGet; i++) {
                    auto start = getObjList.begin() + i * objNumPerGet;
                    std::vector<std::string> objectKeys(start, start + objNumPerGet);
                    std::vector<Optional<Buffer>> buffers;
                    ASSERT_EQ(RetryGet(client, objectKeys, 0, buffers), Status::OK());
                }
            };
            futures.emplace_back(threadPool.Submit(getBatchObjs));
        }
        for (auto &fut : futures) {
            fut.get();
        }
    }
};

TEST_F(EvictionManagerEndToEndTest, MultiClientsLocalSpillGetSuccess)
{
    std::shared_ptr<ObjectClient> client0;
    InitTestClient(0, client0);
    uint64_t dataSize = 170 * 1024;
    HostPort metaAddress;
    cluster_->GetMetaServerAddr(metaAddress);

    // Put, spill will be triggered
    CreateParam param;
    int objectNum = 800;
    for (int i = 0; i < objectNum; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        std::string data(dataSize, std::to_string(i % MOD)[0]);
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(RetryCreate(client0, objectKey, dataSize, param, buffer));
        DS_ASSERT_OK(buffer->MemoryCopy(const_cast<char *>(data.data()), dataSize));
        DS_ASSERT_OK(buffer->Seal());
    }

    // init clients
    int getClientNum = 8;
    std::vector<std::shared_ptr<ObjectClient>> objectClients(getClientNum);
    for (int i = 0; i < getClientNum; i++) {
        InitTestClient(0, objectClients[i]);
    }

    // Multi clients get
    MultiClientsGet(objectNum, objectClients);
}

TEST_F(EvictionManagerEndToEndTest, LEVEL2_TestEvictWriteThroughObj)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    std::shared_ptr<KVClient> client;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    InitTestKVClient(0, client);
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    HostPort metaAddress;
    cluster_->GetMetaServerAddr(metaAddress);

    // Allocator just remain 1024*1024 bytes for extent allocation.
    int objNum = 15;
    uint64_t totalSize = 1024 * 1024;
    uint64_t metadataSize = 4;
    uint64_t dataSize = totalSize - metadataSize;
    std::string data(dataSize, 'x');
    for (int i = 0; i < objNum; i++) {
        // Put exceed shared_memory_size_mb, will trigger evict
        std::string objectKey = "key_" + std::to_string(i);
        DS_ASSERT_OK(RetrySet(client, objectKey, data, param));

        // Remote get exceed shared_memory_size_mb, will trigger evict
        std::vector<Optional<Buffer>> buffers;
        DS_ASSERT_OK(RetryGet(client2, { objectKey }, 0, buffers));
        ASSERT_TRUE(buffers.size() == 1);
        auto &bufferGet1 = *buffers[0];
        ASSERT_EQ(data, std::string(reinterpret_cast<const char *>(bufferGet1.ImmutableData()), bufferGet1.GetSize()));
    }

    // Get from local or remote disk
    for (int i = 0; i < objNum; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        std::vector<Optional<Buffer>> buffers1;
        DS_ASSERT_OK(RetryGet(client1, { objectKey }, 0, buffers1));
        ASSERT_EQ(buffers1.size(), size_t(1));
        auto &bufferGet1 = *buffers1[0];
        ASSERT_EQ(data, std::string(reinterpret_cast<const char *>(bufferGet1.ImmutableData()), bufferGet1.GetSize()));

        std::vector<Optional<Buffer>> buffers2;
        DS_ASSERT_OK(RetryGet(client2, { objectKey }, 0, buffers2));
        ASSERT_EQ(buffers2.size(), size_t(1));
        auto &bufferGet2 = *buffers2[0];
        ASSERT_EQ(data, std::string(reinterpret_cast<const char *>(bufferGet2.ImmutableData()), bufferGet2.GetSize()));
    }
}

TEST_F(EvictionManagerEndToEndTest, LocalSetFailAndSpill)
{
    std::shared_ptr<ObjectClient> client0;
    InitTestClient(0, client0);
    uint64_t dataSize = 1 * 1024 * 1024;
    HostPort metaAddress;
    cluster_->GetMetaServerAddr(metaAddress);

    CreateParam param;
    std::string objectKey0 = "key_test_0";
    std::string data0(dataSize, 'l');
    std::shared_ptr<Buffer> buffer0;
    DS_ASSERT_OK(RetryCreate(client0, objectKey0, dataSize, param, buffer0));
    DS_ASSERT_OK(buffer0->MemoryCopy(const_cast<char *>(data0.data()), dataSize));
    DS_ASSERT_OK(buffer0->Publish());
    // second publish will fail, and trigger spill test
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.free_object_resource", "1*call()"));
    DS_ASSERT_OK(buffer0->Publish());
    std::vector<Optional<Buffer>> buffers;

    constexpr int objNum = 10;
    for (int i = 0; i < objNum; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        std::string data(dataSize, std::to_string(i % MOD)[0]);
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(RetryCreate(client0, objectKey, dataSize, param, buffer));
        DS_ASSERT_OK(buffer->MemoryCopy(const_cast<char *>(data.data()), dataSize));
        DS_ASSERT_OK(buffer->Publish());
    }
}

TEST_F(EvictionManagerEndToEndTest, LocalSpillGetSuccess)
{
    std::shared_ptr<ObjectClient> client0;
    InitTestClient(0, client0);
    uint64_t dataSize = 500 * 1024;
    HostPort metaAddress;
    cluster_->GetMetaServerAddr(metaAddress);

    // Put, spill will be triggered
    CreateParam param;
    for (int i = 0; i < OBJECT_NUM; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        std::string data(dataSize, std::to_string(i % MOD)[0]);
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(RetryCreate(client0, objectKey, dataSize, param, buffer));
        DS_ASSERT_OK(buffer->MemoryCopy(const_cast<char *>(data.data()), dataSize));
        DS_ASSERT_OK(buffer->Seal());
    }

    // Get
    for (int i = 0; i < OBJECT_NUM; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        std::string data(dataSize, std::to_string(i % MOD)[0]);
        std::vector<Optional<Buffer>> buffer;
        DS_ASSERT_OK(RetryGet(client0, { objectKey }, 0, buffer));
        ASSERT_EQ(std::string((const char *)buffer[0]->ImmutableData(), buffer[0]->GetSize()), data);
    }
}

TEST_F(EvictionManagerEndToEndTest, LocalSpillRemoteGetSuccess)
{
    std::shared_ptr<ObjectClient> client0;
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client0);
    InitTestClient(1, client1);
    uint64_t dataSize = 500 * 1024;
    CreateParam param;
    HostPort metaAddress;
    cluster_->GetMetaServerAddr(metaAddress);

    // worker0 put
    for (int i = 0; i < OBJECT_NUM; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        std::string data(dataSize, std::to_string(i % MOD)[0]);
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(RetryCreate(client0, objectKey, dataSize, param, buffer));
        DS_ASSERT_OK(buffer->MemoryCopy(const_cast<char *>(data.data()), dataSize));
        DS_ASSERT_OK(buffer->Seal());
    }

    // worker1 get
    for (int j = 0; j < OBJECT_NUM; j++) {
        std::string objectKey = "key_" + std::to_string(j);
        std::string data(dataSize, std::to_string(j % MOD)[0]);
        std::vector<Optional<Buffer>> buffer;
        DS_ASSERT_OK(RetryGet(client1, { objectKey }, 0, buffer));
        ASSERT_EQ(std::string((const char *)buffer[0]->ImmutableData(), buffer[0]->GetSize()), data);
    }
}

TEST_F(EvictionManagerEndToEndTest, LocalSpillRemoteGetSuccess2)
{
    std::shared_ptr<ObjectClient> client0;
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client0);
    InitTestClient(1, client1);
    uint64_t dataSize = 500 * 1024;
    CreateParam param;
    HostPort metaAddress;
    cluster_->GetMetaServerAddr(metaAddress);

    // worker0 put
    for (int i = 0; i < OBJECT_NUM; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        std::string data(dataSize, std::to_string(i % MOD)[0]);
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(RetryCreate(client0, objectKey, dataSize, param, buffer));
        DS_ASSERT_OK(buffer->MemoryCopy(const_cast<char *>(data.data()), dataSize));
        DS_ASSERT_OK(buffer->Seal());
    }

    // worker1 get
    for (int j = 0; j < OBJECT_NUM; j++) {
        std::string objectKey = "key_" + std::to_string(j);
        std::string data(dataSize, std::to_string(j % MOD)[0]);
        std::vector<Optional<Buffer>> buffer;
        DS_ASSERT_OK(RetryGet(client1, { objectKey }, 0, buffer));
        ASSERT_EQ(std::string((const char *)buffer[0]->ImmutableData(), buffer[0]->GetSize()), data);
    }
}

TEST_F(EvictionManagerEndToEndTest, OneNodeSpillDeleteSuccess)
{
    std::shared_ptr<ObjectClient> client0;
    InitTestClient(0, client0);
    uint64_t dataSize = 500 * 1024;
    CreateParam param;
    HostPort metaAddress;
    cluster_->GetMetaServerAddr(metaAddress);

    // Put, spill will be triggered
    for (int i = 0; i < OBJECT_NUM; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        std::string data(dataSize, std::to_string(i % MOD)[0]);
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(RetryCreate(client0, objectKey, dataSize, param, buffer));
        DS_ASSERT_OK(buffer->MemoryCopy(const_cast<char *>(data.data()), dataSize));
        DS_ASSERT_OK(buffer->Seal());

        std::vector<std::string> failObjects;
        DS_ASSERT_OK(client0->GIncreaseRef({ objectKey }, failObjects));
        ASSERT_TRUE(failObjects.empty());
    }

    // GDecRef, objects will be deleted from memory and disk
    for (int i = 0; i < OBJECT_NUM; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        std::vector<std::string> failObjects;
        DS_ASSERT_OK(client0->GDecreaseRef({ objectKey }, failObjects));
        ASSERT_TRUE(failObjects.empty());
    }

    // Get
    for (int i = 0; i < OBJECT_NUM; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        std::string data(dataSize, std::to_string(i % MOD)[0]);
        std::vector<Optional<Buffer>> buffer;
        DS_ASSERT_NOT_OK(client0->Get({ objectKey }, 0, buffer));
    }
}

TEST_F(EvictionManagerEndToEndTest, MultiNodeSpillDeleteSuccess)
{
    std::shared_ptr<ObjectClient> client0;
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client0);
    InitTestClient(1, client1);
    uint64_t dataSize = 500 * 1024;
    CreateParam param;
    std::vector<std::string> failedIds;
    HostPort metaAddress;
    cluster_->GetMetaServerAddr(metaAddress);

    for (int i = 0; i < OBJECT_NUM; i++) {
        // worker0 Put, spill will be triggered
        std::string objectKey = "key_" + std::to_string(i);
        std::string data(dataSize, std::to_string(i % MOD)[0]);
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(RetryCreate(client0, objectKey, dataSize, param, buffer));
        DS_ASSERT_OK(buffer->MemoryCopy(const_cast<char *>(data.data()), dataSize));
        DS_ASSERT_OK(client0->GIncreaseRef({ objectKey }, failedIds));
        DS_ASSERT_OK(buffer->Seal());

        // worker1 remote get, not spill
        std::vector<Optional<Buffer>> buffers;
        DS_ASSERT_OK(RetryGet(client1, { objectKey }, 0, buffers));
        ASSERT_EQ(buffers.size(), size_t(1));
        DS_ASSERT_OK(client1->GIncreaseRef({ objectKey }, failedIds));
        ASSERT_EQ(std::string((const char *)buffers[0]->ImmutableData(), buffers[0]->GetSize()), data);
    }

    // Delete
    for (int i = 0; i < OBJECT_NUM; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        DS_ASSERT_OK(client0->GDecreaseRef({ objectKey }, failedIds));
        ASSERT_TRUE(failedIds.empty());
        DS_ASSERT_OK(client1->GDecreaseRef({ objectKey }, failedIds));
        ASSERT_TRUE(failedIds.empty());
    }

    // Get from worker1
    for (int i = 0; i < OBJECT_NUM; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        std::string data(dataSize, std::to_string(i % MOD)[0]);
        std::vector<Optional<Buffer>> buffer;
        DS_ASSERT_NOT_OK(client1->Get({ objectKey }, 0, buffer));
    }
}

TEST_F(EvictionManagerEndToEndTest, EvictObjNotPrimaryCopy)
{
    std::shared_ptr<ObjectClient> client0;
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client0);
    InitTestClient(1, client1);
    uint64_t dataSize = 500 * 1024;
    CreateParam param;
    HostPort metaAddress;
    cluster_->GetMetaServerAddr(metaAddress);
    int i;
    for (i = 0; i < OBJECT_NUM; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        std::string data(dataSize, std::to_string(i % MOD)[0]);
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(RetryCreate(client0, objectKey, dataSize, param, buffer));
        DS_ASSERT_OK(buffer->MemoryCopy(const_cast<char *>(data.data()), dataSize));
        DS_ASSERT_OK(buffer->Seal());

        std::vector<Optional<Buffer>> buffers;
        DS_ASSERT_OK(RetryGet(client1, { objectKey }, 0, buffers));
        ASSERT_EQ(buffers.size(), size_t(1));
        ASSERT_EQ(std::string((const char *)buffers[0]->ImmutableData(), buffers[0]->GetSize()), data);
    }

    LOG(INFO) << FormatString("Put %d objects, begin to get these objects.", i);
    int j;
    for (j = 0; j < i; j++) {
        std::string objectKey = "key_" + std::to_string(j);
        std::string data(dataSize, std::to_string(j % MOD)[0]);
        std::vector<Optional<Buffer>> buffers0, buffers1;
        DS_ASSERT_OK(client0->Get({ objectKey }, 0, buffers0));
        DS_ASSERT_OK(client1->Get({ objectKey }, 0, buffers1));
        ASSERT_TRUE(NotExistsNone(buffers0) || NotExistsNone(buffers1));
        if (NotExistsNone(buffers0)) {
            ASSERT_EQ(std::string((const char *)buffers0[0]->ImmutableData(), buffers0[0]->GetSize()), data);
        }
        if (NotExistsNone(buffers1)) {
            ASSERT_EQ(std::string((const char *)buffers1[0]->ImmutableData(), buffers1[0]->GetSize()), data);
        }
    }
    ASSERT_EQ(i, j);
}

TEST_F(EvictionManagerEndToEndTest, MutableSpillSingleNodeTest)
{
    std::shared_ptr<ObjectClient> client0;
    InitTestClient(0, client0);
    uint64_t dataSize = 500 * 1024;
    HostPort metaAddress;
    cluster_->GetMetaServerAddr(metaAddress);

    // Put, spill will be triggered
    CreateParam param;
    for (int i = 0; i < OBJECT_NUM; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        std::string data(dataSize, std::to_string(i % MOD)[0]);
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(RetryCreate(client0, objectKey, dataSize, param, buffer));
        DS_ASSERT_OK(buffer->MemoryCopy(const_cast<char *>(data.data()), dataSize));
        DS_ASSERT_OK(buffer->Publish());
    }
    constexpr size_t num = 9;
    // Get
    for (int i = 0; i < OBJECT_NUM; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        std::string oldData(dataSize, std::to_string(i % MOD)[0]);
        std::string newData(dataSize, std::to_string(num - (i % MOD))[0]);
        std::vector<Optional<Buffer>> buffer;
        DS_ASSERT_OK(RetryGet(client0, { objectKey }, 0, buffer));
        ASSERT_EQ(std::string((const char *)buffer[0]->ImmutableData(), buffer[0]->GetSize()), oldData);
        DS_ASSERT_OK(buffer[0]->MemoryCopy(const_cast<char *>(newData.data()), dataSize));
        DS_ASSERT_OK(buffer[0]->Publish());
    }

    // Get
    for (int i = 0; i < OBJECT_NUM; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        std::string data(dataSize, std::to_string(num - (i % MOD))[0]);
        std::vector<Optional<Buffer>> buffer;
        DS_ASSERT_OK(RetryGet(client0, { objectKey }, 0, buffer));
        ASSERT_EQ(std::string((const char *)buffer[0]->ImmutableData(), buffer[0]->GetSize()), data);
    }
}

TEST_F(EvictionManagerEndToEndTest, DISABLED_MutableSpillMultiNodeTest)
{
    std::shared_ptr<ObjectClient> client0;
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client0);
    InitTestClient(1, client1);
    uint64_t dataSize = 500 * 1024;
    CreateParam param{ .consistencyType = ConsistencyType::CAUSAL };
    HostPort metaAddress;
    cluster_->GetMetaServerAddr(metaAddress);
    int objNum = 30;  // obj num is 30;
    // client0 Put, spill will be triggered
    for (int i = 0; i < objNum; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        std::string data(dataSize, std::to_string(i % MOD)[0]);
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(RetryCreate(client0, objectKey, dataSize, param, buffer));
        DS_ASSERT_OK(buffer->MemoryCopy(const_cast<char *>(data.data()), dataSize));
        DS_ASSERT_OK(buffer->Publish());
    }
    constexpr size_t num = 9;
    // client1 Get
    for (int i = 0; i < objNum; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        std::string oldData(dataSize, std::to_string(i % MOD)[0]);
        std::string newData(dataSize, std::to_string(num - (i % MOD))[0]);
        std::vector<Optional<Buffer>> buffer;
        DS_ASSERT_OK(RetryGet(client1, { objectKey }, 0, buffer));
        ASSERT_EQ(std::string((const char *)buffer[0]->ImmutableData(), buffer[0]->GetSize()), oldData);
        DS_ASSERT_OK(buffer[0]->MemoryCopy(const_cast<char *>(newData.data()), dataSize));
        DS_ASSERT_OK(buffer[0]->Publish());
    }

    // client0 Get
    for (int i = 0; i < objNum; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        std::string data(dataSize, std::to_string(num - (i % MOD))[0]);
        std::vector<Optional<Buffer>> buffer;
        ASSERT_EQ(RetryGet(client0, { objectKey }, 0, buffer), Status::OK());
        ASSERT_EQ(std::string((const char *)buffer[0]->ImmutableData(), buffer[0]->GetSize()), data);
    }

    // client1 Get
    for (int i = 0; i < objNum; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        std::string data(dataSize, std::to_string(num - (i % MOD))[0]);
        std::vector<Optional<Buffer>> buffer;
        DS_ASSERT_OK(RetryGet(client1, { objectKey }, 0, buffer));
        ASSERT_EQ(std::string((const char *)buffer[0]->ImmutableData(), buffer[0]->GetSize()), data);
    }
}

class EvictionManagerEndToEndTest2 : public OCClientCommon {
public:
    std::vector<std::string> workerAddress_;

    void SetClusterSetupOptions(ExternalClusterOptions &opts)
    {
        opts.numWorkers = 2;
        opts.numEtcd = 1;
        std::string hostIp = "127.0.0.1";
        opts.enableSpill = true;
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        for (auto addr : opts.workerConfigs) {
            workerAddress_.emplace_back(addr.ToString());
        }
        opts.workerGflagParams = " -shared_memory_size_mb=10 -v=1 -spill_size_limit=100";
        opts.numOBS = 1;
    }
};

TEST_F(EvictionManagerEndToEndTest2, DISABLED_TestEvictWriteThroughSpaceFull)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    CreateParam param{};
    HostPort metaAddress;
    cluster_->GetMetaServerAddr(metaAddress);

    // Allocator just remain 1024*1024 bytes for extent allocation.
    uint64_t totalSize = 1024 * 1024;
    uint64_t metadataSize = 4;
    uint64_t dataSize = totalSize - metadataSize;
    std::string data(dataSize, 'x');
    for (int i = 0; i < OBJECT_NUM; i++) {
        // Put exceed shared_memory_size_mb, will trigger evict
        std::string objectKey = "key_" + std::to_string(i);
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(RetryCreate(client1, objectKey, dataSize, param, buffer));
        buffer->MemoryCopy(reinterpret_cast<uint8_t *>(const_cast<char *>(data.c_str())), data.size());
        DS_ASSERT_OK(buffer->Publish());

        // Remote get exceed shared_memory_size_mb, will trigger evict
        std::vector<Optional<Buffer>> buffers;
        DS_ASSERT_OK(RetryGet(client2, { objectKey }, 0, buffers));
        ASSERT_TRUE(buffers.size() == 1);
        auto &bufferGet1 = *buffers[0];
        ASSERT_EQ(data, std::string(reinterpret_cast<const char *>(bufferGet1.ImmutableData()), bufferGet1.GetSize()));
    }

    // Get from local or remote disk
    for (int i = 0; i < OBJECT_NUM; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        std::vector<Optional<Buffer>> buffers1;
        DS_ASSERT_OK(RetryGet(client1, { objectKey }, 0, buffers1));
        ASSERT_EQ(buffers1.size(), size_t(1));
        auto &bufferGet1 = *buffers1[0];
        ASSERT_EQ(data, std::string(reinterpret_cast<const char *>(bufferGet1.ImmutableData()), bufferGet1.GetSize()));

        std::vector<Optional<Buffer>> buffers2;
        DS_ASSERT_OK(RetryGet(client2, { objectKey }, 0, buffers2));
        ASSERT_EQ(buffers2.size(), size_t(1));
        auto &bufferGet2 = *buffers2[0];
        ASSERT_EQ(data, std::string(reinterpret_cast<const char *>(bufferGet2.ImmutableData()), bufferGet2.GetSize()));
    }
}

class EvictionManagerOOMTest : public OCClientCommon {
public:
    std::vector<std::string> workerAddress_;

    void SetClusterSetupOptions(ExternalClusterOptions &opts)
    {
        opts.numWorkers = 2;
        opts.numEtcd = 1;
        std::string hostIp = "127.0.0.1";
        opts.enableSpill = true;
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        for (auto addr : opts.workerConfigs) {
            workerAddress_.emplace_back(addr.ToString());
        }
        opts.workerGflagParams = " -shared_memory_size_mb=2000 -v=1 ";
        opts.numOBS = 1;
    }
};

TEST_F(EvictionManagerOOMTest, DISABLED_OOMTest1)
{
    std::shared_ptr<ObjectClient> client0;
    InitTestClient(0, client0);
    HostPort metaAddress;
    cluster_->GetMetaServerAddr(metaAddress);
    uint64_t dataSize = 1024 * 1024;
    // Put, spill will be triggered
    CreateParam param;
    constexpr int objNum = 5000;
    for (int i = 0; i < objNum; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        std::string data(dataSize, std::to_string(i % MOD)[0]);
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(client0->Create(objectKey, dataSize, param, buffer));
        DS_ASSERT_OK(buffer->MemoryCopy(const_cast<char *>(data.data()), dataSize));
        DS_ASSERT_OK(buffer->Seal());
    }

    // Get
    for (int i = 0; i < objNum; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        std::string data(dataSize, std::to_string(i % MOD)[0]);
        std::vector<Optional<Buffer>> buffer;
        DS_ASSERT_OK(client0->Get({ objectKey }, 0, buffer));
        ASSERT_TRUE(NotExistsNone(buffer));
        ASSERT_EQ(std::string((const char *)buffer[0]->ImmutableData(), buffer[0]->GetSize()), data);
    }
}

class EvictionManagerConcurrently : public ExternalClusterTest, public EvictionManagerCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = workerNum_;
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "false";
        std::string hostIp = "127.0.0.1";
        // port order: worker0 port < worker1 port < worker2 port
        HostPort ipPort1(hostIp, GetFreePort());
        HostPort ipPort2(hostIp, GetFreePort());
        HostPort ipPort3(hostIp, GetFreePort());
        std::map<std::string, int> addressMap;
        addressMap[ipPort1.ToString()] = ipPort1.Port();
        addressMap[ipPort2.ToString()] = ipPort2.Port();
        addressMap[ipPort3.ToString()] = ipPort3.Port();
        for (const auto &kv : addressMap) {
            opts.workerConfigs.emplace_back(hostIp, kv.second);
        }
    }

    void SetUp() override
    {
        akSkManager_ = std::make_shared<AkSkManager>(0);
        akSkManager_->SetClientAkSk(accessKey_, secretKey_);
        ExternalClusterTest::SetUp();
        InitTest();
    }

    void InitWorkerClient(uint32_t workerIndex, std::shared_ptr<ObjectClient> &client, int32_t timeoutMs = 60000)
    {
        HostPort workerAddress;
        ASSERT_TRUE(workerIndex < workerNum_);
        DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex, workerAddress));
        ConnectOptions connectOptions = { .host = workerAddress.Host(),
                                          .port = workerAddress.Port(),
                                          .connectTimeoutMs = timeoutMs };
        connectOptions.accessKey = accessKey_;
        connectOptions.secretKey = secretKey_;
        client = std::make_shared<ObjectClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
    }

    std::shared_ptr<WorkerMasterOCApi> CreateMasterClient(int workerIndex = 0)
    {
        HostPort metaAddress, localAddress;
        cluster_->GetMetaServerAddr(metaAddress);
        cluster_->GetWorkerAddr(workerIndex, localAddress);
        int stubCacheNum = 100;
        RpcStubCacheMgr::Instance().Init(stubCacheNum);
        auto client = WorkerMasterOCApi::CreateWorkerMasterOCApi(metaAddress, localAddress, akSkManager_);
        client->Init();
        return client;
    }

    std::string GetWorkerAddr(int workerIndex = 0)
    {
        HostPort workerAddress;
        cluster_->GetWorkerAddr(workerIndex, workerAddress);
        return workerAddress.ToString();
    }

    Status QueryMetadata(std::shared_ptr<WorkerMasterOCApi> api, int workerIndex,
                         const std::vector<std::string> &objectKeys, datasystem::master::QueryMetaRspPb &rsp)
    {
        const std::string queryReqId = GetStringUuid();
        datasystem::master::QueryMetaReqPb req;
        std::vector<RpcMessage> payloads;
        *req.mutable_ids() = { objectKeys.begin(), objectKeys.end() };
        req.set_request_id(queryReqId);
        req.set_address(GetWorkerAddr(workerIndex));
        return api->QueryMeta(req, 0, rsp, payloads);
    }

    Status CreateMeta(const std::string &objectKey, int workerIndex, std::shared_ptr<WorkerMasterOCApi> client,
                      bool isCopy = false)
    {
        if (isCopy) {
            CreateCopyMetaReqPb req;
            CreateCopyMetaRspPb rsp;
            req.set_object_key(objectKey);
            req.set_address(GetWorkerAddr(workerIndex));
            return client->CreateCopyMeta(req, rsp);
        } else {
            auto metaPb = std::make_unique<ObjectMetaPb>();
            metaPb->set_object_key(objectKey);
            metaPb->set_data_size(1000);
            CreateMetaReqPb req;
            CreateMetaRspPb rsp;
            req.set_address(GetWorkerAddr(workerIndex));
            req.set_allocated_meta(metaPb.release());
            return client->CreateMeta(req, rsp);
        }
    }

    Status UpdateMeta(const std::string &objectKey, std::shared_ptr<WorkerMasterOCApi> client)
    {
        UpdateMetaReqPb metaReq;
        metaReq.set_object_key(objectKey);
        metaReq.set_life_state(static_cast<uint32_t>(ObjectLifeState::OBJECT_SEALED));
        UpdateMetaRspPb metaRsp;
        return client->UpdateMeta(metaReq, metaRsp);
    }

protected:
    void InitTest()
    {
        HostPort metaAddr;
        DS_ASSERT_OK(cluster_->GetMetaServerAddr(metaAddr));
        metaAddr_ = metaAddr;

        HostPort worker0Addr;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, worker0Addr));
        worker0Addr_ = worker0Addr;

        HostPort worker1Addr;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(1, worker1Addr));
        worker1Addr_ = worker1Addr;

        HostPort worker2Addr;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(workerId2_, worker2Addr));
        worker2Addr_ = worker2Addr;

        objectTable_ = std::make_shared<ObjectTable>();
        allocator = datasystem::memory::Allocator::Instance();
        allocator->Init(maxMemorySize);
    }

    HostPort metaAddr_;
    HostPort worker0Addr_;
    HostPort worker1Addr_;
    HostPort worker2Addr_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    std::shared_ptr<AkSkManager> akSkManager_;
    constexpr static int workerNum_ = 3;
    constexpr static int workerId2_ = 2;
};

/***
 * worker2: create & spill
 * worker1: remote get from worker3 & evict delete
 * worker0: remote get from worker2
 */
TEST_F(EvictionManagerConcurrently, EvictBeforeRemoteGet)
{
    std::shared_ptr<ObjectClient> client0;
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitWorkerClient(0, client0);
    InitWorkerClient(1, client1);
    InitWorkerClient(workerId2_, client2);

    std::string objectKey = "key_test";
    uint64_t dataSize = 10 * 1024 * 1024;
    std::string data(dataSize, 'x');
    CreateParam param;

    {
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(RetryCreate(client1, objectKey, dataSize, param, buffer));
        DS_ASSERT_OK(buffer->MemoryCopy(const_cast<char *>(data.data()), dataSize));
        DS_ASSERT_OK(buffer->Seal());
    }

    // Now location table: worker1
    // primary address: worker1

    auto masterClient = CreateMasterClient();
    DS_EXPECT_OK(CreateMeta(objectKey, 0, masterClient, true));

    // Now location table: worker0, worker1
    // primary address: worker1

    datasystem::master::QueryMetaRspPb rsp;
    DS_EXPECT_OK(QueryMetadata(masterClient, workerId2_, { objectKey }, rsp));
    ASSERT_EQ(rsp.query_metas_size(), 1);
    ASSERT_TRUE((rsp.query_metas(0).address() == GetWorkerAddr(0))
                || (rsp.query_metas(0).address() == GetWorkerAddr(1)));
    ASSERT_EQ(rsp.query_metas(0).meta().primary_address(), GetWorkerAddr(1));

    {
        std::vector<Optional<Buffer>> buffer;
        DS_ASSERT_OK(RetryGet(client2, { objectKey }, 0, buffer));
        ASSERT_EQ(std::string((const char *)buffer[0]->ImmutableData(), buffer[0]->GetSize()), data);
    }
}

class EvictionManagerConcurrently2 : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts)
    {
        opts.numWorkers = 2;
        opts.numEtcd = 1;
        opts.enableSpill = true;
        std::string hostIp = "127.0.0.1";
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        opts.workerGflagParams = " -shared_memory_size_mb=10 ";
    }
};

TEST_F(EvictionManagerConcurrently2, DISABLED_EvictDuringPublish)
{
    std::shared_ptr<ObjectClient> client;
    std::shared_ptr<ObjectClient> client0;
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    std::shared_ptr<ObjectClient> client3;
    InitTestClient(0, client);
    InitTestClient(0, client0);
    InitTestClient(0, client1);
    InitTestClient(0, client2);
    InitTestClient(0, client3);

    // Put object key_0 ~ key_9
    uint64_t dataSize = 1024 * 1024;
    HostPort metaAddress;
    cluster_->GetMetaServerAddr(metaAddress);
    constexpr int objNum = 10;
    for (int i = 0; i < objNum; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        std::string data(dataSize, std::to_string(i)[0]);
        CreateParam param;
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(RetryCreate(client, objectKey, dataSize, param, buffer));
        DS_ASSERT_OK(buffer->MemoryCopy(const_cast<char *>(data.data()), dataSize));
        DS_ASSERT_OK(buffer->Publish());
    }

    ThreadPool threadPool(objNum);

    std::vector<std::future<void>> futures;
    // Get key0 ~ key9 in loop
    futures.emplace_back(threadPool.Submit([&]() {
        long loop = 0;
        long loopCnt = 100;
        while (loop++ < loopCnt) {
            for (int i = 0; i < objNum; i++) {
                std::string objectKey = "key_" + std::to_string(i);
                std::string data(dataSize, std::to_string(i)[0]);
                std::vector<Optional<Buffer>> buffer;
                DS_ASSERT_OK(RetryGet(client, { objectKey }, 0, buffer));
                ASSERT_EQ(std::string((const char *)buffer[0]->ImmutableData(), buffer[0]->GetSize()), data);
            }
        }
    }));

    // Get and Publish key_0
    futures.emplace_back(threadPool.Submit([&]() {
        std::string objectKey = "key_0";
        std::string data(dataSize, '0');
        int loopCnt = 500;
        for (int i = 0; i < loopCnt; i++) {
            std::vector<Optional<Buffer>> buffer;
            DS_ASSERT_OK(RetryGet(client0, { objectKey }, 0, buffer));
            ASSERT_EQ(buffer.size(), size_t(1));
            ASSERT_EQ(std::string((const char *)buffer[0]->ImmutableData(), buffer[0]->GetSize()), data);
            DS_ASSERT_OK(RetryPublish(buffer[0]));
        }
    }));

    // Get and Publish key_1
    futures.emplace_back(threadPool.Submit([&]() {
        std::string objectKey = "key_1";
        std::string data(dataSize, '1');
        int loopCnt = 500;
        for (int i = 0; i < loopCnt; i++) {
            std::vector<Optional<Buffer>> buffer;
            DS_ASSERT_OK(RetryGet(client1, { objectKey }, 0, buffer));
            ASSERT_EQ(buffer.size(), size_t(1));
            ASSERT_EQ(std::string((const char *)buffer[0]->ImmutableData(), buffer[0]->GetSize()), data);
            DS_ASSERT_OK(RetryPublish(buffer[0]));
        }
    }));

    // Get and Publish key_2
    futures.emplace_back(threadPool.Submit([&]() {
        std::string objectKey = "key_2";
        std::string data(dataSize, '2');
        int loopCnt = 300;
        for (int i = 0; i < loopCnt; i++) {
            std::vector<Optional<Buffer>> buffer;
            DS_ASSERT_OK(RetryGet(client2, { objectKey }, 0, buffer));
            ASSERT_EQ(buffer.size(), size_t(1));
            ASSERT_EQ(std::string((const char *)buffer[0]->ImmutableData(), buffer[0]->GetSize()), data);
            DS_ASSERT_OK(RetryPublish(buffer[0]));
        }
    }));

    // Get and Publish key_3
    futures.emplace_back(threadPool.Submit([&]() {
        std::string objectKey = "key_3";
        std::string data(dataSize, '3');
        int loopCnt = 300;
        for (int i = 0; i < loopCnt; i++) {
            std::vector<Optional<Buffer>> buffer;
            DS_ASSERT_OK(RetryGet(client3, { objectKey }, 0, buffer));
            ASSERT_EQ(buffer.size(), size_t(1));
            ASSERT_EQ(std::string((const char *)buffer[0]->ImmutableData(), buffer[0]->GetSize()), data);
            DS_ASSERT_OK(RetryPublish(buffer[0]));
        }
    }));

    for (auto &f : futures) {
        f.get();
    }
}

class FastModeSpillBenchMark : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts)
    {
        opts.numWorkers = 1;
        std::string hostIp = "127.0.0.1";
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        std::string shmSize = GetStringFromEnv("SHM_SIZE", 0);
        const std::string ssd = "/data/ssd/spill";
        opts.workerGflagParams = "-shared_memory_size_mb=" + shmSize + " -rpc_thread_num=64 -spill_directories=" + ssd;
    }

    const uint64_t objSize = GetUint64FromEnv("OBJ_SIZE", 0);
    const int loop = GetInt32FromEnv("LOOP", 0);
    const int threadNum = GetInt32FromEnv("THREAD_NUM", 0);
};

struct FileInfo {
    char *buf;
    size_t offset;
    size_t size;
};

void ReadFileTask(int index, long loop, size_t objSize, std::shared_ptr<Queue<FileInfo>> queue)
{
    std::string file = "/data/ssd/file" + std::to_string(index);
    int fd = open(file.c_str(), O_RDWR, 0755);
    for (long i = 0; i < loop; i++) {
        auto *buf = static_cast<char *>(malloc(objSize));
        size_t offset = i * objSize;
        ASSERT_EQ(static_cast<size_t>(pread(fd, buf, objSize, offset)), objSize);
        FileInfo fileInfo{ .buf = buf, .offset = offset, .size = objSize };
        queue->Put(fileInfo);
    }
}

void WriteFileTask(int index, long loop, std::shared_ptr<Queue<FileInfo>> queue)
{
    std::string file = "/data/ssd/file" + std::to_string(index);
    int fd = open(file.c_str(), O_RDWR, 0755);
    for (long i = 0; i < loop; i++) {
        FileInfo fileInfo;
        queue->Take(&fileInfo);
        ASSERT_EQ(static_cast<size_t>(pwrite(fd, fileInfo.buf, fileInfo.size, fileInfo.offset)), fileInfo.size);
        free(fileInfo.buf);
    }
}

TEST_F(FastModeSpillBenchMark, DISABLED_TestRawWrite)
{
    srand((unsigned)time(NULL));
    std::string data(objSize, 'a');
    auto task = [&data, this](int index) {
        (void)index;
        std::string file = "/data/ssd/file" + std::to_string(index);
        int fd = open(file.c_str(), O_RDWR | O_CREAT | O_APPEND, 0755);
        for (long i = 0; i < loop; i++) {
            ASSERT_GE(write(fd, data.data(), objSize), 0);
        }
        close(fd);
    };
    ThreadPool threadPool(threadNum);
    std::vector<std::future<void>> futures;
    Timer timer;
    for (int i = 0; i < threadNum; i++) {
        futures.emplace_back(threadPool.Submit(task, i));
    }
    for (auto &f : futures) {
        f.get();
    }
    LOG(INFO) << "Sequential write time: " << static_cast<uint64_t>(timer.ElapsedMilliSecond());
}

TEST_F(FastModeSpillBenchMark, DISABLED_TestRawReadWrite)
{
    srand((unsigned)time(NULL));
    ASSERT_EQ(system("echo 1 > /proc/sys/vm/drop_caches"), 0);

    std::vector<std::shared_ptr<Queue<FileInfo>>> queues;
    for (int i = 0; i < threadNum; i++) {
        constexpr int cap = 2;
        std::shared_ptr<Queue<FileInfo>> q = std::make_shared<Queue<FileInfo>>(cap);
        queues.push_back(q);
    }

    ThreadPool readPool(threadNum);
    ThreadPool writePool(threadNum);
    std::vector<std::future<void>> futures;
    Timer timer;
    for (int i = 0; i < threadNum; i++) {
        futures.emplace_back(readPool.Submit(ReadFileTask, i, loop, objSize, queues[i]));
    }
    for (int i = 0; i < threadNum; i++) {
        futures.emplace_back(writePool.Submit(WriteFileTask, i, loop, queues[i]));
    }
    for (auto &f : futures) {
        f.get();
    }
    LOG(INFO) << "Read write time: " << static_cast<uint64_t>(timer.ElapsedMilliSecond());

    for (int i = 0; i < threadNum; i++) {
        std::string file = "/data/ssd/file" + std::to_string(i);
        remove(file.c_str());
    }
}

void GetTask(std::shared_ptr<ObjectClient> client, int index, int loop, const std::string &data,
             std::shared_ptr<Queue<std::shared_ptr<Buffer>>> queue)
{
    (void)data;
    int start = index * loop;
    for (int i = start; i < start + loop; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        std::vector<Optional<Buffer>> buffers;
        const int64_t TIME_OUT_MS = 3600 * 1000;
        DS_ASSERT_OK(RetryGet(client, { objectKey }, TIME_OUT_MS, buffers));
        queue->Put(std::make_shared<Buffer>(std::move(*(buffers[0]))));
    }
}

void PutTask(int loop, std::shared_ptr<Queue<std::shared_ptr<Buffer>>> queue)
{
    for (int i = 0; i < loop; i++) {
        std::shared_ptr<Buffer> buffer;
        queue->Take(&buffer);
        DS_ASSERT_OK(RetryPublish(buffer));
    }
}

TEST_F(FastModeSpillBenchMark, DISABLED_TestDsPutGet)
{
    std::string data(objSize, 'a');
    {
        auto task = [&data, this](std::shared_ptr<ObjectClient> client, int start) {
            for (int i = start; i < start + loop; i++) {
                std::string objectKey = "key_" + std::to_string(i);
                std::shared_ptr<Buffer> buffer;
                DS_ASSERT_OK(RetryCreate(client, objectKey, objSize, {}, buffer));
                DS_ASSERT_OK(buffer->MemoryCopy(const_cast<char *>(data.data()), objSize));
                DS_ASSERT_OK(RetryPublish(buffer));
            }
        };

        std::vector<std::shared_ptr<ObjectClient>> clients;
        for (int i = 0; i < threadNum; i++) {
            std::shared_ptr<ObjectClient> client;
            InitTestClient(0, client);
            clients.emplace_back(client);
        }

        ThreadPool threadPool(threadNum);
        std::vector<std::future<void>> futures;
        Timer timer;
        for (int i = 0; i < threadNum; i++) {
            futures.emplace_back(threadPool.Submit(task, clients[i], i * loop));
        }
        for (auto &f : futures) {
            f.get();
        }
        auto timeCost = static_cast<uint64_t>(timer.ElapsedMilliSecond());
        LOG(INFO) << "PUT TIME: " << timeCost;
    }

    ASSERT_EQ(system("echo 1 > /proc/sys/vm/drop_caches"), 0);
    LOG(INFO) << "########################## begin stage 2";
    constexpr int sleepTime = 10;
    std::this_thread::sleep_for(std::chrono::seconds(sleepTime));
    {
        std::vector<std::shared_ptr<ObjectClient>> clients;
        for (int i = 0; i < threadNum; i++) {
            std::shared_ptr<ObjectClient> client;
            InitTestClient(0, client);
            clients.emplace_back(client);
        }

        ThreadPool getPool(threadNum);
        ThreadPool putPool(threadNum);
        std::vector<std::future<void>> futures;
        std::vector<std::shared_ptr<Queue<std::shared_ptr<Buffer>>>> queues;
        for (int i = 0; i < threadNum; i++) {
            auto q = std::make_shared<Queue<std::shared_ptr<Buffer>>>(2);
            queues.push_back(q);
        }

        Timer timer;
        for (int i = 0; i < threadNum; i++) {
            futures.emplace_back(getPool.Submit(GetTask, clients[i], i, loop, data, queues[i]));
        }
        for (int i = 0; i < threadNum; i++) {
            futures.emplace_back(putPool.Submit(PutTask, loop, queues[i]));
        }
        for (auto &f : futures) {
            f.get();
        }
        LOG(INFO) << "Get put time: " << static_cast<uint64_t>(timer.ElapsedMilliSecond());
    }
    PerfManager::Instance()->PrintPerfLog();
}

#ifdef USE_URMA
class UrmaEvictionManagerEndToEndTest : public EvictionManagerEndToEndTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts)
    {
        EvictionManagerEndToEndTest::SetClusterSetupOptions(opts);
        opts.workerGflagParams += " -arena_per_tenant=1 -enable_urma=true ";
    }
};

TEST_F(UrmaEvictionManagerEndToEndTest, UrmaLocalSpillRemoteGetSuccess)
{
    std::shared_ptr<ObjectClient> client0;
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client0);
    InitTestClient(1, client1);
    const uint64_t dataSize = 500 * 1024;
    const CreateParam param;
    HostPort metaAddress;
    cluster_->GetMetaServerAddr(metaAddress);

    // worker0 put
    for (int i = 0; i < OBJECT_NUM; i++) {
        const std::string objectKey = "key_" + std::to_string(i);
        const std::string data(dataSize, std::to_string(i % MOD)[0]);
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(RetryCreate(client0, objectKey, dataSize, param, buffer));
        DS_ASSERT_OK(buffer->MemoryCopy(const_cast<char *>(data.data()), dataSize));
        DS_ASSERT_OK(buffer->Seal());
    }

    // worker1 get
    for (int j = 0; j < OBJECT_NUM; j++) {
        const std::string objectKey = "key_" + std::to_string(j);
        const std::string data(dataSize, std::to_string(j % MOD)[0]);
        std::vector<Optional<Buffer>> buffer;
        DS_ASSERT_OK(RetryGet(client1, { objectKey }, 0, buffer));
        ASSERT_EQ(std::string((const char *)buffer[0]->ImmutableData(), buffer[0]->GetSize()), data);
    }
}
#endif

class EmbeddedMmapTableTets : public ExternalClusterTest, public EvictionManagerCommon {
public:
    ~EmbeddedMmapTableTets() = default;

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 0;
        opts.numEtcd = 0;
    }

    void SetUp() override
    {
        HostPort workerId;
        DS_ASSERT_OK(workerId.ParseString("127.0.0.1:34543"));
        eviction_ = std::make_shared<object_cache::WorkerOcEvictionManager>(nullptr, workerId, workerId);
        ExternalClusterTest::SetUp();
    }

public:
    std::shared_ptr<object_cache::WorkerOcEvictionManager> eviction_;
};

TEST_F(EmbeddedMmapTableTets, EmbeddeMmapMemoryTest)
{
    auto size = 10 * 1024 * 1024;
    allocator = datasystem::memory::Allocator::Instance();
    DS_ASSERT_OK(allocator->Init(size, size));
    std::string workerAddr = "127.0.0.1:" + std::to_string(GetFreePort());
    auto dataSize = 100;
    auto shmUnit = std::make_shared<ShmUnit>();
    auto metadataSize = GetMetaSize(dataSize);
    DS_ASSERT_OK(AllocateMemoryForObject("objectKey", dataSize, metadataSize, true, eviction_, *shmUnit,
                                         datasystem::CacheType::MEMORY));

    std::unique_ptr<datasystem::client::EmbeddedMmapTable> mmapTable =
        std::make_unique<datasystem::client::EmbeddedMmapTable>(false);
    int unusedClientFd = 0;  // for embeddedclient, no need mmap client fd.
    DS_ASSERT_OK(mmapTable->MmapAndStoreFd(unusedClientFd, shmUnit->GetFd(), shmUnit->GetMmapSize(), ""));
    uint8_t *pointer;
    DS_ASSERT_OK(mmapTable->LookupFdPointer(shmUnit->GetFd(), &pointer));
}

TEST_F(EmbeddedMmapTableTets, EmbeddeMmapMemoryDisk)
{
    std::string dir = cluster_->GetRootDir() + "/shared_disk/";
    FLAGS_shared_disk_directory = dir.c_str();
    auto size = 10 * 1024 * 1024;
    allocator = datasystem::memory::Allocator::Instance();
    DS_ASSERT_OK(allocator->Init(size, size));
    std::string workerAddr = "127.0.0.1:" + std::to_string(GetFreePort());
    auto dataSize = 100;
    auto shmUnit = std::make_shared<ShmUnit>();
    auto metadataSize = GetMetaSize(dataSize);
    DS_ASSERT_OK(AllocateMemoryForObject("objectKey", dataSize, metadataSize, true, eviction_, *shmUnit,
                                         datasystem::CacheType::DISK));

    std::unique_ptr<datasystem::client::IMmapTable> mmapTable =
        std::make_unique<datasystem::client::EmbeddedMmapTable>(false);
    int unusedClientFd = 0;  // for embeddedclient, no need mmap client fd.
    DS_ASSERT_OK(mmapTable->MmapAndStoreFd(unusedClientFd, shmUnit->GetFd(), shmUnit->GetMmapSize(), ""));
    uint8_t *pointer;
    DS_ASSERT_OK(mmapTable->LookupFdPointer(shmUnit->GetFd(), &pointer));
}
}  // namespace st
}  // namespace datasystem
