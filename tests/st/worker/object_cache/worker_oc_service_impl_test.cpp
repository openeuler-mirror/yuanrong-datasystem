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
 * Description: WorkerOC ServiceImpl Test
 */

#include "common.h"

#include <securec.h>
#include <string>
#include <vector>

#include "datasystem/client/mmap/shm_mmap_table_entry.h"
#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/master/object_cache/store/object_meta_store.h"
#include "datasystem/master/replica_manager.h"
#include "datasystem/master/object_cache/master_oc_service_impl.h"
#include "datasystem/object/buffer.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/kv_client.h"
#include "datasystem/worker/client_manager/client_manager.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"
#include "datasystem/worker/cluster_manager/worker_health_check.h"
#include "datasystem/common/flags/flags.h"

DS_DECLARE_string(etcd_address);
DS_DECLARE_string(master_address);
DS_DECLARE_bool(enable_distributed_master);
namespace datasystem {
namespace st {
static Status ArrToStr(const void *data, size_t sz, std::string &str)
{
    str.resize(sz);
    int ret = memcpy_s(&str.front(), str.size(), data, sz);
    if (ret != EOK) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "memcpy failed");
    }
    return Status::OK();
}
static uint64_t CalBufferMetaSize()
{
    constexpr int alignment = 0x8;
    constexpr int clientNum = 200;
    uint64_t metaSize = clientNum / alignment + 1;
    metaSize += sizeof(uint32_t) + sizeof(char);
    auto alignCeiling = [](uintptr_t addr, uintptr_t alignment) { return (addr + alignment - 1) & ~(alignment - 1); };
    metaSize = alignCeiling(metaSize, 0x40);
    return metaSize;
}
using namespace datasystem::object_cache;
class ServerUnaryWriterReaderMock : public ::datasystem::ServerUnaryWriterReaderImpl<GetRspPb, GetReqPb> {
public:
    ServerUnaryWriterReaderMock(std::string cli, std::string obj, std::shared_ptr<AkSkManager> akSkManager)
        : datasystem::ServerUnaryWriterReaderImpl<GetRspPb, GetReqPb>(nullptr, {}, {}, false, false),
          promise_(),
          fut_(promise_.get_future()),
          client(cli),
          object(obj),
          akSkManager_(akSkManager)
    {
    }

    Status Write(const GetRspPb &pb) override
    {
        promise_.set_value(pb);
        return Status::OK();
    }

    /**
     * @brief Read message into proto buffer.
     * @param[out] pb Buffer to read into.
     * @return Status of the call.
     */
    Status Read(GetReqPb &pb) override
    {
        pb.set_client_id(client);
        pb.mutable_object_keys()->Add(object.c_str());
        return akSkManager_->GenerateSignature(pb);
    }

    Status SendStatus(const Status &rc) override
    {
        status_ = rc;
        return Status::OK();
    }

    Status SendPayload(std::vector<RpcMessage> &buffer) override
    {
        return SendPaylodHelper(buffer);
    }

    Status SendPayload(const std::vector<MemView> &buffer) override
    {
        return SendPaylodHelper(buffer);
    }

    template <typename T>
    Status SendPaylodHelper(T &buffer)
    {
        if (buffer.empty()) {
            return Status::OK();
        }
        auto &msg = buffer.front();
        RETURN_IF_NOT_OK(ArrToStr((uint8_t *)msg.Data(), msg.Size(), str_));
        return Status::OK();
    }

    GetRspPb getRspPb_;
    std::string str_;
    Status status_ = Status::OK();
    std::promise<GetRspPb> promise_;
    std::future<GetRspPb> fut_;
    std::string client;
    std::string object;
    std::shared_ptr<AkSkManager> akSkManager_;
};

class WorkerOCServiceImplTest : public ExternalClusterTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.numEtcd = 1;
        opts.numOBS = 1;
        FLAGS_v = 1;
        FLAGS_enable_distributed_master = false;
        opts.systemSecretKey = "";
        opts.systemAccessKey = "";
        opts.tenantSecretKey = "";
        opts.tenantAccessKey = "";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        Logging::GetInstance()->Start("ds_llt", true, 1);
        int stubCacheNum = 100;
        RpcStubCacheMgr::Instance().Init(stubCacheNum);
        FLAGS_enable_distributed_master = false;
        HostPort metaAddress;
        std::string hostIp = "127.0.0.1";
        auto port = GetFreePort();
        localAddress_ = HostPort{ hostIp, port };
        uint64_t sharedMemoryBytes = 64 * 1024ul * 1024ul;  // convert mb to bytes.
        ASSERT_EQ(datasystem::memory::Allocator::Instance()->Init(sharedMemoryBytes, 0, false, true), Status::OK());

        cluster_->GetMetaServerAddr(metaAddress);

        std::string etcdUrl = "";
        if (cluster_->GetEtcdNum() > 0) {
            for (size_t i = 0; i < cluster_->GetEtcdNum(); ++i) {
                std::pair<HostPort, HostPort> addrs;
                cluster_->GetEtcdAddrs(i, addrs);
                if (!etcdUrl.empty()) {
                    etcdUrl += ",";
                }
                etcdUrl += addrs.first.ToString();
            }
        }

        using ObjectTable = SafeTable<ImmutableString, ObjectInterface>;
        auto objectTable = std::make_shared<ObjectTable>();

        FLAGS_etcd_address = etcdUrl;
        HostPort workerAddr;
        cluster_->GetWorkerAddr(0, workerAddr);
        FLAGS_master_address = workerAddr.ToString();
        LOG(INFO) << "Given etcd address is: " << FLAGS_etcd_address;
        auto evictionManager =
            std::make_shared<object_cache::WorkerOcEvictionManager>(objectTable, localAddress_, metaAddress, nullptr);
        akSkManager_ = std::make_shared<AkSkManager>();
        std::pair<HostPort, HostPort> addrs;
        cluster_->GetEtcdAddrs(0, addrs);
        etcdStore_ = std::make_unique<EtcdStore>(addrs.first.ToString());
        etcdStore_->Init();
        etcdCM_ = std::make_unique<EtcdClusterManager>(localAddress_, metaAddress, etcdStore_.get(), false);
        replicaManager_ = std::make_unique<ReplicaManager>();
        objCacheMasterSvc_ = std::make_unique<datasystem::master::MasterOCServiceImpl>(
            localAddress_, nullptr, akSkManager_, replicaManager_.get(), nullptr);
        workerOcServiceImpl = std::make_unique<datasystem::object_cache::WorkerOCServiceImpl>(
            localAddress_, metaAddress, objectTable, akSkManager_, evictionManager, nullptr, etcdStore_.get(),
            objCacheMasterSvc_.get());
        workerOcServiceImpl->SetClusterManager(etcdCM_.get());
        ASSERT_EQ(workerOcServiceImpl->Init(), Status::OK());
        ClusterInfo clusterInfo;
        DS_ASSERT_OK(EtcdClusterManager::ConstructClusterInfoViaEtcd(etcdStore_.get(), clusterInfo));
        ASSERT_EQ(etcdCM_->Init(clusterInfo), Status::OK());
        SetHealthProbe();
        etcdCM_->SetWorkerReady();

        worker::ClientManager::Instance().Init();
        static const int WAIT_FOR_INIT_DONE_SEC = 7;
        std::this_thread::sleep_for(std::chrono::seconds(WAIT_FOR_INIT_DONE_SEC));
    }

    void TearDown() override
    {
        etcdStore_->Shutdown();
        etcdCM_->Shutdown();
        etcdCM_.reset();
        ExternalClusterTest::TearDown();
        workerOcServiceImpl.reset();
    }

    void Create(const std::string &clientId, const std::string &objectKey, const std::string &str, CreateRspPb &rsp)
    {
        CreateReqPb req;
        req.set_object_key(objectKey);
        req.set_client_id(clientId);
        req.set_data_size(str.size());

        workerOcServiceImpl->Create(req, rsp);
    }

    static void Write(const CreateRspPb &rsp, const std::string &str,
                      std::shared_ptr<client::IMmapTableEntry> &mmapTableEntryPtr)
    {
        if (mmapTableEntryPtr == nullptr) {
            mmapTableEntryPtr = std::make_shared<client::ShmMmapTableEntry>(rsp.store_fd(), rsp.mmap_size());
        }
        auto &mmapTableEntry = *mmapTableEntryPtr;
        ASSERT_EQ(mmapTableEntry.Init(false, ""), Status::OK());
        size_t dataSz = str.size();
        auto objPtr = mmapTableEntry.Pointer() + rsp.offset() + CalBufferMetaSize();
        int ret = memcpy_s((void *)objPtr, dataSz, str.data(), dataSz);
        ASSERT_EQ(ret, EOK);
    }

    Status Seal(const std::string &clientId, const std::string &objectKey, uint64_t dataSz, const std::string &shmId,
                std::vector<RpcMessage> payloads = {})
    {
        return PublishDetail(
            { .clientId = clientId, .objectKey = objectKey, .shmId = shmId, .dataSz = dataSz, .isSeal = true },
            std::move(payloads));
    }

    Status Publish(const std::string &clientId, const std::string &objectKey, uint64_t dataSz, const std::string &shmId,
                   std::vector<RpcMessage> payloads = {})
    {
        return PublishDetail(
            { .clientId = clientId, .objectKey = objectKey, .shmId = shmId, .dataSz = dataSz, .isSeal = false },
            std::move(payloads));
    }

    struct PubParams {
        const std::string &clientId;
        const std::string &objectKey;
        const std::string &shmId;
        uint64_t dataSz;
        bool isSeal;
    };

    Status PublishDetail(const PubParams &pubParams, std::vector<RpcMessage> payloads)
    {
        datasystem::PublishReqPb publishReqPb;
        publishReqPb.set_client_id(pubParams.clientId);
        publishReqPb.set_object_key(pubParams.objectKey);
        publishReqPb.set_data_size(pubParams.dataSz);
        publishReqPb.set_shm_id(pubParams.shmId);
        publishReqPb.set_is_seal(pubParams.isSeal);
        publishReqPb.set_write_mode(static_cast<uint32_t>(WriteMode::NONE_L2_CACHE));
        publishReqPb.set_consistency_type(static_cast<uint32_t>(ConsistencyType::CAUSAL));
        RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(publishReqPb));
        datasystem::PublishRspPb publishRspPb;
        return workerOcServiceImpl->Publish(publishReqPb, publishRspPb, std::move(payloads));
    }

    uint64_t GetMetaSize(uint64_t dataSize)
    {
        const uint64_t defaultMetaSize = 10;
        return WorkerOcServiceCrudCommonApi::CanTransferByShm(dataSize) ? defaultMetaSize : 0;
    }

    static std::vector<RpcMessage> GetPayloads(const std::string &str)
    {
        Status rc;
        std::vector<RpcMessage> msg;
        msg.emplace_back();
        auto &ele = msg.back();
        rc = ele.ZeroCopyBuffer((void *)str.data(), str.size());
        EXPECT_TRUE(rc.IsOk());
        return msg;
    }

    void DecreaseRef(const std::string &clientId, const std::string &shmId)
    {
        DecreaseReferenceRequest decReq;
        DecreaseReferenceResponse decRsp;
        decReq.set_client_id(clientId);
        decReq.add_object_keys(shmId);
        ASSERT_EQ(workerOcServiceImpl->DecreaseReference(decReq, decRsp), Status::OK());
    }

    void GIncreaseRef(const std::string &clientId, const std::string &objKey)
    {
        GIncreaseReqPb req;
        GIncreaseRspPb rsp;
        req.set_client_id(clientId);
        req.set_address(clientId);
        req.add_object_keys(objKey);
        req.set_timeout(RPC_TIMEOUT);
        DS_ASSERT_OK(workerOcServiceImpl->GIncreaseRef(req, rsp));
    }

    void Delete(const std::string &clientId, const std::string &objectKey)
    {
        DeleteAllCopyReqPb delReq;
        DeleteAllCopyRspPb delRsp;
        delReq.set_client_id(clientId);
        delReq.add_object_keys(objectKey);
        ASSERT_EQ(workerOcServiceImpl->DeleteAllCopy(delReq, delRsp), Status::OK());
    }

    RandomData random_;
    std::unique_ptr<datasystem::object_cache::WorkerOCServiceImpl> workerOcServiceImpl;
    std::unique_ptr<EtcdClusterManager> etcdCM_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    std::shared_ptr<datasystem::AkSkManager> akSkManager_;
    std::unique_ptr<EtcdStore> etcdStore_;
    std::unique_ptr<datasystem::ReplicaManager> replicaManager_{ nullptr };
    std::unique_ptr<datasystem::master::MasterOCServiceImpl> objCacheMasterSvc_{ nullptr };
    HostPort localAddress_;
};

// Non shared memory case: Publish -> Get.
TEST_F(WorkerOCServiceImplTest, TestPublish)
{
    auto clientId = ClientKey::Intern("c1");
    uint32_t lockId;
    DS_ASSERT_OK(worker::ClientManager::Instance().AddClient(clientId, false, 1, "", "", "", "", lockId));

    size_t dataSz = 100;
    auto str = random_.GetRandomString(dataSz);

    VLOG(1) << ".......... Publish";
    ASSERT_EQ(Status::OK(), Publish(clientId, "o1", dataSz, "", GetPayloads(str)));
    VLOG(1) << "Finished Publish";

    auto pimpl = std::make_unique<ServerUnaryWriterReaderMock>(clientId, "o1", akSkManager_);
    auto *mockReaderWriterPtr = pimpl.get();
    auto mockReaderWriter = std::make_shared<ServerUnaryWriterReader<GetRspPb, GetReqPb>>(std::move(pimpl));
    ASSERT_EQ(Status::OK(), workerOcServiceImpl->Get(mockReaderWriter));
    auto rsp = mockReaderWriterPtr->fut_.get();
    VLOG(1) << rsp.DebugString();
    ASSERT_EQ(Status::OK(), mockReaderWriterPtr->status_);
    ASSERT_EQ(str, mockReaderWriterPtr->str_);
}

// Shared memory case: Create -> Write -> Publish -> Get -> DecRef -> Delete.
TEST_F(WorkerOCServiceImplTest, TestPut)
{
    auto clientId = ClientKey::Intern("c1");
    uint32_t lockId;
    DS_ASSERT_OK(worker::ClientManager::Instance().AddClient(clientId, true, true, "", "", "", "", lockId));

    // Create.
    std::string shmId;
    CreateRspPb createRspPb;
    std::string objectKey = "o1";
    size_t dataSz = 10'000'000;
    auto str = random_.GetRandomString(dataSz);
    Create(clientId, objectKey, str, createRspPb);
    shmId = createRspPb.shm_id();

    // Write.
    std::shared_ptr<client::IMmapTableEntry> mmapTableEntry;
    Write(createRspPb, str, mmapTableEntry);

    // Publish.
    ASSERT_EQ(Status::OK(), Publish(clientId, objectKey, dataSz, shmId));

    // Get.
    auto pimpl = std::make_unique<ServerUnaryWriterReaderMock>(clientId, objectKey, akSkManager_);
    auto *mockReaderWriterPtr = pimpl.get();
    auto mockReaderWriter = std::make_shared<ServerUnaryWriterReader<GetRspPb, GetReqPb>>(std::move(pimpl));
    ASSERT_EQ(Status::OK(), workerOcServiceImpl->Get(mockReaderWriter));
    auto getRsp = mockReaderWriterPtr->fut_.get();
    const GetRspPb::ObjectInfoPb &info = getRsp.objects(0);
    ASSERT_EQ(createRspPb.store_fd(), info.store_fd());
    ASSERT_EQ(createRspPb.mmap_size(), (uint64_t)info.mmap_size());
    ASSERT_EQ((uint64_t)info.data_size(), dataSz);
    std::hash<std::string> hash;
    std::string getStr;
    auto metaSize = workerOcServiceImpl->GetMetadataSize();
    ArrToStr(mmapTableEntry->Pointer() + info.offset() + metaSize, dataSz, getStr);
    ASSERT_EQ(hash(str), hash(getStr));

    // DecRef.
    DecreaseRef(clientId, shmId);

    // Delete.
    Delete(clientId, objectKey);
}

TEST_F(WorkerOCServiceImplTest, TestMultiCreate)
{
    auto clientId = ClientKey::Intern("c1");
    uint32_t lockId;
    DS_ASSERT_OK(worker::ClientManager::Instance().AddClient(clientId, true, true, "", "", "", "", lockId));

    MultiCreateReqPb req;
    std::string objectKey = "o1";
    size_t dataSz = 10'000'000;
    auto str = random_.GetRandomString(dataSz);

    req.set_client_id(clientId);
    req.add_object_key(objectKey);
    req.add_data_size(str.size());

    MultiCreateRspPb resp;
    DS_ASSERT_OK(workerOcServiceImpl->MultiCreate(req, resp));
}

TEST_F(WorkerOCServiceImplTest, DISABLED_TestSealAfterSealVarySz)
{
    auto clientId = ClientKey::Intern("c1");
    datasystem::worker::ClientManager::Instance().AddClient(clientId, 1);

    // NoShm Put.
    std::string shmId;
    int64_t dataSz = 100'000;
    std::string objectKey = "o1";
    auto str = random_.GetRandomString(dataSz);
    Publish(clientId, objectKey, dataSz, shmId, GetPayloads(str));
    Seal(clientId, objectKey, dataSz, shmId, GetPayloads(str));

    // Create.
    CreateRspPb createRspPb;
    int64_t bigDataSz = 600'000;
    auto bigStr = random_.GetRandomString(bigDataSz);
    Create(clientId, objectKey, bigStr, createRspPb);

    // Write.
    std::shared_ptr<client::IMmapTableEntry> mmapTableEntry;
    Write(createRspPb, bigStr, mmapTableEntry);

    // Publish.
    DS_ASSERT_NOT_OK(Publish(clientId, objectKey, bigDataSz, createRspPb.shm_id()));
    auto pimpl1 = std::make_unique<ServerUnaryWriterReaderMock>(clientId, objectKey, akSkManager_);
    auto mockReaderWriter1 = std::make_shared<ServerUnaryWriterReader<GetRspPb, GetReqPb>>(std::move(pimpl1));
    DS_ASSERT_NOT_OK(workerOcServiceImpl->Get(mockReaderWriter1));
    DS_ASSERT_NOT_OK(Seal(clientId, objectKey, bigDataSz, createRspPb.shm_id()));

    // Get should fail.
    auto pimpl2 = std::make_unique<ServerUnaryWriterReaderMock>(clientId, objectKey, akSkManager_);
    auto mockReaderWriter2 = std::make_shared<ServerUnaryWriterReader<GetRspPb, GetReqPb>>(std::move(pimpl2));
    DS_ASSERT_NOT_OK(workerOcServiceImpl->Get(mockReaderWriter2));
}

TEST_F(WorkerOCServiceImplTest, TestObjectTableDeadlock)
{
    ClientKey clientId = ClientKey::Intern("c1");
    DS_ASSERT_OK(worker::ClientManager::Instance().AddClient(clientId, 1));
    size_t dataSz = 100;
    auto str = random_.GetRandomString(dataSz);

    std::string objectKey = "o1";
    GIncreaseRef(clientId, objectKey);

    VLOG(1) << ".......... Publish";
    DS_ASSERT_OK(Publish(clientId, objectKey, dataSz, "", GetPayloads(str)));
    VLOG(1) << "Finished Publish";

    datasystem::inject::Set("worker.FillObjData.Start", "1*sleep(500)");
    datasystem::inject::Set("worker.ClearObject.BeforeErase", "1*sleep(1000)");
    HostPort metaAddress;
    cluster_->GetMetaServerAddr(metaAddress);
    ThreadPool threadPool(2);
    auto fut1 = threadPool.Submit([&]() { workerOcServiceImpl->PushMetadataToMaster(metaAddress); });
    auto fut2 = threadPool.Submit([&]() { workerOcServiceImpl->RefreshMeta(clientId); });
    fut1.get();
    fut2.get();
}

TEST_F(WorkerOCServiceImplTest, LEVEL2_TestRemoveMetaLocation)
{
    datasystem::inject::Set("voluntaryscaledown.task.taskisrunning", "return()");
    std::shared_ptr<KVClient> client;
    ConnectOptions connectOptions;
    HostPort workerAddr;
    cluster_->GetWorkerAddr(0, workerAddr);
    connectOptions = { .host = workerAddr.Host(), .port = workerAddr.Port() };
    client = std::make_shared<KVClient>(connectOptions);
    DS_ASSERT_OK(client->Init());
    std::vector<std::string> keys;
    SetParam param = { .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    for (int i = 0; i < 20; i++) {  // Generate 20 objects
        std::string key = "KEY_REMOVELOCATIOIN_" + std::to_string(i);
        DS_ASSERT_OK(client->Set(key, "aaaaaaaaa", param));
    }
    auto clientId = ClientKey::Intern("c1");
    DS_ASSERT_OK(worker::ClientManager::Instance().AddClient(clientId, 1));
    for (int i = 0; i < 20; i++) {  // Generate 20 objects
        std::string key = "KEY_REMOVELOCATIOIN_" + std::to_string(i);
        keys.emplace_back(key);
        auto pimpl = std::make_unique<ServerUnaryWriterReaderMock>(clientId, key, akSkManager_);
        auto mockReaderWriter1 = std::make_shared<ServerUnaryWriterReader<GetRspPb, GetReqPb>>(std::move(pimpl));
        DS_ASSERT_OK(workerOcServiceImpl->Get(mockReaderWriter1));
    }
    sleep(5);  // Wait 5 seconds for objects manipulation finished
    std::stringstream table;
    table << ETCD_LOCATION_TABLE_PREFIX << ETCD_HASH_SUFFIX;
    DS_ASSERT_OK(workerOcServiceImpl->ProcessVoluntaryScaledown(""));
    for (auto key : keys) {
        uint32_t hash = MurmurHash3_32(key);
        auto locationKey1 = master::Hash2Str(hash) + "/" + localAddress_.ToString() + "_" + key;
        DS_ASSERT_NOT_OK(etcdStore_->Delete(table.str(), locationKey1));
    }
}
}  // namespace st
};  // namespace datasystem
