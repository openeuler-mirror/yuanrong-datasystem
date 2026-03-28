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
 * Description: Test interface to HashRing
 */
#include <google/protobuf/util/message_differencer.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>
#include <thread>

#include "common.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/container_util.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/common/log/log.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"
#include "datasystem/worker/hash_ring/hash_ring.h"
#include "datasystem/worker/worker_cli.h"

using namespace datasystem::worker;

DS_DECLARE_string(etcd_address);
DS_DECLARE_string(master_address);
DS_DECLARE_uint32(add_node_wait_time_s);
DS_DECLARE_uint32(node_dead_timeout_s);
DS_DECLARE_bool(auto_del_dead_node);

namespace datasystem {
namespace st {
namespace {
// The valid range is [start, end)
struct Range {
    uint32_t start;
    uint32_t end;
};

WorkerPb MakeWorkerPb(std::initializer_list<uint32_t> &&tokens, WorkerPb::StatePb state = WorkerPb::ACTIVE)
{
    WorkerPb pb;
    for (auto token : tokens) {
        pb.mutable_hash_tokens()->Add(std::move(token));
    }
    pb.set_state(state);
    return pb;
}

void InsertWorker(HashRingPb &pb, const std::string &id, WorkerPb &&workerPb)
{
    workerPb.set_worker_uuid(GetStringUuid());
    pb.mutable_workers()->insert({ id, workerPb });
}
}  // namespace

constexpr static int HASH_RING_NUM_TWO = 2;
constexpr static int HASH_RING_NUM_THREE = 3;
constexpr static int DEFAULT_ADD_NODE_WAIT_TIME_S = 3;
constexpr static int CHECK_INTERVAL_MS = 10;

class TestHashRing : public HashRing {
public:
    /**
     * @brief Get the primary worker addr by consistent hash algorithm.
     * @param[in] key Use the key to calculate the uint32 hash value, and then find the first node that is greater than
     * the hash value on the consistent hash ring.
     * @param[out] outWorkerAddr the outWorkerAddr is calc by consistent hash algorithm when enable consistent
     * hash(enable_distribute_master is true and etcd_address is valid);
     * @return Status of the call.
     */
    Status GetPrimaryWorkerAddr(const std::string &key, std::string &outWorkerAddr) const;

    /**
     * @brief Get the hash tokens of the hash ring.
     * @return Return the hash tokens of the hash ring.
     */
    std::vector<uint32_t> GetHashTokens() const;

    /**
     * @brief Get the primary worker by consistent hash algorithm.
     * @param[in] keyHash Find the first node that is greater than the keyHash on the consistent hash ring.
     * @param[out] outWorkerAddr the outWorkerAddr is calc by consistent hash algorithm when enable consistent
     * @return Status of the call.
     */
    Status GetPrimaryWorkerAddr(uint32_t keyHash, std::string &outWorkerAddr) const;
};

Status TestHashRing::GetPrimaryWorkerAddr(const std::string &key, std::string &outWorkerAddr) const
{
    uint32_t hash = hashFunction_(key);
    return HashRing::GetPrimaryWorkerAddr(hash, outWorkerAddr);
}

std::vector<uint32_t> TestHashRing::GetHashTokens() const
{
    std::vector<uint32_t> ret;
    if (state_.load() != RUNNING) {
        return {};
    }
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    std::transform(tokenMap_.begin(), tokenMap_.end(), std::back_inserter(ret),
                   [](const auto &kv) { return kv.first; });
    return ret;
}

Status TestHashRing::GetPrimaryWorkerAddr(uint32_t keyHash, std::string &outWorkerAddr) const
{
    return HashRing::GetPrimaryWorkerAddr(keyHash, outWorkerAddr);
}

class HashRingTest : public ExternalClusterTest {
protected:
    HashRingTest()
    {
    }
    ~HashRingTest() = default;

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numMasters = 0;
        opts.numWorkers = 0;
        FLAGS_v = 1;
        FLAGS_add_node_wait_time_s = DEFAULT_ADD_NODE_WAIT_TIME_S;
    }

    void TearDown() override
    {
        exit_ = true;
        for (auto &cm : etcdCMs_) {
            if (cm) {
                cm->Shutdown();
                cm.reset();
            }
        }
        for (auto &db : etcdStores_) {
            if (db) {
                db->Shutdown();
                db.reset();
            }
        }
        ExternalClusterTest::TearDown();
    }

protected:
    void InitTestEtcdInstance();
    void PutRingToEtcd(const std::string &jsonRing);
    void InitRing(uint32_t workerNum);
    void ResetRing();
    void RestartRing(int workerIndex);
    void CheckAllRunning();
    void CheckAllRunningAndTokens(const std::vector<uint32_t> &expectTokens);
    void CheckRange(const std::vector<std::vector<Range>> &workerRange);
    void CheckRingInEtcd(const std::string &jsonRing, int timeoutMs);
    TestHashRing *ConstructAndGetRing(const std::string &addrStr)
    {
        HostPort addr;
        addr.ParseString(addrStr);
        etcdStores_.emplace_back(std::make_unique<EtcdStore>(FLAGS_etcd_address));
        etcdStores_.back()->Init();
        etcdCMs_.emplace_back(std::make_unique<EtcdClusterManager>(addr, addr, etcdStores_.back().get(), false));
        rings_.emplace_back(static_cast<TestHashRing *>(etcdCMs_.back()->GetHashRing()));
        ClusterInfo clusterInfo;
        DS_EXPECT_OK(EtcdClusterManager::ConstructClusterInfoViaEtcd(etcdStores_.back().get(), clusterInfo));
        DS_EXPECT_OK(etcdCMs_.back()->Init(clusterInfo));
        etcdCMs_.back()->SetWorkerReady();
        return static_cast<TestHashRing *>(etcdCMs_.back()->GetHashRing());
    }
    std::unique_ptr<EtcdStore> db_;
    std::vector<std::unique_ptr<EtcdClusterManager>> etcdCMs_;  // just used for construction of hash rings.
    std::vector<std::unique_ptr<EtcdStore>> etcdStores_;        // Each EtcdStore is used for each EtcdClusterManager.
    std::unique_ptr<ThreadPool> threadPool_{ nullptr };
    std::vector<std::future<Status>> futures_;
    std::vector<TestHashRing *> rings_;
    std::vector<std::string> workerIds_;
    std::atomic<bool> exit_{ false };
    // The key range for worker0: [4294967292, 357913941), [1073741823, 1431655764),
    //                            [2147483646, 2505397587), [3221225469, 3579139410)
    // The key range for worker1: [357913941, 715827882 ), [1431655764, 1789569705),
    //                            [2505397587, 2863311528), [3579139410, 3937053351)
    // The key range for worker2: [715827882, 1073741823), [1789569705, 2147483646),
    //                            [2863311528, 3221225469), [3937053351, 4294967292)
    // tokens for all workers:         worker0     worker1       worker2
    std::vector<uint32_t> threeWorkerFourVirtualNodeRingTokens_{ 357913941,  715827882,  1073741823, 1431655764,
                                                                 1789569705, 2147483646, 2505397587, 2863311528,
                                                                 3221225469, 3579139410, 3937053351, 4294967292 };
    std::vector<std::vector<Range>> threeWorkerFourVirtualNodeWorkerRange_{ { { 4294967292, 357913941 },
                                                                              { 1073741823, 1431655764 },
                                                                              { 2147483646, 2505397587 },
                                                                              { 3221225469, 3579139410 } },
                                                                            { { 357913941, 715827882 },
                                                                              { 1431655764, 1789569705 },
                                                                              { 2505397587, 2863311528 },
                                                                              { 3579139410, 3937053351 } },
                                                                            { { 715827882, 1073741823 },
                                                                              { 1789569705, 2147483646 },
                                                                              { 2863311528, 3221225469 },
                                                                              { 3937053351, 4294967292 } } };
};

void HashRingTest::InitTestEtcdInstance()
{
    std::string etcdAddress;
    for (size_t i = 0; i < cluster_->GetEtcdNum(); ++i) {
        std::pair<HostPort, HostPort> addrs;
        cluster_->GetEtcdAddrs(i, addrs);
        if (!etcdAddress.empty()) {
            etcdAddress += ",";
        }
        etcdAddress += addrs.first.ToString();
    }
    FLAGS_etcd_address = etcdAddress;
    LOG(INFO) << "The etcd address is:" << FLAGS_etcd_address << std::endl;
    db_ = std::make_unique<EtcdStore>(etcdAddress);
    if ((db_ != nullptr) && (db_->Init().IsOk())) {
        db_->DropTable(ETCD_RING_PREFIX);
        // We don't check rc here. If table to drop does not exist, it's fine.
        (void)db_->CreateTable(ETCD_RING_PREFIX, ETCD_RING_PREFIX);
    }
}

void HashRingTest::PutRingToEtcd(const std::string &jsonRing)
{
    HashRingPb hashRing;
    auto rc = google::protobuf::util::JsonStringToMessage(jsonRing, &hashRing);
    ASSERT_TRUE(rc.ok()) << rc.ToString();
    ASSERT_EQ(db_->Put(ETCD_RING_PREFIX, "", hashRing.SerializeAsString()), Status::OK());
}

void HashRingTest::CheckRingInEtcd(const std::string &jsonRing, int timeoutMs)
{
    HashRingPb hashRing;
    auto rc = google::protobuf::util::JsonStringToMessage(jsonRing, &hashRing);
    ASSERT_TRUE(rc.ok()) << rc.ToString();

    Timer timer;
    HashRingPb currRing;
    while (timer.ElapsedMilliSecond() < timeoutMs) {
        std::string ringStr;
        (void)db_->Get(ETCD_RING_PREFIX, "", ringStr);
        if (!currRing.ParseFromString(ringStr)) {
            continue;
        }
        if (google::protobuf::util::MessageDifferencer::Equals(hashRing, currRing)) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));  // 200ms
    }
    ASSERT_TRUE(false) << "The ring in etcd is not as expected in " << timeoutMs << "ms."
                       << "hashring:" << currRing.DebugString();
}

void HashRingTest::InitRing(uint32_t workerNum)
{
    rings_.resize(workerNum);
    workerIds_.resize(workerNum);
    threadPool_ = std::make_unique<ThreadPool>(workerNum);

    for (uint32_t i = 0; i < workerNum; i++) {
        HostPort addr;
        addr.ParseString("127.0.0.1:" + std::to_string(i));
        workerIds_[i] = addr.ToString();
        LOG(INFO) << "Ready to init for " << workerIds_[i];
        etcdStores_.emplace_back(std::make_unique<EtcdStore>(FLAGS_etcd_address));
        etcdStores_.back()->Init();
        etcdCMs_.emplace_back(std::make_unique<EtcdClusterManager>(addr, addr, etcdStores_.back().get(), false));
        rings_[i] = static_cast<TestHashRing *>(etcdCMs_.back()->GetHashRing());
        futures_.emplace_back(threadPool_->Submit([this, i]() {
            ClusterInfo clusterInfo;
            RETURN_IF_NOT_OK(EtcdClusterManager::ConstructClusterInfoViaEtcd(etcdStores_[i].get(), clusterInfo));
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(etcdCMs_[i]->Init(clusterInfo), "etcd cm init failed.");
            etcdCMs_[i]->SetWorkerReady();
            while (!this->rings_[i]->IsRunning() && !this->exit_.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(CHECK_INTERVAL_MS));
            }
            LOG(INFO) << workerIds_[i] << " finished init and is running ?" << this->rings_[i]->IsRunning();
            return Status::OK();
        }));
    }
}

void HashRingTest::ResetRing()
{
    exit_ = true;
    for (auto &future : futures_) {
        (void)future.get();
    }
    futures_.clear();
    rings_.clear();
    threadPool_.reset();
    exit_ = false;
}

void HashRingTest::RestartRing(int workerIndex)
{
    CHECK_LT(workerIndex, (int)rings_.size());
    CHECK_EQ(rings_.size(), workerIds_.size());
    LOG(INFO) << "Start to shutdown worker " << workerIds_[workerIndex];
    rings_[workerIndex] = nullptr;
    etcdCMs_[workerIndex].reset();
    std::this_thread::sleep_for(std::chrono::seconds(1));
    LOG(INFO) << "Start to restart worker " << workerIds_[workerIndex];
    HostPort addr;
    addr.ParseString("127.0.0.1:" + std::to_string(workerIndex));
    etcdStores_[workerIndex].reset();
    etcdStores_[workerIndex] = std::make_unique<EtcdStore>(FLAGS_etcd_address);
    etcdStores_[workerIndex]->Init();
    etcdCMs_.emplace(etcdCMs_.begin() + workerIndex,
                     std::make_unique<EtcdClusterManager>(addr, addr, etcdStores_[workerIndex].get(), false));
    const auto &cm = etcdCMs_[workerIndex];
    ClusterInfo clusterInfo;
    DS_ASSERT_OK(EtcdClusterManager::ConstructClusterInfoViaEtcd(etcdStores_[workerIndex].get(), clusterInfo));
    DS_ASSERT_OK(cm->Init(clusterInfo));
    cm->SetWorkerReady();
    rings_[workerIndex] = static_cast<TestHashRing *>(cm->GetHashRing());
    futures_.emplace_back(threadPool_->Submit([this, workerIndex]() {
        while (!this->rings_[workerIndex]->IsRunning() && !this->exit_.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(CHECK_INTERVAL_MS));
        }
        LOG(INFO) << workerIds_[workerIndex] << " finished init and is running ?"
                  << this->rings_[workerIndex]->IsRunning();
        return Status::OK();
    }));
}

void HashRingTest::CheckAllRunning()
{
    for (auto &future : futures_) {
        auto status = future.get();
        EXPECT_EQ(status, Status::OK());
    }
    futures_.clear();
}

void HashRingTest::CheckAllRunningAndTokens(const std::vector<uint32_t> &expectTokens)
{
    CheckAllRunning();
    for (auto ring : rings_) {
        ASSERT_TRUE(ring->IsRunning());
        std::vector<uint32_t> outTokens = ring->GetHashTokens();
        EXPECT_EQ(outTokens, expectTokens);
    }
}

void HashRingTest::CheckRange(const std::vector<std::vector<Range>> &workerRange)
{
    for (size_t i = 0; i < workerRange.size(); i++) {
        for (size_t j = 0; j < workerRange[i].size(); j++) {
            int start = workerRange[i][j].start;
            int end = workerRange[i][j].end;
            std::string &workerId = workerIds_[i];
            auto &ring = rings_[i];
            std::string addr;
            DS_ASSERT_OK(ring->GetPrimaryWorkerAddr(start, addr));
            EXPECT_EQ(addr, workerId);
            DS_ASSERT_OK(ring->GetPrimaryWorkerAddr(end, addr));
            EXPECT_NE(addr, workerId);
            if (end > 0) {
                DS_ASSERT_OK(ring->GetPrimaryWorkerAddr(end - 1, addr));
                EXPECT_EQ(addr, workerId);
            }
            if (start > end) {
                DS_ASSERT_OK(ring->GetPrimaryWorkerAddr(UINT32_MAX, addr));
                EXPECT_EQ(addr, workerId);
                DS_ASSERT_OK(ring->GetPrimaryWorkerAddr(0, addr));
                EXPECT_EQ(addr, workerId);
            }
        }
    }
}

TEST_F(HashRingTest, SetInitWorkerNumBeforeStart)
{
    InitTestEtcdInstance();
    InitRing(HASH_RING_NUM_THREE);
    CheckAllRunningAndTokens(threeWorkerFourVirtualNodeRingTokens_);
    CheckRange(threeWorkerFourVirtualNodeWorkerRange_);
}

TEST_F(HashRingTest, WillNotChangeWhenRestartWorker)
{
    InitTestEtcdInstance();
    InitRing(HASH_RING_NUM_THREE);
    CheckAllRunningAndTokens(threeWorkerFourVirtualNodeRingTokens_);
    std::vector<std::string> beforeRestart;
    for (auto ring : rings_) {
        beforeRestart.emplace_back(ring->GetLocalWorkerUuid());
    }
    RestartRing(0);
    CheckAllRunningAndTokens(threeWorkerFourVirtualNodeRingTokens_);
    for (int i = 0; i < HASH_RING_NUM_THREE; i++) {
        ASSERT_EQ(rings_[i]->GetLocalWorkerUuid(), beforeRestart[i]);
    }
}

TEST_F(HashRingTest, AddNodeToWorkingRing)
{
    FLAGS_add_node_wait_time_s = 0;
    datasystem::inject::Set("HashRing.SubmitScaleUpTask.skip", "return(1)");

    InitTestEtcdInstance();
    InitRing(HASH_RING_NUM_TWO);
    for (auto &future : futures_) {
        auto status = future.get();
        ASSERT_EQ(status, Status::OK());
    }
    futures_.clear();

    auto newWorker = "127.0.0.1:4";
    TestHashRing *ring = ConstructAndGetRing(newWorker);
    auto future = threadPool_->Submit([this, ring, newWorker]() {
        while (!ring->IsRunning() && !this->exit_.load()) {
            sleep(1);
        }
        LOG(INFO) << newWorker << " finished init and is running ?" << ring->IsRunning();
        return Status::OK();
    });
    auto status = future.get();
    ASSERT_EQ(status, Status::OK());

    EXPECT_EQ(rings_[0]->GetHashTokens(), ring->GetHashTokens());
    EXPECT_EQ(rings_[0]->GetHashTokens().size(), 12U);
}

// add multiple nodes in one scale-up operation + add nodes in multiple scale-up operations
TEST_F(HashRingTest, LEVEL2_AddNodesInBatch)
{
    constexpr int waitTime = 5;
    FLAGS_add_node_wait_time_s = waitTime;

    datasystem::inject::Set("HashRing.SubmitScaleUpTask.skip", "return(1)");

    InitTestEtcdInstance();
    InitRing(HASH_RING_NUM_TWO);
    for (auto &future : futures_) {
        auto status = future.get();
        ASSERT_EQ(status, Status::OK());
    }
    futures_.clear();

    auto workers = { "127.0.0.1:4", "127.0.0.1:5", "127.0.0.1:6", "127.0.0.1:3" };
    std::vector<std::thread> threads;

    // add one worker every two seconds
    for (auto &newWorker : workers) {
        auto ring = ConstructAndGetRing(newWorker);
        futures_.emplace_back(threadPool_->Submit(([this, ring, newWorker]() {
            while (!ring->IsRunning() && !this->exit_.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(CHECK_INTERVAL_MS));
            }
            LOG(INFO) << newWorker << " finished init and is running ?" << ring->IsRunning();
            return Status::OK();
        })));
        constexpr int addInterval = 2;
        sleep(addInterval);
    }

    // wait for all workers to join
    for (auto &future : futures_) {
        auto status = future.get();
        ASSERT_EQ(status, Status::OK());
    }
    sleep(1);  // receive event delay

    // expect tokens in each ring are the same, and the master service is ready.
    auto expectTokens = rings_[0]->GetHashTokens();
    HostPort master;
    for (auto &ring : rings_) {
        ASSERT_EQ(ring->GetHashTokens(), expectTokens);
        DS_ASSERT_OK(ring->GetMasterAddr("any_key", master));
    }
    EXPECT_EQ(expectTokens.size(), rings_.size() * workers.size());
}

TEST_F(HashRingTest, InitInDifferentState)
{
    InitTestEtcdInstance();

    HashRingPb ringPb;
    ringPb.set_cluster_id("");
    ringPb.set_cluster_has_init(true);
    InsertWorker(ringPb, "127.0.0.1:19562", MakeWorkerPb({ 357913941, 1431655764, 2505397587, 3579139410 }));
    InsertWorker(ringPb, "127.0.0.1:25428", MakeWorkerPb({ 715827882, 1789569705, 2863311528, 3937053351 }));
    InsertWorker(ringPb, "127.0.0.1:42753", MakeWorkerPb({ 1073741823, 2147483646, 3221225469, 3221225469 }));
    InsertWorker(ringPb, "127.0.0.1:22746",
                 MakeWorkerPb({ 196852666, 554766608, 912680549, 1270594490 }, WorkerPb::JOINING));
    InsertWorker(ringPb, "127.0.0.1:33333", MakeWorkerPb({}, WorkerPb::INITIAL));
    ASSERT_EQ(db_->Put(ETCD_RING_PREFIX, "", ringPb.SerializeAsString()), Status::OK());

    HostPort master;
    auto activeRestartRing = ConstructAndGetRing("127.0.0.1:42753");
    ASSERT_TRUE(activeRestartRing->IsRunning());  // for restart active node, must be running after Init()
    DS_ASSERT_OK(activeRestartRing->GetMasterAddr("redirect_test_4", master));
    ASSERT_EQ(master.ToString(), "127.0.0.1:19562");  // hash ring service is ready

    auto newRing = ConstructAndGetRing("127.0.0.1:44444");
    ASSERT_FALSE(newRing->IsRunning());
    ASSERT_TRUE(newRing->IsWorkable());  // for newly-added node, must be pre-running after Init()
    DS_ASSERT_OK(newRing->GetMasterAddr("redirect_test_4", master));
    ASSERT_EQ(master.ToString(), "127.0.0.1:19562");  // hash ring service is ready

    auto initRestartRing = ConstructAndGetRing("127.0.0.1:33333");
    ASSERT_FALSE(initRestartRing->IsRunning());
    ASSERT_TRUE(initRestartRing->IsWorkable());  // for restart init node, must be pre-running after Init()
    DS_ASSERT_OK(initRestartRing->GetMasterAddr("redirect_test_4", master));
    ASSERT_EQ(master.ToString(), "127.0.0.1:19562");  // hash ring service is ready

    auto joiningRestartRing = ConstructAndGetRing("127.0.0.1:22746");
    ASSERT_FALSE(joiningRestartRing->IsRunning());
    ASSERT_TRUE(joiningRestartRing->IsWorkable());  // for restart joining node, must be pre-running after Init()
    DS_ASSERT_OK(joiningRestartRing->GetMasterAddr("redirect_test_4", master));
    ASSERT_EQ(master.ToString(), "127.0.0.1:19562");  // hash ring service is ready
}

TEST_F(HashRingTest, LEVEL1_TestNeedRedirect)
{
    constexpr int waitTime = 3;
    FLAGS_add_node_wait_time_s = waitTime;

    datasystem::inject::Set("HashRing.SubmitScaleUpTask.skip", "return(10)");
    datasystem::inject::Set("MurmurHash3", "100*return()");

    InitTestEtcdInstance();
    InitRing(HASH_RING_NUM_TWO);
    for (auto &future : futures_) {
        auto status = future.get();
        ASSERT_EQ(status, Status::OK());
    }
    futures_.clear();

    std::string key = "a_key_hash_to_1073741824";  // expect on worker0, will migrate to worker2 later
    HostPort masterAddr;
    std::string dbName;
    DS_ASSERT_OK(rings_[0]->GetMasterAddr(key, masterAddr));
    EXPECT_EQ(masterAddr.ToString(), "127.0.0.1:0");
    EXPECT_FALSE(rings_[0]->NeedRedirect(key, masterAddr));

    auto newWorker = "127.0.0.1:4";
    auto ring = ConstructAndGetRing(newWorker);
    auto future = threadPool_->Submit([this, ring, newWorker]() {
        while (!ring->IsRunning() && !this->exit_.load()) {
            sleep(1);
        }
        LOG(INFO) << newWorker << " finished init and is running ?" << ring->IsRunning();
        return Status::OK();
    });

    // wait for less than HashRing.SubmitScaleUpTask.skip sleep time, expect node is migrating.
    using namespace std::chrono_literals;
    future.wait_for(5s);
    // hit the migrating branch, redirect successfully
    EXPECT_TRUE(rings_[0]->NeedRedirect(key, masterAddr));
    EXPECT_EQ(masterAddr.ToString(), newWorker);

    // new node has joined.
    auto status = future.get();
    ASSERT_EQ(status, Status::OK());
    // hit the branch of rehash after migration, redirect successfully
    EXPECT_TRUE(rings_[0]->NeedRedirect(key, masterAddr));
    EXPECT_EQ(masterAddr.ToString(), newWorker);
}

TEST_F(HashRingTest, GetPrimaryWorker)
{
    InitTestEtcdInstance();
    InitRing(HASH_RING_NUM_THREE);
    inject::Set("HashRing.UpdateRing.sleep", "sleep(2000)");
    std::this_thread::sleep_for(std::chrono::seconds(1));
    std::string userDataKey{ "userDataKey" };  // murmurhash(userDataKey) = 2350663102, fall into worker0
    for (auto &future : futures_) {
        auto status = future.get();
        EXPECT_EQ(status, Status::OK());
    }
    for (auto &r : rings_) {
        std::string outWorkerAddr;
        r->GetPrimaryWorkerAddr(userDataKey, outWorkerAddr);
        EXPECT_EQ(outWorkerAddr, "127.0.0.1:0");
    }
}

TEST_F(HashRingTest, LEVEL1_10Worker)
{
    InitTestEtcdInstance();
    constexpr uint32_t workerNum = 10;
    InitRing(workerNum);
    CheckAllRunning();
}

TEST_F(HashRingTest, RestartDonotModify)
{
    InitTestEtcdInstance();
    HashRingPb ringPb;
    ringPb.set_cluster_has_init(true);
    InsertWorker(ringPb, "127.0.0.1:0", MakeWorkerPb({ 357913941, 1431655764, 2505397587, 3579139410 }));
    InsertWorker(ringPb, "127.0.0.1:1", MakeWorkerPb({ 715827882, 1789569705, 2863311528, 3937053351 }));
    InsertWorker(ringPb, "127.0.0.1:2", MakeWorkerPb({ 1073741823, 2147483646, 3221225469, 4294967292 }));
    DS_ASSERT_OK(db_->Put(ETCD_RING_PREFIX, "", ringPb.SerializeAsString()));

    RangeSearchResult oldRes;
    DS_ASSERT_OK(db_->Get(ETCD_RING_PREFIX, "", oldRes));

    constexpr uint32_t workerNum = 3;
    InitRing(workerNum);
    CheckAllRunning();

    RangeSearchResult newRes;
    DS_ASSERT_OK(db_->Get(ETCD_RING_PREFIX, "", newRes));
    ASSERT_EQ(oldRes.modRevision, newRes.modRevision);
}

TEST_F(HashRingTest, StartRingDuringScalingDown)
{
    // init rings with a scale-down task
    InitTestEtcdInstance();
    FLAGS_add_node_wait_time_s = 0;
    std::string hashRingJsonStr = R"({
        "clusterHasInit": true,
        "workers": {
            "127.0.0.1:0": {"hashTokens": [90000, 270000], "workerUuid": "dXVpZDA=", "state": "ACTIVE"},
            "127.0.0.1:1": {"hashTokens": [60000, 120000], "workerUuid": "dXVpZDE=", "state": "ACTIVE"}
        },
        "delNodeInfo": {
            "127.0.0.1:0": {"changedRanges": [{"workerId": "not-exist-worker", "from": 120000, "end": 180000}]}
        }
    })";
    PutRingToEtcd(hashRingJsonStr);
    InitRing(HASH_RING_NUM_TWO);

    // the ring1 should wait for the former sacle-down task's completion so it will not be running for a whilie
    sleep(1);
    ASSERT_FALSE(rings_[0]->IsRunning());

    // finish the scale down task
    hashRingJsonStr = R"({
        "clusterHasInit": true,
        "workers": {
            "127.0.0.1:1": {"hashTokens": [90000, 270000], "workerUuid": "dXVpZDA=", "state": "ACTIVE"}
        }
    })";
    PutRingToEtcd(hashRingJsonStr);

    // expect the ring can change to running and get a new workerUuid. (skip out of date event)
    CheckAllRunning();
    ASSERT_TRUE(Validator::ValidateUuid("WorkerUuid", rings_[0]->GetLocalWorkerUuid()));
}

TEST_F(HashRingTest, StartScaleTasks)
{
    FLAGS_v = 2;  // log level is 2
    // init rings with a scale-down task
    datasystem::inject::Set("ClearDataDelay", "1*sleep(3000)");
    datasystem::inject::Set("notExcuteClearData", "call()");
    datasystem::inject::Set("notExcuteClearData.skip", "return()");
    InitTestEtcdInstance();
    FLAGS_add_node_wait_time_s = 0;
    std::string hashRingJsonStr = R"({
        "clusterHasInit": true,
        "workers": {
            "127.0.0.1:0": {"hashTokens": [90000, 270000], "workerUuid": "dXVpZDA=", "state": "ACTIVE"},
            "127.0.0.1:1": {"hashTokens": [60000, 120000], "workerUuid": "dXVpZDE=", "state": "ACTIVE"},
            "127.0.0.1:2": {"hashTokens": [0, 180000], "workerUuid": "cXdl", "state": "ACTIVE"},
            "127.0.0.1:3": {"hashTokens": [230000, 350000], "workerUuid": "Cnd3cWU=", "state": "ACTIVE"},
            "127.0.0.1:4": {"hashTokens": [320000, 380000], "workerUuid": "cXdlcm0=", "state": "ACTIVE"}
        },
    })";
    PutRingToEtcd(hashRingJsonStr);
    std::string hashRingJsonStr2 = R"({
        "clusterHasInit": true,
        "workers": {
            "127.0.0.1:0": {"hashTokens": [90000, 270000], "workerUuid": "dXVpZDA=", "state": "ACTIVE"},
            "127.0.0.1:1": {"hashTokens": [60000, 120000], "workerUuid": "dXVpZDE=", "state": "ACTIVE"},
            "127.0.0.1:2": {"hashTokens": [0, 180000], "workerUuid": "cXdl", "state": "ACTIVE"},
            "127.0.0.1:3": {"hashTokens": [230000, 350000], "workerUuid": "Cnd3cWU=", "state": "ACTIVE"},
            "127.0.0.1:4": {"hashTokens": [320000, 380000], "workerUuid": "cXdlcm0=", "state": "ACTIVE"}
        },
        "delNodeInfo": {
            "127.0.0.1:4": {"changedRanges": [{"workerId": "127.0.0.1:0", "from": 20000, "end": 30000}]}
        }
    })";
    std::string hashRingJsonStr3 = R"({
        "clusterHasInit": true,
        "workers": {
            "127.0.0.1:0": {"hashTokens": [90000, 270000], "workerUuid": "dXVpZDA=", "state": "ACTIVE"},
            "127.0.0.1:1": {"hashTokens": [60000, 120000], "workerUuid": "dXVpZDE=", "state": "ACTIVE"},
            "127.0.0.1:2": {"hashTokens": [0, 180000], "workerUuid": "cXdl", "state": "ACTIVE"},
            "127.0.0.1:3": {"hashTokens": [230000, 350000], "workerUuid": "Cnd3cWU=", "state": "ACTIVE"},
            "127.0.0.1:4": {"hashTokens": [320000, 380000], "workerUuid": "cXdlcm0=", "state": "ACTIVE"}
        },
        "delNodeInfo": {
            "127.0.0.1:4": {"changedRanges": [{"workerId": "127.0.0.1:0", "from": 20000, "end": 30000}]},
            "127.0.0.1:3": {"changedRanges": [{"workerId": "127.0.0.1:0", "from": 20000, "end": 50000}]}
        }
    })";
    std::vector<std::string> hashRings = { hashRingJsonStr2, hashRingJsonStr3 };
    InitRing(1);
    CheckAllRunning();
    for (const auto &ring : hashRings) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));  // interval is 200 ms;
        PutRingToEtcd(ring);
    }
    Timer timer;
    bool sucesss = false;
    while (timer.ElapsedSecond() < 5) {  // wait for 5s
        auto workers = rings_[0]->GetWorkersInDelNodeInfo();
        if (workers.find("127.0.0.1:4") == workers.end() && workers.find("127.0.0.1:3") == workers.end()) {
            sucesss = true;
            break;
        }
    }
    ASSERT_TRUE(sucesss);
}

TEST_F(HashRingTest, StartScaleTaskNotExcuteMultipleTimes)
{
    FLAGS_v = 2;  // log level is 2
    // init rings with a scale-down task
    datasystem::inject::Set("ClearDataDelay", "1*sleep(3000)");
    datasystem::inject::Set("notExcuteClearData", "call()");
    datasystem::inject::Set("notExcuteClearData.skip", "1*return()");
    InitTestEtcdInstance();
    FLAGS_add_node_wait_time_s = 0;
    std::string hashRingJsonStr = R"({
        "clusterHasInit": true,
        "workers": {
            "127.0.0.1:0": {"hashTokens": [90000, 270000], "workerUuid": "dXVpZDA=", "state": "ACTIVE"},
            "127.0.0.1:1": {"hashTokens": [60000, 120000], "workerUuid": "dXVpZDE=", "state": "ACTIVE"},
            "127.0.0.1:2": {"hashTokens": [0, 180000], "workerUuid": "cXdl", "state": "ACTIVE"},
            "127.0.0.1:3": {"hashTokens": [230000, 350000], "workerUuid": "Cnd3cWU=", "state": "ACTIVE"},
            "127.0.0.1:4": {"hashTokens": [320000, 380000], "workerUuid": "cXdlcm0=", "state": "ACTIVE"}
        },
    })";
    PutRingToEtcd(hashRingJsonStr);
    std::string hashRingJsonStr1 = R"({
        "clusterHasInit": true,
        "workers": {
            "127.0.0.1:0": {"hashTokens": [90000, 270000], "workerUuid": "dXVpZDA=", "state": "ACTIVE"},
            "127.0.0.1:1": {"hashTokens": [60000, 120000], "workerUuid": "dXVpZDE=", "state": "ACTIVE"},
            "127.0.0.1:2": {"hashTokens": [0, 180000], "workerUuid": "cXdl", "state": "ACTIVE"},
            "127.0.0.1:3": {"hashTokens": [230000, 350000], "workerUuid": "Cnd3cWU=", "state": "ACTIVE"},
            "127.0.0.1:4": {"hashTokens": [320000, 380000], "workerUuid": "cXdlcm0=", "state": "ACTIVE"}
        },
        "delNodeInfo": {
            "127.0.0.1:4": {"changedRanges": [{"workerId": "127.0.0.1:0", "from": 20000, "end": 30000}]}
        }
    })";
    std::string hashRingJsonStr2 = R"({
        "clusterHasInit": true,
        "workers": {
            "127.0.0.1:0": {"hashTokens": [90000, 270000], "workerUuid": "dXVpZDA=", "state": "ACTIVE"},
            "127.0.0.1:1": {"hashTokens": [60000, 120000], "workerUuid": "dXVpZDE=", "state": "ACTIVE"},
            "127.0.0.1:2": {"hashTokens": [0, 180000], "workerUuid": "cXdl", "state": "ACTIVE"},
            "127.0.0.1:3": {"hashTokens": [230000, 350000], "workerUuid": "Cnd3cWU=", "state": "ACTIVE"},
            "127.0.0.1:4": {"hashTokens": [320000, 380000], "workerUuid": "cXdlcm0=", "state": "ACTIVE"}
        },
        "delNodeInfo": {
            "127.0.0.1:4": {"changedRanges": [{"workerId": "127.0.0.1:0", "from": 20000, "end": 30000}]},
            "127.0.0.1:6": {"changedRanges": [{"workerId": "127.0.0.1:1", "from": 20000, "end": 30000}]}
        }
    })";
    std::string hashRingJsonStr3 = R"({
        "clusterHasInit": true,
        "workers": {
            "127.0.0.1:0": {"hashTokens": [90000, 270000], "workerUuid": "dXVpZDA=", "state": "ACTIVE"},
            "127.0.0.1:1": {"hashTokens": [60000, 120000], "workerUuid": "dXVpZDE=", "state": "ACTIVE"},
            "127.0.0.1:2": {"hashTokens": [0, 180000], "workerUuid": "cXdl", "state": "ACTIVE"},
            "127.0.0.1:3": {"hashTokens": [230000, 350000], "workerUuid": "Cnd3cWU=", "state": "ACTIVE"},
            "127.0.0.1:4": {"hashTokens": [320000, 380000], "workerUuid": "cXdlcm0=", "state": "ACTIVE"}
        },
        "delNodeInfo": {
            "127.0.0.1:4": {"changedRanges": [{"workerId": "127.0.0.1:0", "from": 20000, "end": 30000}]},
            "127.0.0.1:7": {"changedRanges": [{"workerId": "127.0.0.1:1", "from": 20000, "end": 30000}]}
        }
    })";
    std::string hashRingJsonStr4 = R"({
        "clusterHasInit": true,
        "workers": {
            "127.0.0.1:0": {"hashTokens": [90000, 270000], "workerUuid": "dXVpZDA=", "state": "ACTIVE"},
            "127.0.0.1:1": {"hashTokens": [60000, 120000], "workerUuid": "dXVpZDE=", "state": "ACTIVE"},
            "127.0.0.1:2": {"hashTokens": [0, 180000], "workerUuid": "cXdl", "state": "ACTIVE"},
            "127.0.0.1:3": {"hashTokens": [230000, 350000], "workerUuid": "Cnd3cWU=", "state": "ACTIVE"},
            "127.0.0.1:4": {"hashTokens": [320000, 380000], "workerUuid": "cXdlcm0=", "state": "ACTIVE"}
        },
        "delNodeInfo": {
            "127.0.0.1:4": {"changedRanges": [{"workerId": "127.0.0.1:0", "from": 20000, "end": 30000}]}
        }
    })";
    std::vector<std::string> hashRings = { hashRingJsonStr1, hashRingJsonStr2, hashRingJsonStr3, hashRingJsonStr4 };
    InitRing(1);
    CheckAllRunning();
    for (const auto &ring : hashRings) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));  // interval is 200 ms;
        PutRingToEtcd(ring);
    }
    Timer timer;
    bool sucesss = false;
    while (timer.ElapsedSecond() < 5) {  // wait for 5s
        auto workers = rings_[0]->GetWorkersInDelNodeInfo();
        if (workers.find("127.0.0.1:4") == workers.end()) {
            sucesss = true;
            break;
        }
    }
    ASSERT_TRUE(sucesss);
}

TEST_F(HashRingTest, RemoveInitNode)
{
    // init rings with a scale-down task
    datasystem::inject::Set("EtcdClusterManager.CheckWaitNodeTableComplete.waitTime", "call(2)");

    InitTestEtcdInstance();
    FLAGS_add_node_wait_time_s = 0;
    FLAGS_node_timeout_s = 2;       // 2s
    FLAGS_node_dead_timeout_s = 3;  // 3s
    FLAGS_auto_del_dead_node = true;

    std::string hashRingJsonStr = R"({
        "clusterHasInit": true,
        "workers": {
            "127.0.0.1:0": {"hashTokens": [90000, 270000], "workerUuid": "dXVpZDA=", "state": "ACTIVE"},
            "127.0.0.1:1": {"workerUuid": "dXVpZDE=", "state": "INITIAL"}
        },
    })";
    PutRingToEtcd(hashRingJsonStr);

    InitRing(1);
    CheckAllRunning();
    etcdCMs_[0]->CheckWaitNodeTableComplete();

    std::string expectRing = R"({
        "clusterHasInit": true,
        "workers": {
            "127.0.0.1:0": {"hashTokens": [90000, 270000], "workerUuid": "dXVpZDA=", "state": "ACTIVE"},
        },
        "keyWithWorkerIdMetaMap": {
            "uuid1": "127.0.0.1:0"
        }
    })";
    CheckRingInEtcd(expectRing, 5'000);  // wait for 5'000 ms
}

TEST_F(HashRingTest, EtcdWatchGet)
{
    TimerQueue::GetInstance()->Initialize();
    // init rings with a scale-down task
    datasystem::inject::Set("EtcdClusterManager.CheckWaitNodeTableComplete.waitTime", "call(2)");
    datasystem::inject::Set("EtcdWatch.RetrieveEventPassively.RetrieveEventQuickly", "call(100)");
    InitTestEtcdInstance();
    FLAGS_add_node_wait_time_s = 0;

    std::string hashRingJsonStr = R"({
        "clusterHasInit": true,
        "workers": {
            "127.0.0.1:0": {"hashTokens": [90000, 270000], "workerUuid": "dXVpZDA=", "state": "ACTIVE"},
            "127.0.0.1:1": {"hashTokens": [120000, 130000], "workerUuid": "dXVpZDE=", "state": "JOINING"},
        },
        "addNodeInfo": {
            "127.0.0.1:1": {"changedRanges": [{"workerId": "127.0.0.1:0", "from": 20000, "end": 25000},
            {"workerId": "127.0.0.1:0", "from": 25000, "end": 30000}]}
        }
    })";
    std::string hashRingJsonStr1 = R"({
        "clusterHasInit": true,
        "workers": {
            "127.0.0.1:0": {"hashTokens": [90000, 270000], "workerUuid": "dXVpZDA=", "state": "ACTIVE",
                            "needScaleDown": true},
            "127.0.0.1:1": {"hashTokens": [120000, 130000], "workerUuid": "dXVpZDE=", "state": "ACTIVE"}
        }
    })";

    std::string hashRingJsonStr2 = R"({
        "clusterHasInit": true,
        "workers": {
            "127.0.0.1:0": {"hashTokens": [90000, 270000], "workerUuid": "dXVpZDA=", "state": "LEAVING",
                            "needScaleDown": true},
            "127.0.0.1:1": {"hashTokens": [120000, 130000], "workerUuid": "dXVpZDE=", "state": "ACTIVE"}
        },
        "addNodeInfo": {
            "127.0.0.1:1": {"changedRanges": [{"workerId": "127.0.0.1:0", "from": 90000, "end": 270000}]}
        }
    })";
    PutRingToEtcd(hashRingJsonStr);
    InitRing(1);
    CheckAllRunning();
    etcdCMs_[0]->CheckWaitNodeTableComplete();
    datasystem::inject::Set("EtcdWatch.StoreEvents.IgnoreEvent", "sleep(2000)");
    PutRingToEtcd(hashRingJsonStr1);
    PutRingToEtcd(hashRingJsonStr2);
    std::string expectRing = R"({
        "clusterHasInit": true,
        "workers": {
            "127.0.0.1:0": {"hashTokens": [90000], "workerUuid": "dXVpZDA=", "state": "LEAVING", "needScaleDown": true},
            "127.0.0.1:1": {"hashTokens": [120000, 130000], "workerUuid": "dXVpZDE=", "state": "ACTIVE"}
        }
    })";
    CheckRingInEtcd(expectRing, 8'000);  // wait for 8'000 ms
}

TEST_F(HashRingTest, CasRetry)
{
    InitTestEtcdInstance();
    datasystem::inject::Set("EtcdClusterManager.CheckWaitNodeTableComplete.waitTime", "call(2)");
    datasystem::inject::Set("hashring.finishaddnodeinfo", "sleep(3000)");
    datasystem::inject::Set("waitUntilHealth", "return()");
    std::string hashRingJsonStr = R"({
        "clusterHasInit": true,
        "workers": {
            "127.0.0.1:0": {"hashTokens": [90000, 270000], "workerUuid": "dXVpZDA", "state": "ACTIVE",
                            "needScaleDown": true},
            "127.0.0.1:1": {"hashTokens": [120000, 130000], "workerUuid": "dXVpZDE=", "state": "ACTIVE"}
        }
        })";
    std::string hashRingJsonStr1 = R"({
        "clusterHasInit": true,
        "workers": {
            "127.0.0.1:0": {"hashTokens": [90000, 270000], "workerUuid": "dXVpZDA", "state": "ACTIVE",
                            "needScaleDown": true},
            "127.0.0.1:1": {"hashTokens": [120000, 130000], "workerUuid": "dXVpZDE=", "state": "ACTIVE"}
        },
        "addNodeInfo": {
            "127.0.0.1:1": {"changedRanges": [{"workerId": "127.0.0.1:0", "from": 90000, "end": 270000},
                                              {"workerId": "127.0.0.1:0", "from": 0, "end": 90000}]}
        }
    })";
    PutRingToEtcd(hashRingJsonStr);
    InitRing(2);  // ring size is 2
    CheckAllRunning();
    etcdCMs_[0]->CheckWaitNodeTableComplete();
    PutRingToEtcd(hashRingJsonStr1);
    sleep(1);  // wait for 2s
    datasystem::inject::Set("SendRpc.Failed.isKeepAliveTimeoutHandler", "return(K_RETRY_IF_LEAVING)");
    datasystem::inject::Set("notExit", "return()");
    std::string expectRing = R"({
        "clusterHasInit": true,
        "workers": {
            "127.0.0.1:1": {"hashTokens": [120000, 130000], "workerUuid": "dXVpZDE=", "state": "ACTIVE"}
        }
    })";
    sleep(5);  // wait for 5s
    datasystem::inject::Clear("SendRpc.Failed.isKeepAliveTimeoutHandler");
    Timer timer;
    HashRingPb ring;
    bool success = false;
    while (timer.ElapsedMilliSecond() < 10000) {  // timeout is 10000 ms
        std::string ringStr;
        std::this_thread::sleep_for(std::chrono::milliseconds(200));  // 200ms
        (void)db_->Get(ETCD_RING_PREFIX, "", ringStr);
        if (!ring.ParseFromString(ringStr)) {
            continue;
        }
        for (auto &worker : ring.workers()) {
            if (worker.second.state() != WorkerPb::ACTIVE) {
                continue;
            }
        }
        if (!ring.add_node_info().empty() || !ring.del_node_info().empty()) {
            continue;
        }
        success = true;
    }
    ASSERT_TRUE(success) << ring.DebugString();
}

TEST_F(HashRingTest, HashRingToJsonFile)
{
    InitTestEtcdInstance();
    HashRingPb ring;
    WorkerPb workerPb;
    workerPb.set_worker_uuid("5d941f6a-dd77-43ee-b3cf-40a1cf77489e");
    workerPb.set_state(WorkerPb::ACTIVE);
    workerPb.set_need_scale_down(true);
    std::vector<uint32_t> tokens = { 703003027, 1320750813, 2986027838, 1537794891 };
    for (auto token : tokens) {
        workerPb.mutable_hash_tokens()->Add(token);
    }
    ring.mutable_workers()->insert({ "127.0.0.1:9999", workerPb });
    ring.set_cluster_has_init(true);
    DS_ASSERT_OK(db_->Put(ETCD_RING_PREFIX, "", ring.SerializeAsString()));
    auto ringPath = GetTestCaseDataDir() + "/ring.json";
    DS_ASSERT_OK(cli::SaveHashRingToFile(ringPath));
    std::string jsonStr;
    DS_ASSERT_OK(ReadFileToString(ringPath, jsonStr));
    ASSERT_TRUE(jsonStr.find("127.0.0.1:9999") != std::string::npos);
}

TEST_F(HashRingTest, LoadHashRingFromJsonFile)
{
    InitTestEtcdInstance();
    auto ringPath = GetTestCaseDataDir() + "/ring.json";
    std::string jsonStr = R"(
        {
            "clusterId": "127.0.0.1:32044",
            "workers": {
                "127.0.0.1:47805": {
                    "hashTokens": [715827882, 1789569705, 2863311528, 3937053351],
                    "workerUuid": "OTJlYjE0NzctZDA3Yi00Y2NjLTk5MDEtZTVmYjg1ZjEzZjlh",
                    "state": "ACTIVE"
                },
                "127.0.0.1:53257": {
                    "hashTokens": [1073741823, 2147483646, 3221225469, 4294967292],
                    "workerUuid": "YWRjNzQyNGEtNGM1MC00ZmVhLTk5YTYtYzFkNDlkZDAxMmJi",
                    "state": "ACTIVE"
                },
                "127.0.0.1:14964": {
                    "hashTokens": [357913941, 1431655764, 2505397587, 3579139410],
                    "workerUuid": "ZTBmYWRlMDItMDIyOS00YzU3LWIwMmMtYzdhMjRhN2MwZDVm",
                    "state": "LEAVING",
                    "needScaleDown": true
                }
            },
            "clusterHasInit": true,
            "addNodeInfo": {
                "127.0.0.1:53257": {
                    "changedRanges": [
                        {
                            "workerId": "127.0.0.1:14964",
                            "from": 715827882,
                            "end": 1073741823
                        },
                        {
                            "workerId": "127.0.0.1:14964",
                            "from": 1789569705,
                            "end": 2147483646
                        },
                        {
                            "workerId": "127.0.0.1:14964",
                            "from": 2863311528,
                            "end": 3221225469
                        },
                        {
                            "workerId": "127.0.0.1:14964",
                            "from": 3937053351,
                            "end": 4294967292
                        }
                    ]
                }
            }
        }
    )";
    std::ofstream outFile(ringPath);
    ASSERT_TRUE(outFile.is_open());
    outFile << jsonStr;
    outFile.close();

    DS_ASSERT_OK(cli::UpdateHashRingFromFile(ringPath));
    HashRingPb ring;
    std::string ringRaw;
    DS_ASSERT_OK(db_->Get(ETCD_RING_PREFIX, "", ringRaw));
    ASSERT_TRUE(ring.ParseFromString(ringRaw));
    const size_t workerCount = 3;
    ASSERT_EQ(ring.workers_size(), workerCount);
    ASSERT_EQ(ring.add_node_info_size(), 1);
}

}  // namespace st
}  // namespace datasystem
