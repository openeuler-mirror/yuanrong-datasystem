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

#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdint>
#include <ctime>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <fstream>
#include <nlohmann/json.hpp>

#include "common.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/kv_client.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/object_client.h"
#include "datasystem/utils/status.h"
#include "oc_client_common.h"
#include "datasystem/utils/embedded_config.h"

DS_DECLARE_string(unix_domain_socket_dir);
DS_DECLARE_string(etcd_address);

using json = nlohmann::json;

namespace datasystem {
namespace st {
class EmbeddedClientTest : public OCClientCommon {
public:
    void TearDown() override
    {
        ExternalClusterTest::TearDown();
    }

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 0;
        opts.numEtcd = 1;
    }
};

TEST_F(EmbeddedClientTest, TestEmbeddedClient)
{
    auto pid0 = fork();
    if (pid0 == 0) {
        std::pair<HostPort, HostPort> etcdAddr;
        DS_ASSERT_OK(cluster_->GetEtcdAddrs(0, etcdAddr));
        std::string workerAddr = "127.0.0.1:" + std::to_string(GetFreePort());
        std::string logDir = cluster_->GetRootDir() + "/worker0/log/";
        std::string rocksdbDir = cluster_->GetRootDir() + "/worker0/rocksdb";
        LOG(INFO) << logDir << "-----------" << workerAddr;
        EmbeddedConfig config = EmbeddedConfig()
                                    .Address(workerAddr)
                                    .EtcdAddress(etcdAddr.first.ToString())
                                    .SharedMemorySizeMb(200)
                                    .LogDir(logDir)
                                    .RocksdbStoreDir(rocksdbDir);
        DS_ASSERT_OK(KVClient::InitEmbedded(config));
        DS_ASSERT_OK(KVClient::EmbeddedInstance().ShutDown());
        exit(0);
    }
    int status;
    waitpid(pid0, &status, 0);
}

TEST_F(EmbeddedClientTest, BasicKVTest)
{
    auto pid0 = fork();
    if (pid0 == 0) {
        std::pair<HostPort, HostPort> etcdAddr;
        DS_ASSERT_OK(cluster_->GetEtcdAddrs(0, etcdAddr));
        std::string workerAddr = "127.0.0.1:" + std::to_string(GetFreePort());
        std::string logDir = cluster_->GetRootDir() + "/worker0/log/";
        std::string rocksdbDir = cluster_->GetRootDir() + "/worker0/rocksdb";
        LOG(INFO) << logDir << "-----------" << workerAddr;
        EmbeddedConfig config = EmbeddedConfig()
                                    .Address(workerAddr)
                                    .EtcdAddress(etcdAddr.first.ToString())
                                    .SharedMemorySizeMb(200)
                                    .LogDir(logDir)
                                    .RocksdbStoreDir(rocksdbDir)
                                    .SetArgs({ { "v", "2" } });
        DS_ASSERT_OK(KVClient::InitEmbedded(config));
        auto &client = KVClient::EmbeddedInstance();
        {
            std::string key = "key";
            uint64_t size = 1024;
            auto data = GenRandomString(size);
            std::shared_ptr<Buffer> buf;
            DS_ASSERT_OK(client.Create(key, size, SetParam(), buf));
            ASSERT_NE(buf, nullptr);
            ASSERT_EQ(size, buf->GetSize());

            DS_ASSERT_OK(buf->WLatch());
            DS_ASSERT_OK(buf->MemoryCopy((void *)data.data(), size));
            DS_ASSERT_OK(client.Set(buf));
            DS_ASSERT_OK(buf->UnWLatch());

            Optional<Buffer> getBuffer;
            DS_ASSERT_OK(client.Get(key, getBuffer));
            ASSERT_EQ(size, getBuffer->GetSize());
            DS_ASSERT_OK(getBuffer->RLatch());
            AssertBufferEqual(*getBuffer, data);
            DS_ASSERT_OK(getBuffer->UnRLatch());
            buf.reset();
        }
        DS_ASSERT_OK(client.ShutDown());
        exit(0);
    }
    int status;
    waitpid(pid0, &status, 0);
}

class KVClientCoprocessTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 0;
        opts.numEtcd = 1;
    }

    void TearDown() override
    {
        ExternalClusterTest::TearDown();
    }

    void SetWorkerObsArgs()
    {
        HostPort obsAddr;
        cluster_->GetOBSAddr(0, obsAddr);
        configArgs_.emplace("l2_cache_type", "obs");
        configArgs_.emplace("obs_endpoint", obsAddr.ToString());
        configArgs_.emplace("obs_access_key", "3rtJpvkP4zowTDsx6XiE");
        configArgs_.emplace("obs_secret_key", "SJx5Zecs7SL7I6Au9XpylG9LwPF29kMwIxisI5Xs");
        configArgs_.emplace("obs_bucket", "test");
    }

    Status StartEmbeddedNode(size_t index)
    {
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(index < workerAdds_.size(), K_RUNTIME_ERROR, "worker size < index ");
        std::string rootDir = cluster_->GetRootDir() + "/worker" + std::to_string(index);
        std::string logDir = rootDir + "/log/";
        std::string rocksdbDir = rootDir + "/rocksdb";
        std::string healthPath = rootDir + "/health";
        auto configArgs = configArgs_;
        configArgs.emplace("log_monitor", "true");
        configArgs.emplace("health_check_path", healthPath);
        auto config =
            EmbeddedConfig()
                .Address(workerAdds_[index].ToString())
                .EtcdAddress(FLAGS_etcd_address)
                .SharedMemorySizeMb(2048)
                .LogDir(logDir)
                .RocksdbStoreDir(rocksdbDir)
                .UnixDomainSocketDir(FLAGS_unix_domain_socket_dir)
                .SetArgs(configArgs);
        return KVClient::InitEmbedded(config);
    }

    Status WaitWorkerReady(int index, int timeoutSec = 20)
    {
        std::string healthCheckPath = cluster_->GetRootDir() + "/worker" + std::to_string(index) + "/health";
        timeval now;
        gettimeofday(&now, NULL);
        time_t deadLine = now.tv_sec + timeoutSec;
        while (true) {
            if (access(healthCheckPath.c_str(), F_OK) != -1) {
                break;
            }
            timeval curr;
            gettimeofday(&curr, NULL);
            CHECK_FAIL_RETURN_STATUS(curr.tv_sec < deadLine, StatusCode::K_RUNTIME_ERROR,
                                     FormatString("CheckHealthFile timed out, %s", healthCheckPath));
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return Status::OK();
    }

    Status RunSecondEmbeddedClient(std::string key)
    {
        RETURN_IF_NOT_OK(StartEmbeddedNode(1));
        auto client2 = &KVClient::EmbeddedInstance();
        std::string k1(key);
        std::string v1got;
        auto s = client2->Get(k1, v1got, 10000); // wait for 10000 ms
        if (!s.IsOk()) {
            std::cerr << "Child Get key1 failed: " << s.ToString() << std::endl;
            return Status(K_RUNTIME_ERROR, "get failed");
        }
        std::string k2 = "key2_from_child";
        std::string v2 = "val2_from_child";
        s = client2->Set(k2, v2);
        if (!s.IsOk()) {
            std::cerr << "Child Set key2 failed: " << s.ToString() << std::endl;
            return Status(K_RUNTIME_ERROR, "set failed");
        }
        std::string finish;
        RETURN_IF_NOT_OK(client2->Get("finish", finish, 10000)); // wait for 10000 ms
        RETURN_IF_NOT_OK(client2->ShutDown());
        return Status::OK();
    }

    void InitTestKVClient(HostPort workerAddress, std::shared_ptr<KVClient> &client)
    {
        ConnectOptions connectOptions = { .host = workerAddress.Host(),
                                          .port = workerAddress.Port(),
                                          .connectTimeoutMs = 60 * 1000,
                                          .requestTimeoutMs = 0,
                                          .token = "",
                                          .clientPublicKey = "",
                                          .clientPrivateKey = "",
                                          .serverPublicKey = "",
                                          .accessKey = "QTWAOYTTINDUT2QVKYUC",
                                          .secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc" };

        client = std::make_shared<KVClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
    }

    void SetUp() override
    {
        for (int i = 0; i < 3; i++) {
            std::string addr = "127.0.0.1:" + std::to_string(GetFreePort());
            HostPort workerAddr;
            workerAddr.ParseString(addr);
            workerAdds_.emplace_back(workerAddr);
        }
        ExternalClusterTest::SetUp();
        std::pair<HostPort, HostPort> etcdAddr;
        DS_ASSERT_OK(cluster_->GetEtcdAddrs(0, etcdAddr));
        FLAGS_etcd_address = etcdAddr.first.ToString();
        if (enableObs_) {
            SetWorkerObsArgs();
        }
    }

    std::vector<HostPort> workerAdds_;
    bool enableObs_ = false;
    std::unordered_map<std::string, std::string> configArgs_;
};

TEST_F(KVClientCoprocessTest, TestLocalGetAndSetSuccess)
{
    auto pid0 = fork();
    if (pid0 == 0) {
        DS_ASSERT_OK(StartEmbeddedNode(0));
        auto client_ = &KVClient::EmbeddedInstance();
        std::string key = "key1";
        std::string value = "value1";
        ASSERT_EQ(client_->Set(key, value), Status::OK());
        std::string valueGet;
        ASSERT_EQ(client_->Get(key, valueGet), Status::OK());
        ASSERT_EQ(value, std::string(valueGet.data(), valueGet.size()));
        ASSERT_EQ(client_->Del(key), Status::OK());
        ASSERT_EQ(client_->Get(key, valueGet).GetCode(), StatusCode::K_NOT_FOUND);
        DS_ASSERT_OK(client_->ShutDown());
        exit(0);
    }
    int status;
    waitpid(pid0, &status, 0);
}

TEST_F(KVClientCoprocessTest, TestRemoteGetAndSetSuccess)
{
    auto pid0 = fork();
    if (pid0 == 0) {
        DS_ASSERT_OK(StartEmbeddedNode(0));
        auto client_ = &KVClient::EmbeddedInstance();
        std::shared_ptr<KVClient> client1;
        InitTestKVClient(workerAdds_[0], client1);

        std::string value1 = "value1";
        std::string key = client1->GenerateKey();
        ASSERT_EQ(client_->Set(key, value1), Status::OK());
        std::string valueGet;
        ASSERT_EQ(client1->Get(key, valueGet), Status::OK());
        ASSERT_EQ(value1, std::string(valueGet.data(), valueGet.size()));
        std::string value2 = "value2";
        ASSERT_EQ(client1->Set(key, value2), Status::OK());
        ASSERT_EQ(client1->Get(key, valueGet), Status::OK());
        ASSERT_EQ(value2, std::string(valueGet.data(), valueGet.size()));
        ASSERT_EQ(client1->Del(key), Status::OK());
        ASSERT_EQ(client_->Get(key, valueGet).GetCode(), StatusCode::K_NOT_FOUND);
        client1.reset();
        DS_ASSERT_OK(client_->ShutDown());
        exit(0);
    }
    int status;
    waitpid(pid0, &status, 0);
}

TEST_F(KVClientCoprocessTest, TestEmbeddedClientsGetAndSetSuccess)
{
    pid_t pid = fork();
    std::string k1 = "key1_from_parent";
    std::string v1 = "val1_from_parent";
    if (pid == 0) {
        DS_ASSERT_OK(RunSecondEmbeddedClient(k1));
        exit(0);
    }
    pid_t pid1 = fork();
    if (pid1 == 0) {
        DS_ASSERT_OK(StartEmbeddedNode(0));
        auto client_ = &KVClient::EmbeddedInstance();
        ASSERT_EQ(client_->Set(k1, v1), Status::OK());

        std::string k2 = "key2_from_child";
        std::string v2got;
        ASSERT_EQ(client_->Get(k2, v2got, 20000), Status::OK());  // wait for 10000 ms
        ASSERT_EQ(v2got, "val2_from_child");
        DS_ASSERT_OK(client_->Set("finish", "finish"));
        DS_ASSERT_OK(client_->ShutDown());
        exit(0);
    }
    int s0, s1;
    waitpid(pid, &s0, 0);
    waitpid(pid1, &s1, 0);
}

TEST_F(KVClientCoprocessTest, TestInitEmbeddedWithInvalidParam)
{
    auto config = EmbeddedConfig()
                      .Address("127.0.0.1:31504")
                      .EtcdAddress("127.0.0.1:2379")
                      .SharedMemorySizeMb(2048)
                      .SetArgs({ { "illegal_xx", "random_value" } });
    Status s = KVClient::InitEmbedded(config);
    ASSERT_EQ(s.GetCode(), static_cast<int>(StatusCode::K_INVALID));
}

TEST_F(KVClientCoprocessTest, TestMSetTxPerfSingleInstance)
{
    auto pid0 = fork();
    if (pid0 == 0) {
        DS_ASSERT_OK(StartEmbeddedNode(0));
        auto client_ = &KVClient::EmbeddedInstance();
        constexpr int kThreadNum = 1;
        constexpr int kKeysPerTx = 10;
        constexpr int kValSize = 1 * 1024;
        constexpr int kRounds = 10;

        std::vector<std::string> keys;
        std::vector<std::string> vals;
        std::vector<StringView> valViews;
        keys.reserve(kKeysPerTx);
        vals.reserve(kKeysPerTx);
        for (int i = 0; i < kKeysPerTx; ++i) {
            auto key = randomData_.GetRandomString(24) + "_" + std::to_string(i);
            keys.emplace_back(key);
            vals.emplace_back(randomData_.GetRandomString(kValSize));
            valViews.emplace_back(vals.back());
        }
        Timer timer;
        auto Worker = [&] {
            for (int r = 0; r < 1; ++r) {
                std::vector<std::string> failedIds;
                DS_ASSERT_OK(client_->MSet(keys, valViews, failedIds));
            }
        };

        std::vector<std::thread> thds(kThreadNum);
        for (auto &t : thds)
            t = std::thread(Worker);
        for (auto &t : thds)
            t.join();
        auto timeElapsedMilliSecond = timer.ElapsedMilliSecond();
        int64_t totalOps = kThreadNum * kRounds * kKeysPerTx;
        LOG(INFO) << "Single-instance MSetTx perf: " << totalOps << " ops, " << timeElapsedMilliSecond / 1000.0
                  << " ms, " << (totalOps * 1000000.0 / timeElapsedMilliSecond) << " ops/sec";

        std::vector<std::string> failedIds;
        DS_ASSERT_OK(client_->Del(keys, failedIds));
        DS_ASSERT_OK(client_->ShutDown());
        exit(0);
    }
    int s0;
    waitpid(pid0, &s0, 0);
}

class KVClientEmbeddedDfxTest : public KVClientCoprocessTest {
public:
    // Setup etcd cluster
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 0;
        opts.numOBS = 1;
        opts.numEtcd = 1;
        enableObs_ = true;
        configArgs_.emplace("node_timeout_s", "3");
        configArgs_.emplace("node_dead_timeout_s", "8");
        configArgs_.emplace("auto_del_dead_node", "true");
    }

    Status GetHashJson(json &ringJson)
    {
        std::string ringDataPath = cluster_->GetRootDir() + "/ring.dat";
        std::string ringJsonPath = cluster_->GetRootDir() + "/ring.json";
        auto cmd = FormatString(
            "%s --endpoints=%s  get /datasystem/ring/  --write-out=json | sed -n 's/.*\"value\":\"\\([^\"]*\\)\".*/\\1/p' | base64 -d > "
            "%s && %s -d %s > %s",
            SearchPath("etcdctl"), FLAGS_etcd_address, ringDataPath, HASH_PHRASE_PATH, ringDataPath, ringJsonPath);
        RETURN_IF_NOT_OK(ExecuteCmd(cmd));
        std::ifstream file(ringJsonPath);
        if (!file.is_open()) {
            return Status(K_RUNTIME_ERROR, "Failed to open file: " + ringJsonPath);
        }
        try {
            ringJson = json::parse(file);
        } catch (const json::parse_error &e) {
            return Status(K_RUNTIME_ERROR, "JSON parse error in file " + ringJsonPath + ": " + e.what());
        }
        return Status::OK();
    }

    template <typename F>
    void WaitHashRingChange(F &&f, uint64_t timeoutMs = 60000)  // default wait 60000 ms
    {
        auto timeOut = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        bool flag = false;
        json ringJson;
        while (std::chrono::steady_clock::now() < timeOut) {
            DS_ASSERT_OK(GetHashJson(ringJson));
            if (f(ringJson)) {
                flag = true;
                break;
            }
            const int interval = 100;  // 100ms;
            std::this_thread::sleep_for(std::chrono::milliseconds(interval));
        }
        LOG(INFO) << "Check " << (flag ? "success" : "failed") << ", Ring info:" << ringJson.dump();
        ASSERT_TRUE(flag);
    }

    void WaitAllNodesJoinIntoHashRing(size_t num, uint64_t timeoutSec = 60)
    {
        int S2Ms = 1000;
        WaitHashRingChange(
            [&](const json &hashRing) {
                if (!hashRing.contains("workers") || !hashRing["workers"].is_object()) {
                    return false;
                }
                auto &workers = hashRing["workers"];
                if (workers.size() != num || hashRing.contains("add_node_info") || hashRing.contains("del_node_info")) {
                    return false;
                }
                for (const auto &[addr, info] : workers.items()) {
                    (void)addr;
                    if (!info.contains("state") || info["state"] != "ACTIVE") {
                        return false;
                    }
                }
                return true;
            },
            timeoutSec * S2Ms);
        sleep(1);  // wait for 1s
    }

    void VoluntaryScaleDownInject(int workerIdx)
    {
        std::string checkFilePath =
            cluster_->GetRootDir() + "/worker" + std::to_string(workerIdx) + "/log/worker-status";
        std::ofstream ofs(checkFilePath);
        if (!ofs.is_open()) {
            LOG(ERROR) << "Can not open worker status file in " << checkFilePath
                       << ", voluntary scale in will not start, errno: " << errno;
        } else {
            ofs << "voluntary scale in\n";
        }
        ofs.close();
    }
};

TEST_F(KVClientEmbeddedDfxTest, EmbeddedClusterKillScaleDownTest) 
{
    auto pid0 = fork();
    if (pid0 == 0) {
        DS_ASSERT_OK(StartEmbeddedNode(0));
        KVClient* client = &KVClient::EmbeddedInstance();
        std::string val;
        DS_ASSERT_OK(client->Get("killpid1", val, 60000)); // timeout is 20000 ms;
        DS_ASSERT_OK(client->ShutDown());
        exit(0);
    }
    auto pid1 = fork();
    if (pid1 == 0) {
        DS_ASSERT_OK(StartEmbeddedNode(1));
        KVClient* client = &KVClient::EmbeddedInstance();
        std::string val;
        DS_ASSERT_OK(client->Get("testfinish", val, 60000)); // timeout is 20000 ms;
        sleep(1); // wait client shutdown.
        DS_ASSERT_OK(client->ShutDown());
        exit(0);
    }
    DS_ASSERT_OK(WaitWorkerReady(0));
    DS_ASSERT_OK(WaitWorkerReady(1));
    auto pid2 = fork();
    if (pid2 == 0) {
        std::shared_ptr<KVClient> cli0, cli1;
        InitTestKVClient(workerAdds_[0], cli0);
        InitTestKVClient(workerAdds_[1], cli1);
        const int kKeys = 10;
        auto dataSize = 100;
        std::vector<std::string> keys(kKeys), vals(kKeys);
        SetParam param{.writeMode = WriteMode::WRITE_THROUGH_L2_CACHE};
        for (int i = 0; i < kKeys; ++i) {
            keys[i] = cli0->GenerateKey();
            vals[i] = GenRandomString(dataSize);
            DS_ASSERT_OK(cli0->Set(keys[i], vals[i], param));
        }
        std::string newKey = cli0->GenerateKey();
        cli0.reset();
        DS_ASSERT_OK(cli1->Set("killpid1", "aaa"));
        WaitAllNodesJoinIntoHashRing(1);
        for (int i = 0; i < kKeys; ++i) {
            std::string got;
            DS_ASSERT_OK(cli1->Get(keys[i], got));
            ASSERT_EQ(got, vals[i]) << "key idx=" << i;
        }
        std::string newVal = GenRandomString(dataSize);
        DS_ASSERT_OK(cli1->Set(newKey, newVal));
        std::string newGot;
        DS_ASSERT_OK(cli1->Get(newKey, newGot));
        ASSERT_EQ(newGot, newVal);
        DS_ASSERT_OK(cli1->Set("testfinish", "aaa"));
        cli1.reset();
        exit(0);
    }
    int s1, s2, s0;
    waitpid(pid0, &s0, 0);
    waitpid(pid1, &s1, 0);
    waitpid(pid2, &s2, 0);
}

TEST_F(KVClientEmbeddedDfxTest, EmbeddedClusterVoluntaryShutDownTest)
{
    auto dataSize = 100;
    const int kKeys = 30;
    std::vector<std::string> keys(kKeys), vals(kKeys);
    for (int i = 0; i < kKeys; ++i) {
        keys[i] = "a_key_for_test_" + std::to_string(i);
        vals[i] = GenRandomString(dataSize);
    }
    auto pid0 = fork();
    if (pid0 == 0) {
        DS_ASSERT_OK(StartEmbeddedNode(0));
        WaitAllNodesJoinIntoHashRing(2);
        KVClient* client = &KVClient::EmbeddedInstance();
        for (int i = 0; i < kKeys; ++i) {
            DS_ASSERT_OK(client->Set(keys[i], vals[i]));
        }
        VoluntaryScaleDownInject(0);
        DS_ASSERT_OK(client->ShutDown());
        exit(0);
    }
    auto pid1 = fork();
    if (pid1 == 0) {
        DS_ASSERT_OK(StartEmbeddedNode(1));
        WaitAllNodesJoinIntoHashRing(2);
        KVClient* client = &KVClient::EmbeddedInstance();
        WaitAllNodesJoinIntoHashRing(1);
        for (int i = 0; i < kKeys; ++i)  {
            std::string got;
            DS_ASSERT_OK(client->Get(keys[i], got));
            ASSERT_EQ(got, vals[i]);
        }
        DS_ASSERT_OK(client->ShutDown());
        exit(0);
    }
    int s1, s0;
    waitpid(pid0, &s0, 0);
    waitpid(pid1, &s1, 0);
}
}  // namespace st
}  // namespace datasystem
