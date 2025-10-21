/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Test ReplicaManager class
 */

#include "datasystem/master/replica_manager.h"
#include <mutex>

#include "common.h"

#include "datasystem/common/eventloop/timer_queue.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"
#include "datasystem/worker/hash_ring/hash_ring_event.h"

DS_DECLARE_bool(enable_meta_replica);
DS_DECLARE_string(etcd_address);

namespace datasystem {
namespace st {
const int RETRY_TIMEOUT_MS = 30000;
struct ReplicaInfo {
    std::set<std::string> primaryReplicas;
    std::set<std::string> backupReplicas;
    bool operator==(const ReplicaInfo &other) const
    {
        return this->primaryReplicas == other.primaryReplicas && this->backupReplicas == other.backupReplicas;
    }

    bool operator!=(const ReplicaInfo &other) const
    {
        return !(*this == other);
    }

    friend std::ostream &operator<<(std::ostream &out, const ReplicaInfo &info)
    {
        out << "primaryReplicas: {" << VectorToString(info.primaryReplicas) << "}, backupReplicas: {"
            << VectorToString(info.backupReplicas) << "}";
        return out;
    }
};

class ReplicaManagerMock : public ReplicaManager {
public:
    Status TryAddPrimary(const std::string &dbName, const std::string &primaryNodeId) override
    {
        (void)dbName;
        (void)primaryNodeId;
        return Status::OK();
    }
    Status CreateMetaManager(const std::string &dbName, RocksStore *objectRocksStore) override
    {
        (void)dbName;
        (void)objectRocksStore;
        return Status::OK();
    }
    Status DestroyMetaManager(const std::string &dbName) override
    {
        (void)dbName;
        return Status::OK();
    };
    ReplicaInfo GetReplicaInfo()
    {
        ReplicaInfo info;
        std::shared_lock<std::shared_timed_mutex> locker(mutex_);
        for (const auto &replica : replicas_) {
            if (replica.second->GetReplicaType() == ReplicaType::Primary) {
                info.primaryReplicas.insert(replica.first);
            } else {
                info.backupReplicas.insert(replica.first);
            }
        }
        return info;
    }
};

class ReplicaManagerTest : public ExternalClusterTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numMasters = 0;
        opts.numWorkers = 0;
        opts.numOBS = 0;
    }

    void SetUp() override
    {
        ClusterTest::SetUp();
        FLAGS_enable_meta_replica = true;
        TimerQueue::GetInstance()->Initialize();
        DS_ASSERT_OK(InitEtcd());
    }
    void TearDown() override
    {
        stop_ = true;
        if (thread_.joinable()) {
            thread_.join();
        }
        ClusterTest::TearDown();
    }
    Status InitEtcd()
    {
        std::pair<HostPort, HostPort> addrs;
        RETURN_IF_NOT_OK(cluster_->GetEtcdAddrs(0, addrs));
        FLAGS_etcd_address = addrs.first.ToString();
        etcdStore_ = std::make_unique<EtcdStore>(FLAGS_etcd_address);
        RETURN_IF_NOT_OK(etcdStore_->Init());
        RETURN_IF_NOT_OK(etcdStore_->CreateTable(ETCD_REPLICA_GROUP_TABLE, ETCD_REPLICA_GROUP_TABLE));
        const int queueSize = 16;
        eventQue_ = std::make_unique<Queue<mvccpb::Event>>(queueSize);
        etcdStore_->SetEventHandler([this](mvccpb::Event &&event) { eventQue_->Add(std::move(event)); });
        RETURN_IF_NOT_OK(etcdStore_->WatchEvents({ { ETCD_REPLICA_GROUP_TABLE, "", false, 0 } }));
        thread_ = std::thread([this] {
            while (!stop_) {
                mvccpb::Event event;
                const uint64_t timeout = 100;  // ms;
                Status rc = eventQue_->Poll(&event, timeout);
                if (rc.GetCode() == K_TRY_AGAIN) {
                    continue;
                }
                std::lock_guard<std::mutex> locker(mutex_);
                for (auto &kv : managers_) {
                    kv.second->EnqueEvent(event);
                }
            }
        });
        return Status::OK();
    }

    Status CreateReplicaManager(int index, const std::string &workerUuid)
    {
        ReplicaManagerParam param;
        param.dbRootPath = GetTestCaseDataDir() + "/worker" + std::to_string(index) + "/rocksdb";
        param.currWorkerId = workerUuid;
        param.akSkManager = nullptr;
        param.etcdStore = etcdStore_.get();
        param.persistenceApi = nullptr;
        param.etcdCM = nullptr;
        param.masterWorkerService = nullptr;
        param.workerWorkerService = nullptr;
        param.isOcEnabled = true;
        auto manager = std::make_unique<ReplicaManagerMock>();
        RETURN_IF_NOT_OK(manager->Init(param));
        std::lock_guard<std::mutex> locker(mutex_);
        managers_.emplace(index, std::move(manager));
        dbNames_.emplace_back(workerUuid);
        return Status::OK();
    }

    Status VerifyReplica(const std::vector<ReplicaInfo> &replicaInfos)
    {
        if (replicaInfos.size() > managers_.size()) {
            RETURN_STATUS(K_RUNTIME_ERROR, FormatString(" replicaInfos size %d greater than managers_ size %d.",
                                                        replicaInfos.size(), managers_.size()));
        }
        for (size_t idx = 0; idx < replicaInfos.size(); idx++) {
            auto mock = dynamic_cast<ReplicaManagerMock *>(managers_[idx].get());
            auto info = mock->GetReplicaInfo();
            if (info != replicaInfos[idx]) {
                LOG(ERROR) << "replica info not match for " << idx << ", current:" << info
                           << ", expected:" << replicaInfos[idx];
                RETURN_STATUS(K_RUNTIME_ERROR, "replica info not match");
            }
        }
        return Status::OK();
    }
    Status SetWaitingElection(const std::string &dbName, const std::string &nextPrimaryId)
    {
        return etcdStore_->CAS(
            ETCD_REPLICA_GROUP_TABLE, dbName,
            [&nextPrimaryId](const std::string &oldValue, std::unique_ptr<std::string> &newValue, bool & /* retry */) {
                ReplicaGroupPb replicaGroup;
                if (!replicaGroup.ParseFromString(oldValue)) {
                    LOG(WARNING) << "Parse to ReplicaGroupPb failed.";
                    return Status::OK();
                }
                if (replicaGroup.primary_id().empty()) {
                    return Status::OK();
                }
                for (auto &item : *replicaGroup.mutable_replicas()) {
                    if (item.worker_id() == nextPrimaryId) {
                        item.set_seq(1);
                    }
                }
                replicaGroup.set_primary_id("");
                newValue = std::make_unique<std::string>(replicaGroup.SerializeAsString());
                return Status::OK();
            });
    }

    Status RemoveReplica(const std::string &dbName)
    {
        return etcdStore_->Delete(ETCD_REPLICA_GROUP_TABLE, dbName);
    }

    template <typename Func>
    void UntilTrueOrTimeout(Func &&func, uint64_t timeoutMs = RETRY_TIMEOUT_MS)
    {
        auto timeOut = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        while (std::chrono::steady_clock::now() < timeOut) {
            std::string value;
            if (func()) {
                return;
            }
            const int interval = 1000;  // 1000ms;
            std::this_thread::sleep_for(std::chrono::milliseconds(interval));
        }
        ASSERT_TRUE(false) << "Timeout";
    }

    void InitReplicaManagers(const std::vector<std::string> &nodes)
    {
        for (size_t idx = 0; idx < nodes.size(); idx++) {
            DS_ASSERT_OK(CreateReplicaManager(idx, nodes[idx]));
            ClusterInfo clusterInfo;
            DS_ASSERT_OK(EtcdClusterManager::ConstructClusterInfoViaEtcd(etcdStore_.get(), clusterInfo));
            DS_ASSERT_OK(managers_[idx]->InitReplicaForStart(false, clusterInfo));
            size_t nextIdx = (idx + 1) % nodes.size();
            HashRingEvent::ClusterInitFinish::GetInstance().NotifyAll(nodes[idx], nodes[nextIdx]);
        }
    }

protected:
    std::unique_ptr<EtcdStore> etcdStore_;
    std::unique_ptr<Queue<mvccpb::Event>> eventQue_;
    std::thread thread_;
    std::mutex mutex_;
    std::unordered_map<int, std::unique_ptr<ReplicaManager>> managers_;
    std::atomic<bool> stop_{ false };
    std::vector<std::string> dbNames_;
};

TEST_F(ReplicaManagerTest, TestInit)
{
    InitReplicaManagers({ "node0", "node1", "node2" });

    UntilTrueOrTimeout([&] {
        return VerifyReplica({ ReplicaInfo{ { "node0" }, { "node2" } }, ReplicaInfo{ { "node1" }, { "node0" } },
                               ReplicaInfo{ { "node2" }, { "node1" } } });
    });

    // switch
    managers_[0]->AddOrSwitchTo("node0", ReplicaType::Backup);
    UntilTrueOrTimeout([&] { return VerifyReplica({ ReplicaInfo{ {}, { "node0", "node2" } } }); });
    DS_ASSERT_OK(SetWaitingElection("node0", "node1"));

    UntilTrueOrTimeout([&] {
        return VerifyReplica({ ReplicaInfo{ {}, { "node0", "node2" } }, ReplicaInfo{ { "node0", "node1" }, {} },
                               ReplicaInfo{ { "node2" }, { "node1" } } });
    });

    // delete replica
    DS_ASSERT_OK(RemoveReplica("node1"));

    UntilTrueOrTimeout([&] {
        return VerifyReplica({ ReplicaInfo{ {}, { "node0", "node2" } }, ReplicaInfo{ { "node0" }, {} },
                               ReplicaInfo{ { "node2" }, {} } });
    });
}

TEST_F(ReplicaManagerTest, TestAdjustBackupReplicaLocation)
{
    InitReplicaManagers({ "node0", "node1", "node2", "node3" });

    // add replica node1 at node0,node3
    DS_ASSERT_OK(managers_[0]->AdjustReplicaLocationImpl("node1", { "node0", "node3" }));

    UntilTrueOrTimeout([&] {
        return VerifyReplica({ ReplicaInfo{ { "node0" }, { "node1", "node3" } },
                               ReplicaInfo{ { "node1" }, { "node0" } }, ReplicaInfo{ { "node2" }, { "node1" } },
                               ReplicaInfo{ { "node3" }, { "node1", "node2" } } });
    });

    // remote replica node1 at node0,node3
    DS_ASSERT_OK(managers_[0]->AdjustReplicaLocationImpl("node1", {}, { "node0", "node3" }));
    UntilTrueOrTimeout([&] {
        return VerifyReplica({ ReplicaInfo{ { "node0" }, { "node3" } }, ReplicaInfo{ { "node1" }, { "node0" } },
                               ReplicaInfo{ { "node2" }, { "node1" } }, ReplicaInfo{ { "node3" }, { "node2" } } });
    });

    // add and remote at the same time.
    DS_ASSERT_OK(managers_[0]->AdjustReplicaLocationImpl("node1", { "node0" }, { "node2" }));
    UntilTrueOrTimeout([&] {
        return VerifyReplica({ ReplicaInfo{ { "node0" }, { "node1", "node3" } },
                               ReplicaInfo{ { "node1" }, { "node0" } }, ReplicaInfo{ { "node2" }, {} },
                               ReplicaInfo{ { "node3" }, { "node2" } } });
    });
}

TEST_F(ReplicaManagerTest, TestNotifyStartElection)
{
    InitReplicaManagers({ "node0", "node1", "node2" });
    UntilTrueOrTimeout([&] {
        return VerifyReplica({ ReplicaInfo{ { "node0" }, { "node2" } }, ReplicaInfo{ { "node1" }, { "node0" } },
                               ReplicaInfo{ { "node2" }, { "node1" } } });
    });

    DS_ASSERT_OK(
        managers_[0]->NotifyStartElection("node0", [](const std::string &dbName, ReplicaGroupPb &replicaGroup) {
            for (auto &replica : *replicaGroup.mutable_replicas()) {
                if (replica.worker_id() != dbName) {
                    replica.set_seq(1);
                }
            }
            return true;
        }));

    // waiting replica switch.
    UntilTrueOrTimeout([&] {
        return VerifyReplica({ ReplicaInfo{ {}, { "node0", "node2" } }, ReplicaInfo{ { "node0", "node1" }, {} },
                               ReplicaInfo{ { "node2" }, { "node1" } } });
    });
}
}  // namespace st
}  // namespace datasystem
