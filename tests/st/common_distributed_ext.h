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
 * Description: Datasystem unit test base class, each testcases files need include this head file.
 */
#ifndef DATASYSTEM_TEST_ST_COMMON_DISTRIBUTED_EXT_H
#define DATASYSTEM_TEST_ST_COMMON_DISTRIBUTED_EXT_H

#include <map>
#include <memory>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "common.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/worker/hash_ring/hash_ring.h"
#include "datasystem/worker/hash_ring/hash_ring_tools.h"

DS_DECLARE_string(etcd_address);
DS_DECLARE_string(log_dir);

namespace datasystem {
namespace st {

const int WORKER_RECEIVE_DELAY = 1;
const int RETRY_TIMEOUT_MS = 30000;

struct WorkerEntry {
    std::string uuid;
    HostPort addr;
    int index;
    int nextIndex;
    std::string nextUuid;
};

class CommonDistributedExt {
public:
    void InitTestEtcdInstance()
    {
        if (etcd_ != nullptr) {
            return;
        }
        std::string etcdAddress;
        for (size_t i = 0; i < GetCluster()->GetEtcdNum(); ++i) {
            std::pair<HostPort, HostPort> addrs;
            GetCluster()->GetEtcdAddrs(i, addrs);
            if (!etcdAddress.empty()) {
                etcdAddress += ",";
            }
            etcdAddress += addrs.first.ToString();
        }
        FLAGS_etcd_address = etcdAddress;
        etcd_ = std::make_unique<EtcdStore>(etcdAddress);
        DS_ASSERT_OK(etcd_->Init());
        DS_ASSERT_OK(etcd_->CreateTable(ETCD_RING_PREFIX, ETCD_RING_PREFIX));
        DS_ASSERT_OK(etcd_->CreateTable(ETCD_CLUSTER_TABLE, "/" + std::string(ETCD_CLUSTER_TABLE)));
        DS_ASSERT_OK(etcd_->CreateTable(ETCD_REPLICA_GROUP_TABLE, ETCD_REPLICA_GROUP_TABLE));
    }

    void GetHashRingPb(HashRingPb &ring)
    {
        if (!etcd_) {
            InitTestEtcdInstance();
        }
        std::string value;
        DS_ASSERT_OK(etcd_->Get(ETCD_RING_PREFIX, "", value));
        ASSERT_TRUE(ring.ParseFromString(value));
    }

    void ObtainHashTokens()
    {
        HashRingPb ring;
        GetHashRingPb(ring);
        hashTokens_.clear();
        for (const auto &kv : ring.workers()) {
            if (kv.second.state() != WorkerPb::ACTIVE && kv.second.state() != WorkerPb::LEAVING) {
                continue;
            }
            const auto &workerId = kv.first;
            for (auto token : kv.second.hash_tokens()) {
                hashTokens_.insert({ token, workerId });
            }
        }
    }

    void AssertAllNodesJoinIntoHashRing(int num)
    {
        HashRingPb ring;
        GetHashRingPb(ring);
        ASSERT_EQ(ring.workers_size(), num) << ring.ShortDebugString();
        for (auto &worker : ring.workers()) {
            ASSERT_TRUE(worker.second.state() == WorkerPb::ACTIVE) << ring.ShortDebugString();
        }
        ASSERT_TRUE(ring.add_node_info_size() == 0) << ring.ShortDebugString();
        ASSERT_TRUE(ring.del_node_info_size() == 0) << ring.ShortDebugString();
    }

    void AssertDelNodeInfoExist()
    {
        if (!etcd_) {
            InitTestEtcdInstance();
        }
        std::string value;
        DS_ASSERT_OK(etcd_->Get(ETCD_RING_PREFIX, "", value));
        HashRingPb ring;
        ASSERT_TRUE(ring.ParseFromString(value));
        ASSERT_TRUE(ring.del_node_info_size() != 0) << ring.ShortDebugString();
    }

    void WaitAllNodesJoinIntoHashRing(int num, uint64_t timeoutSec = 60)
    {
        int S2Ms = 1000;
        WaitHashRingChange(
            [&](const HashRingPb &hashRing) {
                if (hashRing.workers_size() != num || hashRing.add_node_info_size() != 0
                    || hashRing.del_node_info_size() != 0) {
                    return false;
                }
                for (auto &worker : hashRing.workers()) {
                    if (worker.second.state() != WorkerPb::ACTIVE) {
                        return false;
                    }
                }
                return true;
            },
            timeoutSec * S2Ms);
        sleep(WORKER_RECEIVE_DELAY);
    }

    template <typename F>
    void WaitHashRingChange(F &&f, uint64_t timeoutMs = shutdownTimeoutMs)
    {
        if (!etcd_) {
            InitTestEtcdInstance();
        }
        auto timeOut = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        bool flag = false;
        HashRingPb ring;
        while (std::chrono::steady_clock::now() < timeOut) {
            std::string hashRingStr;
            DS_ASSERT_OK(etcd_->Get(ETCD_RING_PREFIX, "", hashRingStr));
            ASSERT_TRUE(ring.ParseFromString(hashRingStr));
            if (f(ring)) {
                flag = true;
                break;
            }
            const int interval = 100;  // 100ms;
            std::this_thread::sleep_for(std::chrono::milliseconds(interval));
        }
        LOG(INFO) << "Check " << (flag ? "success" : "failed")
                  << ", Ring info:" << worker::HashRingToJsonString(ring);
        ASSERT_TRUE(flag);
    }

    void VoluntaryScaleDownInject(int workerIdx)
    {
        std::string checkFilePath = FLAGS_log_dir.c_str();
        std::string client = "client";
        checkFilePath = checkFilePath.substr(0, checkFilePath.length() - client.length()) + "/worker"
                        + std::to_string(workerIdx) + "/log/worker-status";
        std::ofstream ofs(checkFilePath);
        if (!ofs.is_open()) {
            LOG(ERROR) << "Can not open worker status file in " << checkFilePath
                       << ", voluntary scale in will not start, errno: " << errno;
        } else {
            ofs << "voluntary scale in\n";
        }
        ofs.close();
        kill(GetCluster()->GetWorkerPid(workerIdx), SIGTERM);
    }

    void GetWorkerUuidMap(std::map<std::string, std::string> &workerUuids)
    {
        if (!etcd_) {
            InitTestEtcdInstance();
        }

        std::string value;
        DS_ASSERT_OK(etcd_->Get(ETCD_RING_PREFIX, "", value));
        HashRingPb ring;
        ASSERT_TRUE(ring.ParseFromString(value));
        workerUuids.clear();
        for (auto &kv : ring.workers()) {
            workerUuids.emplace(kv.second.worker_uuid(), kv.first);
        }
    }

    void InitWorkersInfoMap(const std::vector<int> &indexes)
    {
        auto cluster = GetCluster();
        std::map<std::string, std::string> workerUuids;
        std::map<std::string, std::pair<HostPort, int>> workerUuidWithIndex;
        GetWorkerUuidMap(workerUuids);
        for (auto index : indexes) {
            workersInfo_.erase(index);
            HostPort addr;
            DS_ASSERT_OK(cluster->GetWorkerAddr(index, addr));
            for (auto &kv : workerUuids) {
                if (addr.ToString() != kv.second) {
                    continue;
                }

                workerUuidWithIndex.emplace(kv.first, std::make_pair(addr, index));
            }
        }

        for (auto &kv : workerUuidWithIndex) {
            auto index = kv.second.second;
            auto workerUuid = kv.first;
            auto iter = workerUuidWithIndex.find(workerUuid);
            if (iter != workerUuidWithIndex.end()) {
                iter = worker::LoopNext(workerUuidWithIndex, iter);
                workersInfo_.emplace(index, WorkerEntry{ .uuid = workerUuid,
                                                         .addr = kv.second.first,
                                                         .index = index,
                                                         .nextIndex = iter->second.second,
                                                         .nextUuid = iter->first });
            }
        }
    }

    Status SetWaitingElection(const std::string &dbName, const std::string &nextPrimaryId)
    {
        return etcd_->CAS(
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
                        item.set_seq(INT64_MAX);
                    }
                }
                replicaGroup.set_primary_id("");
                newValue = std::make_unique<std::string>(replicaGroup.SerializeAsString());
                return Status::OK();
            });
    }

    template <typename Func>
    void WaitReplicaReady(int index, Func &&func, uint64_t timeoutMs = 30000)
    {
        auto timeOut = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        bool flag = false;
        ReplicaGroupPb replicaGroupPb;
        std::string workerUuid = workersInfo_[index].uuid;
        ASSERT_TRUE(!workerUuid.empty()) << "workerUuid is empty for worker " << index;
        std::string nextWorkerUuid;
        std::map<std::string, std::string> workerUuids;
        while (std::chrono::steady_clock::now() < timeOut) {
            std::string value;
            DS_ASSERT_OK(etcd_->Get(ETCD_REPLICA_GROUP_TABLE, workerUuid, value));
            ASSERT_TRUE(replicaGroupPb.ParseFromString(value));
            // get next worker.
            workerUuids.clear();
            GetWorkerUuidMap(workerUuids);
            auto iter = workerUuids.find(workerUuid);
            if (iter != workerUuids.end()) {
                iter = worker::LoopNext(workerUuids, iter);
                nextWorkerUuid = iter->first;
                if (func(workerUuid, nextWorkerUuid, replicaGroupPb)) {
                    flag = true;
                    break;
                }
            }
            const int interval = 100;  // 100ms;
            std::this_thread::sleep_for(std::chrono::milliseconds(interval));
        }
        std::string value;
        DS_ASSERT_OK(etcd_->Get(ETCD_RING_PREFIX, "", value));
        HashRingPb ring;
        ASSERT_TRUE(ring.ParseFromString(value));
        LOG(INFO) << "Check worker index:" << index << (flag ? " success" : " failed")
                  << ", ReplicaGroupPb info:" << replicaGroupPb.ShortDebugString() << ", workerUuid:" << workerUuid
                  << " nextWorkerUuid:" << nextWorkerUuid;
        LOG(INFO) << "uuid to addr:" << MapToString(workerUuids);
        LOG(INFO) << "hashring:" << ring.DebugString();
        ASSERT_TRUE(flag);
    }

    void WaitReplicaNotInCurrentNode(int index, uint64_t timeoutMs = RETRY_TIMEOUT_MS)
    {
        WaitReplicaReady(
            index,
            [](const std::string &workerUuid, const std::string &, const ReplicaGroupPb &replicaGroupPb) {
                const auto &primaryId = replicaGroupPb.primary_id();
                return !primaryId.empty() && primaryId != workerUuid;
            },
            timeoutMs);
    }

    void WaitChangeReplicaInCurrentNode(int index, std::string &notExistWorker, uint64_t timeoutMs = RETRY_TIMEOUT_MS)
    {
        WaitReplicaReady(
            index,
            [&notExistWorker](const std::string &workerUuid, const std::string &,
                              const ReplicaGroupPb &replicaGroupPb) {
                for (const auto &replica : replicaGroupPb.replicas()) {
                    if (replica.worker_id() == notExistWorker) {
                        return false;
                    }
                }
                const auto &primaryId = replicaGroupPb.primary_id();
                return !primaryId.empty() && primaryId == workerUuid;
            },
            timeoutMs);
    }

    void WaitReplicaInCurrentNode(int index, uint64_t timeoutMs = RETRY_TIMEOUT_MS)
    {
        WaitReplicaReady(
            index,
            [](const std::string &workerUuid, const std::string &, const ReplicaGroupPb &replicaGroupPb) {
                const auto &primaryId = replicaGroupPb.primary_id();
                return !primaryId.empty() && primaryId == workerUuid;
            },
            timeoutMs);
    }

    void WaitReplicaLocationMatch(const std::vector<uint32_t> &indexes, uint64_t timeoutMs = RETRY_TIMEOUT_MS)
    {
        auto func = [](const std::string &workerUuid, const std::string &nextWorkerUuid,
                       const ReplicaGroupPb &replicaGroupPb) {
            const auto &primaryId = replicaGroupPb.primary_id();
            const auto &replicas = replicaGroupPb.replicas();
            const size_t replicaCount = 2;
            if (primaryId.empty() || primaryId != workerUuid || replicas.size() != replicaCount) {
                return false;
            }
            // the next worker in ReplicaGroupPb.
            for (const auto &replica : replicas) {
                if (replica.worker_id() == nextWorkerUuid) {
                    return true;
                }
            }
            return false;
        };
        for (auto index : indexes) {
            WaitReplicaReady(index, func, timeoutMs);
        }
    }

    bool GetTwoWorkerNotBackupEachOther(int &index1, int &index2)
    {
        ObtainHashTokens();
        auto getEntryByAddr = [this](std::string &addr) {
            WorkerEntry entry;
            for (const auto &kv : workersInfo_) {
                if (kv.second.addr.ToString() == addr) {
                    entry = kv.second;
                    break;
                }
            }
            return entry;
        };
        LOG(INFO) << "hash tokens:" << MapToString(hashTokens_);
        auto iter = hashTokens_.begin();
        while (iter != hashTokens_.end()) {
            auto addr1 = iter->second;
            auto addr2 = worker::LoopNext(hashTokens_, iter)->second;
            auto entry1 = getEntryByAddr(addr1);
            auto entry2 = getEntryByAddr(addr2);
            if (entry1.uuid.empty() || entry2.uuid.empty()) {
                continue;
            }
            if (entry1.nextUuid != entry2.uuid && entry1.uuid != entry2.nextUuid) {
                index1 = entry1.index;
                index2 = entry2.index;
                return true;
            }
            iter++;
        }
        LOG(INFO) << "not found worker index.";
        return false;
    }

    void InjectSyncCap(const std::vector<int> &indexes, uint64_t currLsn, uint64_t sendLsn)
    {
        for (auto idx : indexes) {
            DS_ASSERT_OK(GetCluster()->SetInjectAction(WORKER, idx, "worker.CheckReplicaLocation", "call(1000)"));
            DS_ASSERT_OK(GetCluster()->SetInjectAction(WORKER, idx, "replica.SyncGap",
                                                       FormatString("return(%zu,%zu)", currLsn, sendLsn)));
        }
    }

    void ResetSyncCap(const std::vector<int> &indexes)
    {
        for (auto idx : indexes) {
            DS_ASSERT_OK(GetCluster()->ClearInjectAction(WORKER, idx, "replica.SyncGap"));
        }
    }

    void GetMetaLocationById(const std::string &id, const std::vector<int> &indexes, WorkerEntry &entry)
    {
        auto hash = MurmurHash3_32(id);
        if (hashTokens_.empty()) {
            ObtainHashTokens();
        }
        if (workersInfo_.empty()) {
            InitWorkersInfoMap(indexes);
        }
        ASSERT_TRUE(!hashTokens_.empty()) << "hashToken_ not init.";
        auto iter = hashTokens_.upper_bound(hash);
        auto addr = iter == hashTokens_.end() ? hashTokens_.begin()->second : iter->second;
        for (auto &kv : workersInfo_) {
            if (kv.second.addr.ToString() == addr) {
                entry = kv.second;
                return;
            }
        }
        ASSERT_TRUE(false) << "not found addr " << addr << " from workersInfo_ ";
    }

protected:
    virtual BaseCluster *GetCluster() = 0;
    std::unique_ptr<EtcdStore> etcd_;
    const static uint64_t shutdownTimeoutMs = 60000;  // 1min
    std::map<int, WorkerEntry> workersInfo_;
    std::map<uint32_t, std::string> hashTokens_;
};
}  // namespace st
}  // namespace datasystem
#endif
