/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
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
#include "datasystem/common/kvstore/coordination_keys.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/protos/cluster_topology.pb.h"

DS_DECLARE_string(etcd_address);
DS_DECLARE_string(log_dir);

namespace datasystem {
namespace st {
const int WORKER_RECEIVE_DELAY = 1;
const int RETRY_TIMEOUT_MS = 30000;

template <typename Container>
typename Container::iterator LoopNext(Container &container, typename Container::iterator iter)
{
    ++iter;
    return iter == container.end() ? container.begin() : iter;
}

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
        DS_ASSERT_OK(RegisterTopologyTables(*etcd_));
    }

    void GetClusterTopologyPb(ClusterTopologyPb &ring)
    {
        if (!etcd_) {
            InitTestEtcdInstance();
        }
        std::string value;
        DS_ASSERT_OK(etcd_->Get(GetTopologyTableName(), "", value));
        ASSERT_TRUE(ring.ParseFromString(value));
    }

    void ObtainTokens()
    {
        ClusterTopologyPb ring;
        GetClusterTopologyPb(ring);
        hashTokens_.clear();
        for (const auto &kv : ring.members()) {
            if (kv.second.state() != MembershipPb::ACTIVE && kv.second.state() != MembershipPb::LEAVING) {
                continue;
            }
            const auto &workerId = kv.first;
            for (auto token : kv.second.tokens()) {
                hashTokens_.insert({ token, workerId });
            }
        }
    }

    void AssertAllNodesJoinIntoClusterTopology(int num)
    {
        ClusterTopologyPb ring;
        GetClusterTopologyPb(ring);
        ASSERT_EQ(ring.members_size(), num) << ring.ShortDebugString();
        for (auto &worker : ring.members()) {
            ASSERT_TRUE(worker.second.state() == MembershipPb::ACTIVE) << ring.ShortDebugString();
        }
    }

    void AssertDelNodeInfoExist()
    {
        FAIL() << "Phase2 topology no longer stores legacy del_node_info in ClusterTopologyPb.";
    }

    void WaitAllMembersJoinClusterTopology(int num, uint64_t timeoutSec = 60)
    {
        int S2Ms = 1000;
        WaitClusterTopologyChange(
            [&](const ClusterTopologyPb &hashRing) {
                if (hashRing.members_size() != num) {
                    return false;
                }
                for (auto &worker : hashRing.members()) {
                    if (worker.second.state() != MembershipPb::ACTIVE) {
                        return false;
                    }
                }
                return true;
            },
            timeoutSec * S2Ms);
        sleep(WORKER_RECEIVE_DELAY);
    }

    template <typename F>
    void WaitClusterTopologyChange(F &&f, uint64_t timeoutMs = shutdownTimeoutMs)
    {
        if (!etcd_) {
            InitTestEtcdInstance();
        }
        auto timeOut = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        bool flag = false;
        ClusterTopologyPb ring;
        while (std::chrono::steady_clock::now() < timeOut) {
            std::string hashRingStr;
            DS_ASSERT_OK(etcd_->Get(GetTopologyTableName(), "", hashRingStr));
            ASSERT_TRUE(ring.ParseFromString(hashRingStr));
            if (f(ring)) {
                flag = true;
                break;
            }
            const int interval = 100;  // 100ms;
            std::this_thread::sleep_for(std::chrono::milliseconds(interval));
        }
        LOG(INFO) << "Check " << (flag ? "success" : "failed") << ", Ring info:" << ring.ShortDebugString();
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

    void GetMemberIdMap(std::map<std::string, std::string> &workerUuids)
    {
        if (!etcd_) {
            InitTestEtcdInstance();
        }

        std::string value;
        DS_ASSERT_OK(etcd_->Get(GetTopologyTableName(), "", value));
        ClusterTopologyPb ring;
        ASSERT_TRUE(ring.ParseFromString(value));
        workerUuids.clear();
        for (auto &kv : ring.members()) {
            workerUuids.emplace(BytesUuidToString(kv.second.id()), kv.first);
        }
    }

    void InitWorkersInfoMap(const std::vector<int> &indexes)
    {
        auto cluster = GetCluster();
        std::map<std::string, std::string> workerUuids;
        std::map<std::string, std::pair<HostPort, int>> workerUuidWithIndex;
        GetMemberIdMap(workerUuids);
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
                iter = LoopNext(workerUuidWithIndex, iter);
                workersInfo_.emplace(index, WorkerEntry{ .uuid = workerUuid,
                                                         .addr = kv.second.first,
                                                         .index = index,
                                                         .nextIndex = iter->second.second,
                                                         .nextUuid = iter->first });
            }
        }
    }



    bool GetTwoWorkerNotBackupEachOther(int &index1, int &index2)
    {
        ObtainTokens();
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
            auto addr2 = LoopNext(hashTokens_, iter)->second;
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
            ObtainTokens();
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
