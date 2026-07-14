/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#include "oc_client_common.h"

#include "datasystem/common/kvstore/coordination_keys.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/protos/cluster_topology.pb.h"

namespace datasystem {
namespace st {
namespace {
constexpr char HASH_TO_WORKER_KEY_PREFIX[] = "a_key_hash_to_";
using ClusterTopologyPb = datasystem::ClusterTopologyPb;
}

void OCClientCommon::GetWorkerUuids(EtcdStore *db, std::unordered_map<HostPort, std::string> &uuidMap)
{
    std::string value;
    DS_ASSERT_OK(db->Get(GetTopologyTableName(), "", value));
    ClusterTopologyPb ring;
    ring.ParseFromString(value);
    for (auto worker : ring.members()) {
        HostPort workerAddr;
        DS_ASSERT_OK(workerAddr.ParseString(worker.first));
        uuidMap.emplace(std::move(workerAddr), BytesUuidToString(worker.second.id()));
    }
}

std::unique_ptr<EtcdStore> OCClientCommon::InitTestEtcdInstance(std::string azName)
{
    std::string etcdAddress = cluster_->GetEtcdAddrs();
    FLAGS_etcd_address = etcdAddress;
    LOG(INFO) << "The etcd address is:" << FLAGS_etcd_address << std::endl;
    auto db = std::make_unique<EtcdStore>(etcdAddress);
    if ((db != nullptr) && (db->Init().IsOk())) {
        // We don't check rc here. If table to drop does not exist, it's fine.
        (void)RegisterTopologyTables(*db, azName.empty() ? FLAGS_cluster_name : azName);
    }
    return db;
}

void OCClientCommon::SetWorkerHashInjection(const std::vector<uint32_t> &workerIndexes)
{
    if (workerIndexes.empty()) {
        for (size_t i = 0; i < cluster_->GetWorkerNum(); ++i) {
            DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, i, "MurmurHash3", "return()"));
        }
        return;
    }
    for (auto workerIndex : workerIndexes) {
        DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, workerIndex, "MurmurHash3", "return()"));
    }
}

void OCClientCommon::SetWorkerHashInjection(std::initializer_list<uint32_t> workerIndexes)
{
    SetWorkerHashInjection(std::vector<uint32_t>(workerIndexes));
}

void OCClientCommon::GetObjectKeysHashToWorker(EtcdStore *db, uint32_t workerIndex, size_t objectCount,
                                               std::vector<std::string> &objectKeys)
{
    ASSERT_NE(db, nullptr);
    std::string value;
    DS_ASSERT_OK(db->Get(GetTopologyTableName(), "", value));
    ClusterTopologyPb ring;
    ASSERT_TRUE(ring.ParseFromString(value));
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex, workerAddress));
    std::map<uint32_t, std::string> tokenWorkers;
    for (const auto &worker : ring.members()) {
        for (auto token : worker.second.tokens()) {
            tokenWorkers.emplace(token, worker.first);
        }
    }
    objectKeys.clear();
    for (auto iter = tokenWorkers.begin(); iter != tokenWorkers.end() && objectKeys.size() < objectCount; ++iter) {
        if (iter->second != workerAddress.ToString()) {
            continue;
        }
        auto prev = iter == tokenWorkers.begin() ? std::prev(tokenWorkers.end()) : std::prev(iter);
        uint32_t distance = iter->first - prev->first;
        for (uint32_t offset = 1; offset <= distance && objectKeys.size() < objectCount; ++offset) {
            objectKeys.emplace_back(HASH_TO_WORKER_KEY_PREFIX + std::to_string(iter->first - offset + 1));
        }
    }
    ASSERT_EQ(objectKeys.size(), objectCount);
}

std::string OCClientCommon::GetObjectKeyHashToWorker(EtcdStore *db, uint32_t workerIndex)
{
    std::vector<std::string> objectKeys;
    GetObjectKeysHashToWorker(db, workerIndex, 1, objectKeys);
    return objectKeys.front();
}
}  // namespace st
}  // namespace datasystem
