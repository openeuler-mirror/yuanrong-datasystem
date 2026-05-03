/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#include "oc_client_common.h"

#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/protos/hash_ring.pb.h"

namespace datasystem {
namespace st {
void OCClientCommon::GetWorkerUuids(EtcdStore *db, std::unordered_map<HostPort, std::string> &uuidMap)
{
    std::string value;
    DS_ASSERT_OK(db->Get(ETCD_RING_PREFIX, "", value));
    HashRingPb ring;
    ring.ParseFromString(value);
    for (auto worker : ring.workers()) {
        HostPort workerAddr;
        DS_ASSERT_OK(workerAddr.ParseString(worker.first));
        uuidMap.emplace(std::move(workerAddr), worker.second.worker_uuid());
    }
}

std::unique_ptr<EtcdStore> OCClientCommon::InitTestEtcdInstance(std::string azName)
{
    std::string etcdAddress = cluster_->GetEtcdAddrs();
    FLAGS_etcd_address = etcdAddress;
    LOG(INFO) << "The etcd address is:" << FLAGS_etcd_address << std::endl;
    auto db = std::make_unique<EtcdStore>(etcdAddress);
    if ((db != nullptr) && (db->Init().IsOk())) {
        auto prefix = azName.empty() ? "" : "/" + azName;
        // We don't check rc here. If table to drop does not exist, it's fine.
        (void)db->CreateTable(ETCD_RING_PREFIX, prefix + ETCD_RING_PREFIX);
        (void)db->CreateTable(ETCD_CLUSTER_TABLE, prefix + "/" + ETCD_CLUSTER_TABLE);
    }
    return db;
}
}  // namespace st
}  // namespace datasystem
