/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Util function for kv client tests.
 */
#ifndef DATASYSTEM_TEST_ST_CLIENT_KV_CACHE_KV_CLIENT_COMMON_H
#define DATASYSTEM_TEST_ST_CLIENT_KV_CACHE_KV_CLIENT_COMMON_H

#include "client/object_cache/oc_client_common.h"
namespace datasystem {
namespace st {
class KVClientCommon : virtual public OCClientCommon {
public:
    void InitTestEtcdInstance(std::vector<std::string> otherAzNames = {})
    {
        if (db_ != nullptr) {
            return;
        }
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
        db_ = std::make_unique<EtcdStore>(etcdAddress);
        DS_ASSERT_OK(db_->Init());
        (void)db_->CreateTable(ETCD_RING_PREFIX, ETCD_RING_PREFIX);
        (void)db_->CreateTable(ETCD_CLUSTER_TABLE, "/" + std::string(ETCD_CLUSTER_TABLE));
        for (const auto &otherAzName : otherAzNames) {
            auto otherAzRingStr = "/" + otherAzName + ETCD_RING_PREFIX;
            (void)db_->CreateTable(otherAzRingStr, otherAzRingStr);
        }
    }
protected:
    std::unique_ptr<EtcdStore> db_;
};
}  // namespace st
}  // namespace datasystem
#endif  // DATASYSTEM_TEST_ST_CLIENT_KV_CACHE_KV_CLIENT_COMMON_H
