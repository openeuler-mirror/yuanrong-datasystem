/*
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

/**
 * Description: Cluster metadata store abstraction used by EtcdClusterManager.
 */
#include "datasystem/worker/cluster_manager/cluster_store.h"

#include <sstream>
#include <utility>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
std::string ClusterStoreEvent::ToString() const
{
    std::stringstream ss;
    ss << "type: " << (type == ClusterStoreEventType::PUT ? "PUT" : "DELETE") << ", key: " << key
       << ", value: " << value << ", version: " << version << ", revision: " << revision;
    return ss.str();
}

EtcdClusterStore::EtcdClusterStore(EtcdStore *etcdStore) : etcdStore_(etcdStore)
{
}

ClusterStoreEvent EtcdClusterStore::FromEtcdEvent(const mvccpb::Event &event)
{
    ClusterStoreEvent clusterEvent;
    clusterEvent.type =
        event.type() == mvccpb::Event_EventType_DELETE ? ClusterStoreEventType::DELETE : ClusterStoreEventType::PUT;
    clusterEvent.key = event.kv().key();
    clusterEvent.value = event.kv().value();
    clusterEvent.version = event.kv().version();
    clusterEvent.revision = event.kv().mod_revision();
    return clusterEvent;
}

mvccpb::Event EtcdClusterStore::ToEtcdEvent(const ClusterStoreEvent &event)
{
    mvccpb::Event etcdEvent;
    etcdEvent.set_type(event.type == ClusterStoreEventType::DELETE ? mvccpb::Event_EventType_DELETE
                                                                   : mvccpb::Event_EventType_PUT);
    auto *kv = etcdEvent.mutable_kv();
    kv->set_key(event.key);
    kv->set_value(event.value);
    kv->set_version(event.version);
    kv->set_mod_revision(event.revision);
    return etcdEvent;
}

Status EtcdClusterStore::GetAll(const std::string &tableName,
                                std::vector<std::pair<std::string, std::string>> &outKeyValues)
{
    CHECK_FAIL_RETURN_STATUS(etcdStore_ != nullptr, K_RUNTIME_ERROR, "EtcdStore is null");
    return etcdStore_->GetAll(tableName, outKeyValues);
}

Status EtcdClusterStore::Get(const std::string &tableName, const std::string &key, std::string &value)
{
    CHECK_FAIL_RETURN_STATUS(etcdStore_ != nullptr, K_RUNTIME_ERROR, "EtcdStore is null");
    return etcdStore_->Get(tableName, key, value);
}

Status EtcdClusterStore::Get(const std::string &tableName, const std::string &key, RangeSearchResult &res,
                             int32_t timeoutMs)
{
    CHECK_FAIL_RETURN_STATUS(etcdStore_ != nullptr, K_RUNTIME_ERROR, "EtcdStore is null");
    return etcdStore_->Get(tableName, key, res, timeoutMs);
}

Status EtcdClusterStore::CAS(const std::string &tableName, const std::string &key, const ProcessFunction &processFunc,
                             RangeSearchResult &res)
{
    CHECK_FAIL_RETURN_STATUS(etcdStore_ != nullptr, K_RUNTIME_ERROR, "EtcdStore is null");
    return etcdStore_->CAS(tableName, key, processFunc, res);
}

Status EtcdClusterStore::CAS(const std::string &tableName, const std::string &key, const ProcessFunction &processFunc)
{
    CHECK_FAIL_RETURN_STATUS(etcdStore_ != nullptr, K_RUNTIME_ERROR, "EtcdStore is null");
    return etcdStore_->CAS(tableName, key, processFunc);
}

Status EtcdClusterStore::CAS(const std::string &tableName, const std::string &key, const std::string &oldValue,
                             const std::string &newValue)
{
    CHECK_FAIL_RETURN_STATUS(etcdStore_ != nullptr, K_RUNTIME_ERROR, "EtcdStore is null");
    return etcdStore_->CAS(tableName, key, oldValue, newValue);
}

Status EtcdClusterStore::Delete(const std::string &tableName, const std::string &key)
{
    CHECK_FAIL_RETURN_STATUS(etcdStore_ != nullptr, K_RUNTIME_ERROR, "EtcdStore is null");
    return etcdStore_->Delete(tableName, key);
}

Status EtcdClusterStore::WatchEvents(const std::vector<ClusterWatchElement> &watchKeys)
{
    CHECK_FAIL_RETURN_STATUS(etcdStore_ != nullptr, K_RUNTIME_ERROR, "EtcdStore is null");
    std::vector<WatchElement> etcdWatchKeys;
    etcdWatchKeys.reserve(watchKeys.size());
    for (const auto &watchKey : watchKeys) {
        etcdWatchKeys.emplace_back(WatchElement{ watchKey.tableName, watchKey.key, watchKey.startRevision });
    }
    return etcdStore_->WatchEvents(etcdWatchKeys);
}

Status EtcdClusterStore::InitKeepAlive(const std::string &tableName, const std::string &key, bool isRestart,
                                       bool isStoreAvailableWhenStart)
{
    CHECK_FAIL_RETURN_STATUS(etcdStore_ != nullptr, K_RUNTIME_ERROR, "EtcdStore is null");
    return etcdStore_->InitKeepAlive(tableName, key, isRestart, isStoreAvailableWhenStart);
}

Status EtcdClusterStore::UpdateNodeState(const std::string &state)
{
    CHECK_FAIL_RETURN_STATUS(etcdStore_ != nullptr, K_RUNTIME_ERROR, "EtcdStore is null");
    return etcdStore_->UpdateNodeState(state);
}

Status EtcdClusterStore::GetStorePrefix(const std::string &tableName, std::string &prefix)
{
    CHECK_FAIL_RETURN_STATUS(etcdStore_ != nullptr, K_RUNTIME_ERROR, "EtcdStore is null");
    return etcdStore_->GetEtcdPrefix(tableName, prefix);
}

Status EtcdClusterStore::InformReconciliationDone(const HostPort &workerAddr)
{
    CHECK_FAIL_RETURN_STATUS(etcdStore_ != nullptr, K_RUNTIME_ERROR, "EtcdStore is null");
    return etcdStore_->InformEtcdReconciliationDone(workerAddr);
}

bool EtcdClusterStore::IsKeepAliveTimeout()
{
    return etcdStore_ != nullptr && etcdStore_->IsKeepAliveTimeout();
}

bool EtcdClusterStore::IsCreateFirstLease()
{
    return etcdStore_ != nullptr && etcdStore_->IsCreateFirstLease();
}

void EtcdClusterStore::SetEventHandler(EventHandler &&eventHandler)
{
    if (etcdStore_ == nullptr) {
        return;
    }
    etcdStore_->SetEventHandler(
        [handler = std::move(eventHandler)](mvccpb::Event &&event) mutable { handler(FromEtcdEvent(event)); });
}

void EtcdClusterStore::SetCheckStoreStateWhenNetworkFailedHandler(std::function<bool()> handler)
{
    if (etcdStore_ == nullptr) {
        return;
    }
    etcdStore_->SetCheckEtcdStateWhenNetworkFailedHandler(std::move(handler));
}
}  // namespace datasystem
