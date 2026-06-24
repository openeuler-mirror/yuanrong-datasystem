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
 * Description: Etcd-backed topology coordination backend.
 */
#include "datasystem/topology/coordination_backend/etcd_coordination_backend.h"

#include <utility>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace topology {

EtcdCoordinationBackend::EtcdCoordinationBackend(EtcdStore *etcdStore) : etcdStore_(etcdStore)
{
}

CoordinationEvent EtcdCoordinationBackend::FromEtcdEvent(const mvccpb::Event &event)
{
    CoordinationEvent clusterEvent;
    clusterEvent.type =
        event.type() == mvccpb::Event_EventType_DELETE ? CoordinationEventType::DELETE : CoordinationEventType::PUT;
    clusterEvent.key = event.kv().key();
    clusterEvent.value = event.kv().value();
    clusterEvent.version = event.kv().version();
    clusterEvent.revision = event.kv().mod_revision();
    return clusterEvent;
}

Status EtcdCoordinationBackend::GetAll(const std::string &tableName,
                                       std::vector<std::pair<std::string, std::string>> &outKeyValues)
{
    CHECK_FAIL_RETURN_STATUS(etcdStore_ != nullptr, K_RUNTIME_ERROR, "EtcdStore is null");
    return etcdStore_->GetAll(tableName, outKeyValues);
}

Status EtcdCoordinationBackend::Get(const std::string &tableName, const std::string &key, std::string &value)
{
    CHECK_FAIL_RETURN_STATUS(etcdStore_ != nullptr, K_RUNTIME_ERROR, "EtcdStore is null");
    return etcdStore_->Get(tableName, key, value);
}

Status EtcdCoordinationBackend::Get(const std::string &tableName, const std::string &key, RangeSearchResult &res,
                                    int32_t timeoutMs)
{
    CHECK_FAIL_RETURN_STATUS(etcdStore_ != nullptr, K_RUNTIME_ERROR, "EtcdStore is null");
    return etcdStore_->Get(tableName, key, res, timeoutMs);
}

Status EtcdCoordinationBackend::CAS(const std::string &tableName, const std::string &key,
                                    const ProcessFunction &processFunc, RangeSearchResult &res)
{
    CHECK_FAIL_RETURN_STATUS(etcdStore_ != nullptr, K_RUNTIME_ERROR, "EtcdStore is null");
    return etcdStore_->CAS(tableName, key, processFunc, res);
}

Status EtcdCoordinationBackend::CAS(const std::string &tableName, const std::string &key,
                                    const ProcessFunction &processFunc)
{
    CHECK_FAIL_RETURN_STATUS(etcdStore_ != nullptr, K_RUNTIME_ERROR, "EtcdStore is null");
    return etcdStore_->CAS(tableName, key, processFunc);
}

Status EtcdCoordinationBackend::CAS(const std::string &tableName, const std::string &key, const std::string &oldValue,
                                    const std::string &newValue)
{
    CHECK_FAIL_RETURN_STATUS(etcdStore_ != nullptr, K_RUNTIME_ERROR, "EtcdStore is null");
    return etcdStore_->CAS(tableName, key, oldValue, newValue);
}

Status EtcdCoordinationBackend::Delete(const std::string &tableName, const std::string &key)
{
    CHECK_FAIL_RETURN_STATUS(etcdStore_ != nullptr, K_RUNTIME_ERROR, "EtcdStore is null");
    return etcdStore_->Delete(tableName, key);
}

Status EtcdCoordinationBackend::WatchEvents(const std::vector<WatchKey> &watchKeys)
{
    CHECK_FAIL_RETURN_STATUS(etcdStore_ != nullptr, K_RUNTIME_ERROR, "EtcdStore is null");
    std::vector<WatchElement> etcdWatchKeys;
    etcdWatchKeys.reserve(watchKeys.size());
    for (const auto &watchKey : watchKeys) {
        etcdWatchKeys.emplace_back(
            datasystem::WatchElement{ watchKey.tableName, watchKey.key, watchKey.startRevision });
    }
    return etcdStore_->WatchEvents(etcdWatchKeys);
}

Status EtcdCoordinationBackend::InitKeepAlive(const std::string &tableName, const std::string &key, bool isRestart,
                                              bool isStoreAvailableWhenStart)
{
    CHECK_FAIL_RETURN_STATUS(etcdStore_ != nullptr, K_RUNTIME_ERROR, "EtcdStore is null");
    return etcdStore_->InitKeepAlive(tableName, key, isRestart, isStoreAvailableWhenStart);
}

Status EtcdCoordinationBackend::UpdateNodeState(const std::string &state)
{
    CHECK_FAIL_RETURN_STATUS(etcdStore_ != nullptr, K_RUNTIME_ERROR, "EtcdStore is null");
    return etcdStore_->UpdateNodeState(state);
}

Status EtcdCoordinationBackend::GetStorePrefix(const std::string &tableName, std::string &prefix)
{
    CHECK_FAIL_RETURN_STATUS(etcdStore_ != nullptr, K_RUNTIME_ERROR, "EtcdStore is null");
    return etcdStore_->GetEtcdPrefix(tableName, prefix);
}

Status EtcdCoordinationBackend::InformReconciliationDone(const HostPort &workerAddr)
{
    CHECK_FAIL_RETURN_STATUS(etcdStore_ != nullptr, K_RUNTIME_ERROR, "EtcdStore is null");
    return etcdStore_->InformEtcdReconciliationDone(workerAddr);
}

bool EtcdCoordinationBackend::IsKeepAliveTimeout()
{
    return etcdStore_ != nullptr && etcdStore_->IsKeepAliveTimeout();
}

bool EtcdCoordinationBackend::IsCreateFirstLease()
{
    return etcdStore_ != nullptr && etcdStore_->IsCreateFirstLease();
}

void EtcdCoordinationBackend::SetEventHandler(EventHandler &&eventHandler)
{
    if (etcdStore_ == nullptr) {
        return;
    }
    etcdStore_->SetEventHandler(
        [handler = std::move(eventHandler)](mvccpb::Event &&event) mutable { handler(FromEtcdEvent(event)); });
}

void EtcdCoordinationBackend::SetCheckStoreStateWhenNetworkFailedHandler(std::function<bool()> handler)
{
    if (etcdStore_ == nullptr) {
        return;
    }
    etcdStore_->SetCheckEtcdStateWhenNetworkFailedHandler(std::move(handler));
}
}  // namespace topology
}  // namespace datasystem
