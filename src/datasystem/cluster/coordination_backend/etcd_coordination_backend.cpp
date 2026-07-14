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

/**
 * Description: ETCD adapter for the cluster coordination backend contract.
 */
#include "datasystem/cluster/coordination_backend/etcd_coordination_backend.h"

#include <utility>

#include "datasystem/common/util/status_helper.h"

namespace datasystem::cluster {

EtcdCoordinationBackend::EtcdCoordinationBackend(EtcdStore *etcdStore) : etcdStore_(etcdStore)
{
}

CoordinationEvent EtcdCoordinationBackend::FromEtcdEvent(const mvccpb::Event &event)
{
    CoordinationEvent output;
    output.type =
        event.type() == mvccpb::Event_EventType_DELETE ? CoordinationEventType::DELETE : CoordinationEventType::PUT;
    output.key = event.kv().key();
    output.value = event.kv().value();
    output.version = event.kv().version();
    output.revision = event.kv().mod_revision();
    return output;
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

Status EtcdCoordinationBackend::Delete(const std::string &tableName, const std::string &key, int timeoutMs)
{
    CHECK_FAIL_RETURN_STATUS(etcdStore_ != nullptr, K_RUNTIME_ERROR, "EtcdStore is null");
    return etcdStore_->Delete(tableName, key, 0, timeoutMs);
}

Status EtcdCoordinationBackend::WatchEvents(const std::vector<WatchKey> &watchKeys)
{
    CHECK_FAIL_RETURN_STATUS(etcdStore_ != nullptr, K_RUNTIME_ERROR, "EtcdStore is null");
    std::vector<WatchElement> descriptors;
    descriptors.reserve(watchKeys.size());
    for (const auto &watchKey : watchKeys) {
        descriptors.emplace_back(
            WatchElement{ watchKey.tableName, watchKey.key, watchKey.startRevision, !watchKey.key.empty() });
    }
    return etcdStore_->WatchEvents(descriptors);
}

Status EtcdCoordinationBackend::InitKeepAlive(const std::string &tableName, const std::string &key, bool isRestart,
                                              bool isStoreAvailableWhenStart)
{
    CHECK_FAIL_RETURN_STATUS(etcdStore_ != nullptr, K_RUNTIME_ERROR, "EtcdStore is null");
    return etcdStore_->InitKeepAlive(tableName, key, isRestart, isStoreAvailableWhenStart);
}

Status EtcdCoordinationBackend::ShutdownEventSources()
{
    if (etcdStore_ == nullptr) {
        return Status::OK();
    }
    Status rc = etcdStore_->ShutdownEventSources();
    etcdStore_->SetEventHandler({});
    etcdStore_->SetCheckEtcdStateWhenNetworkFailedHandler(nullptr);
    return rc;
}

Status EtcdCoordinationBackend::Shutdown()
{
    return etcdStore_ == nullptr ? Status::OK() : etcdStore_->Shutdown();
}

Status EtcdCoordinationBackend::UpdateNodeState(MemberLifecycleState state)
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
    return etcdStore_->InformReconciliationDone(workerAddr);
}

bool EtcdCoordinationBackend::IsKeepAliveTimeout()
{
    return etcdStore_ != nullptr && etcdStore_->IsKeepAliveTimeout();
}

bool EtcdCoordinationBackend::IsFirstKeepAliveSent()
{
    return etcdStore_ != nullptr && etcdStore_->IsFirstKeepAliveSent();
}

void EtcdCoordinationBackend::SetEventHandler(EventHandler &&eventHandler)
{
    if (etcdStore_ == nullptr) {
        return;
    }
    etcdStore_->SetEventHandler([handler = std::move(eventHandler)](mvccpb::Event &&event) mutable {
        if (handler) {
            handler(FromEtcdEvent(event));
        }
    });
}

void EtcdCoordinationBackend::SetCheckStoreStateWhenNetworkFailedHandler(std::function<bool()> handler)
{
    if (etcdStore_ != nullptr) {
        etcdStore_->SetCheckEtcdStateWhenNetworkFailedHandler(std::move(handler));
    }
}

}  // namespace datasystem::cluster
