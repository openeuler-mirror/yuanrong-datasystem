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

/**
 * Description: Etcd wrapper for slot recovery coordination data.
 */
#include "datasystem/worker/object_cache/slot_recovery/slot_recovery_store.h"

#include "datasystem/common/kvstore/etcd/etcd_constants.h"

namespace datasystem {
namespace object_cache {
namespace {
Status ParseIncidentValue(const std::string &value, SlotRecoveryInfoPb &info)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(info.ParseFromString(value), K_RUNTIME_ERROR,
                                         "Parse SlotRecoveryInfoPb failed");
    return Status::OK();
}
} // namespace

Status SlotRecoveryStore::Init()
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(etcdStore_ != nullptr, K_INVALID, "etcdStore is null");
    RETURN_IF_NOT_OK_EXCEPT(etcdStore_->CreateTable(ETCD_SLOT_RECOVERY_TABLE, ETCD_SLOT_RECOVERY_TABLE), K_DUPLICATED);
    return Status::OK();
}

Status SlotRecoveryStore::GetIncident(const std::string &failedWorker, SlotRecoveryInfoPb &info)
{
    std::string value;
    RETURN_IF_NOT_OK(etcdStore_->Get(ETCD_SLOT_RECOVERY_TABLE, failedWorker, value));
    return ParseIncidentValue(value, info);
}

Status SlotRecoveryStore::ListIncidents(std::vector<std::pair<std::string, SlotRecoveryInfoPb>> &incidents)
{
    std::vector<std::pair<std::string, std::string>> keyValues;
    RETURN_IF_NOT_OK(etcdStore_->GetAll(ETCD_SLOT_RECOVERY_TABLE, keyValues));
    incidents.clear();
    incidents.reserve(keyValues.size());
    for (const auto &keyValue : keyValues) {
        SlotRecoveryInfoPb info;
        RETURN_IF_NOT_OK(ParseIncidentValue(keyValue.second, info));
        incidents.emplace_back(keyValue.first, std::move(info));
    }
    return Status::OK();
}

Status SlotRecoveryStore::DeleteIncident(const std::string &failedWorker)
{
    return etcdStore_->Delete(ETCD_SLOT_RECOVERY_TABLE, failedWorker);
}

Status SlotRecoveryStore::UpdateIncident(const std::string &failedWorker, const SlotRecoveryInfoPb &info)
{
    return etcdStore_->Put(ETCD_SLOT_RECOVERY_TABLE, failedWorker, info.SerializeAsString());
}

Status SlotRecoveryStore::CASIncident(const std::string &failedWorker, const IncidentMutator &mutator)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(mutator != nullptr, K_INVALID, "incident mutator is null");
    return etcdStore_->CAS(
        ETCD_SLOT_RECOVERY_TABLE, failedWorker,
        [&mutator](const std::string &oldValue, std::unique_ptr<std::string> &newValue, bool &retry) {
            SlotRecoveryInfoPb info;
            bool existed = !oldValue.empty();
            if (existed) {
                RETURN_IF_NOT_OK(ParseIncidentValue(oldValue, info));
            }
            bool writeBack = false;
            RETURN_IF_NOT_OK(mutator(info, existed, writeBack));
            if (!writeBack) {
                newValue = nullptr;
                return Status::OK();
            }
            newValue = std::make_unique<std::string>(info.SerializeAsString());
            retry = true;
            return Status::OK();
        });
}

}  // namespace object_cache
}  // namespace datasystem
