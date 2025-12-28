/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
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
 * Description: This class is used to record the reverse mapping of metadata related to ClientId.
 */
#include "datasystem/master/object_cache/device/master_dev_client_meta_manager.h"

#include <memory>
#include <vector>

#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace master {
TbbDeviceMetaOpRecord *MasterDevClientMetaManager::GetTableByType(RecordType type)
{
    switch (type) {
        case RecordType::NPUID:
            return &DeviceMetaOpRecordNpuId_;
        case RecordType::OBJECTKEY:
            return &DeviceMetaOpRecordObjectKey_;
        case RecordType::HCCLPEERID:
            return &DeviceMetaOpRecordHcclPeerId_;
        case RecordType::WORKER2CLIENT:
            return &worker2ClientsTable_;
        default:
            LOG(ERROR) << FormatString("TbbDeviceMetaOpRecord type no find");
            return nullptr;
    }
}

Status MasterDevClientMetaManager::AddValue(const RecordType type, const std::string &key, const std::string &value)
{
    auto table = GetTableByType(type);
    if (table == nullptr) {
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, "TbbDeviceMetaOpRecord type no find");
    }

    TbbDeviceMetaOpRecord::accessor acc;
    if (table->find(acc, key)) {
        (void)acc->second.insert(value);
    } else {
        bool rc = table->insert(std::make_pair(key, std::set<std::string>{ value }));
        if (!rc) {
            RETURN_STATUS_LOG_ERROR(
                K_RUNTIME_ERROR, FormatString("Failed to insert the key %s and value %s into the table.", key, value));
        }
    }
    return Status::OK();
}

std::set<std::string> MasterDevClientMetaManager::GetValue(const RecordType type, const std::string &key)
{
    return GetValueImpl(type, key, false);
}

std::set<std::string> MasterDevClientMetaManager::GetValueAndErase(const RecordType type, const std::string &key)
{
    return GetValueImpl(type, key, true);
}

std::set<std::string> MasterDevClientMetaManager::GetValueImpl(const RecordType type, const std::string &key,
                                                               bool eraseAfterGet)
{
    auto table = GetTableByType(type);
    if (table == nullptr) {
        return {};
    }
    TbbDeviceMetaOpRecord::accessor acc;
    bool found = table->find(acc, key);
    if (found) {
        std::set<std::string> records = (acc->second);

        // Erase the entry if requested
        if (eraseAfterGet) {
            table->erase(acc);
        }
        return records;
    } else {
        LOG(INFO) << FormatString("Cannot find the key %s in the table of %s record type.", key, EnumToString(type));
    }
    return {};
}

Status MasterDevClientMetaManager::EraseValue(const RecordType type, const std::string &key, const std::string &value)
{
    auto table = GetTableByType(type);
    RETURN_RUNTIME_ERROR_IF_NULL(table);
    TbbDeviceMetaOpRecord::accessor acc;
    if (table->find(acc, key)) {
        // Delete the values of the v (set) in k,v table
        acc->second.erase(value);

        // If set is empty, delete the entire entry from the table
        if (acc->second.empty()) {
            table->erase(acc);
        }
    } else {
        LOG(INFO) << FormatString("Cannot find the key in the table of %s record type, key:%s, value:%s.",
                                  EnumToString(type), key, value);
    }
    return Status::OK();
}

std::string MasterDevClientMetaManager::EnumToString(RecordType value)
{
    auto it = enumToString_.find(value);
    if (it != enumToString_.end()) {
        return it->second;
    } else {
        return "Unknown";
    }
}

}  // namespace master
}  // namespace datasystem