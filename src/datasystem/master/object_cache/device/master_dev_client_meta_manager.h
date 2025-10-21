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
 * Description: Defines the class to manage hccl rootinfo, established in destination npus.
 */
#ifndef DATASYSTEM_MASTER_OBJECT_CACHE_DEVICE_MASTER_DEV_REQ_Device_Meta_Op_Record
#define DATASYSTEM_MASTER_OBJECT_CACHE_DEVICE_MASTER_DEV_REQ_Device_Meta_Op_Record

#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/p2p_subscribe.pb.h"

namespace datasystem {
namespace master {

enum class RecordType { NPUID = 1, OBJECTKEY = 2, HCCLPEERID = 3, WORKER2CLIENT = 4 };

// Record Map. key -> {set<value>}.
using TbbDeviceMetaOpRecord = tbb::concurrent_hash_map<ImmutableString, std::set<std::string>>;

class MasterDevClientMetaManager {
public:
    /**
     * @brief Insert input values into the {type} table according to clientId
     * @param[in] type The type of use table {npuid, objectkey, hcclpeerid}.
     * @param[in] key The identifier (could be client id, worker address, or other context-specific identifier).
     * @param[in] value Universal string value.
     * @return K_OK on success; the error code otherwise.
     */
    Status AddValue(const RecordType type, const std::string &key, const std::string &value);

    /**
     * @brief Retrieve the value set associated with the specified key from the record table
     * @param[in] type The type of record table to query {npuid, objectkey, hcclpeerid, worker2client, etc.}
     * @param[in] key The identifier used to lookup the value set (client id, worker address,
     *               or other context-specific identifier)
     * @return The value set associated with the key. Returns an empty set if the key is not found
     *         or if the specified table does not exist
     */
    std::set<std::string> GetValue(const RecordType type, const std::string &key);

    /**
     * @brief Retrieve the set array from the {type} table based on clientid
     * @param[in] type The type of use table {npuid, objectkey, hcclpeerid}.
     * @param[in] key The identifier (could be client id, worker address, or other context-specific identifier).
     * @return The getting value set from the record table.
     */
    std::set<std::string> GetValueAndErase(const RecordType type, const std::string &key);

    /**
     * @brief Delete the specified value in k(client id)-v(value set) of the {type} table
     * @param[in] type The type of use table {npuid,objectkey}.
     * @param[in] key The identifier (could be client id, worker address, or other context-specific identifier).
     * @param[in] value need to delete value
     * @return K_OK on success; the error code otherwise.
     */
    Status EraseValue(const RecordType type, const std::string &key, const std::string &value);

    /**
     * @brief Convert a RecordType enum value to its corresponding string representation
     * @param[in] value The RecordType enumeration value to be converted to string
     * @return String representation of the provided RecordType value
     */
    std::string EnumToString(RecordType value);

private:
    /**
     * @brief Return pointers of different tables based on the type value.
     * @param[in] type The type of use table {npuid, objectkey, hcclpeerid}.
     * @return The TbbDeviceMetaOpRecord point
     */
    std::shared_ptr<TbbDeviceMetaOpRecord> GetTableByType(RecordType type);

    /**
     * @brief Internal implementation: Looks up value by key with optional erase after retrieval
     * @param type Record type used to determine which table to query
     * @param key The key to search for in the table
     * @param eraseAfterGet If true, removes the key-value pair from table after retrieval
     * @return The set of values associated with the key, or empty set if key not found
     */
    std::set<std::string> GetValueImpl(const RecordType type, const std::string &key, bool eraseAfterGet);

    // clientId -> {clientId: npuId}.
    // Pub: NotifyClientClearMeta,UpdateNpuPendingGetReqsTable
    // Sub: ReleaseMetaData.
    TbbDeviceMetaOpRecord DeviceMetaOpRecordNpuId_;

    // clientId -> {clientId: ObjectKey}. Pub: PutP2PMetaImpl , Sub: ReleaseMetaData.
    TbbDeviceMetaOpRecord DeviceMetaOpRecordObjectKey_;

    // clientId -> {clientId: HcclPeerId}. just for rootinfo
    TbbDeviceMetaOpRecord DeviceMetaOpRecordHcclPeerId_;

    // workerAddress -> {set<clientid>}
    // Record the client request from which worker for future cleanup the client metadata When worker is scaled down.
    TbbDeviceMetaOpRecord worker2ClientsTable_;

    std::unordered_map<RecordType, std::string> enumToString_ = { { RecordType::NPUID, "NPUID" },
                                                                  { RecordType::OBJECTKEY, "OBJECTKEY" },
                                                                  { RecordType::HCCLPEERID, "HCCLPEERID" },
                                                                  { RecordType::WORKER2CLIENT, "WORKER2CLIENT" } };
};

}  // namespace master
}  // namespace datasystem

#endif
