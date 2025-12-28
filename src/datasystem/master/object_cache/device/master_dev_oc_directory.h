/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Defines classes of device object directory and related subscription tables, i.e., object get request
 * table.
 */
#ifndef DATASYSTEM_MASTER_OBJECT_CACHE_DEVICE_MASTER_DEV_OC_DIRECTORY_H
#define DATASYSTEM_MASTER_OBJECT_CACHE_DEVICE_MASTER_DEV_OC_DIRECTORY_H

#include <functional>
#include <google/protobuf/repeated_field.h>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/util/request_table.h"
#include "datasystem/client/hetero_cache/device_util.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/protos/p2p_subscribe.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace master {

class ObjectDirectory {
public:
    ObjectDirectory(DeviceObjectMetaPb metaPb);

    /**
     * @brief Get the lock guard to lock the directory instance.
     * @return The lock guard.
     */
    std::lock_guard<std::recursive_mutex> GetLockGuard();

    /**
     * @brief Get the lifetime of device object directory.
     * @return The lifetime.
     */
    LifetimeType GetLifetime() const;

    /**
     * @brief Get the datainfos of device object directory.
     * @return The repeat field of DataInfo Pb.
     */
    google::protobuf::RepeatedPtrField<DataInfoPb> GetDataInfos();

    /**
     * @brief Find location in directory.
     * @param[in] clientId  The client id.
     * @param[in] deviceId The device id.
     * @return [found, location] The result of finding location.
     */
    std::tuple<bool, DeviceLocationPb *> FindLocation(const std::string &clientId, int32_t deviceId);

    /**
     * @brief Verifies data is legal between sender and receiver.
     *
     * This function performs the following validations:
     * 1. Checks if the number of data blobs matches between put and get operations.
     * 2. Validates data types for each corresponding blob.
     * 3. Ensures the requested offset and size do not overflow.
     * 4. Confirms the requested data range does not exceed available data boundaries.
     * 5. Confirms the requested locations are not already present in the directory (ensures uniqueness).
     *
     * @param[in] getMetaPb The device meta pb from GetP2PMeta request.
     * @return Status Returns Status::OK() if validation passes,
     *                otherwise returns an appropriate error status.
     */
    Status VerifyDataRangeLegality(const DeviceObjectMetaPb &getMetaPb);

    /**
     * @brief Add location to directory.
     * @param[in] clientId  The client id.
     * @param[in] deviceId The device id.
     */
    void AddLocation(const std::string &clientId, int32_t deviceId);

    /**
     * @brief Select a valid location to client.
     * @param[in] selectFunc lamda function.
     * @param[out] loc The selected location.
     * @return K_OK on success; the error code otherwise.
     */
    template <typename F>
    Status SelectLocation(F &&selectFunc, DeviceLocationPb *&loc)
    {
        std::vector<DeviceLocationPb *> locPtrVec;
        locPtrVec.reserve(locMap_.size());
        for (auto &iter : locMap_) {
            if (iter.second.location_state() != PENDING_REMOVE) {
                locPtrVec.emplace_back(&iter.second);
            }
        }
        return selectFunc(locPtrVec, loc);
    }

    /**
     * @brief Update location state after select.
     * @param[in] loc The selected location.
     */
    void UpdateLocationAfterSelect(DeviceLocationPb *loc);

    /**
     * @brief Update location when ack.
     * @param[in] clientId  The client id.
     * @param[in] deviceId The device id.
     * @return K_OK on success; the error code otherwise.
     */
    Status AckLocation(const std::string &clientId, int32_t deviceId);

    /**
     * @brief Clear all locations and notify clients to clear meta.
     * @return K_OK on success; the error code otherwise.
     */
    Status ClearAllLocations();

    /**
     * @brief Clear all locations and notify clients except the specified one to clear meta.
     * @param clientId The identifier of the client to be excluded.
     * @return K_OK on success; the error code otherwise.
     */
    Status ClearAllLocationsExceptClient(const std::string &clientId);

    /**
     * @brief Remove single location.
     * @param[in] clientId  The client id.
     * @param[in] deviceId The device id.
     * @return K_OK on success; the error code otherwise.
     */
    Status RemoveLocation(const std::string &clientId, int32_t deviceId);

    /**
     * @brief Register callback.
     * @param[in] removeLocCallback The callback when a location is removed.(Notify client to clear meta.)
     * @param[in] removeDirCallback The callback when all locations are removed.(Notify master to erase directory.)
     */
    static void RegisterCallback(
        std::function<void(const std::string &, const std::string &, int32_t)> removeLocCallBack,
        std::function<void(const std::string &)> removeDirCallBack);

    /**
     * @brief Get device meta protobuf message.
     * @return The DeviceObjectMetaPb message.
     */
    const DeviceObjectMetaPb GetMetaPb() const;

    /**
     * @brief remove the location info of client
     * @param[in] clientId The client id.
     * @return K_OK on success; the error code otherwise.
     */
    Status ClearClientAllLocations(const std::string &clientId);

private:
    inline static std::function<void(const std::string &, const std::string &, int32_t)> removeLocCallBack_;
    inline static std::function<void(const std::string &)> removeDirCallBack_;

    DeviceObjectMetaPb metaPb_;
    std::unordered_map<std::string, DeviceLocationPb> locMap_;
    std::recursive_mutex rMutex_;
};

// Object Directory. ObjectKey -> (Locations, Properties). Pub: Put, Sub: Get.
using TbbObjDirTable = tbb::concurrent_hash_map<ImmutableString, std::shared_ptr<ObjectDirectory>>;

class ObjectDirectoryTable {
public:
    /**
     * @brief Put object to directory table. Check duplicate
     * publishing of the same object.If not duplicate, update the
     * object directory, i.e., object primary copy location and property.
     * @param[in] objectKey The object key.
     * @param[in] metaPb The meta pb message.
     * @param[out] objDirectory The object directory of given object key.
     * @return K_OK on success; the error code otherwise.
     */
    Status PutObject(const std::string &objectKey, const DeviceObjectMetaPb &metaPb,
                     std::shared_ptr<ObjectDirectory> &objDirectory);

    /**
     * @brief Get object directory.
     * @param[in] objectKey The object key.
     * @param[out] objectDirectory The object directory.
     * @return K_OK on success; the error code otherwise.
     */
    Status GetObjectDirectory(const std::string &objectKey, std::shared_ptr<ObjectDirectory> &objectDirectory);

    /**
     * @brief Get object dataInfos. Perform onGetCallback with directory as parameters on successful get.
     * @param[in] objectKey The object key.
     * @param[in] onGetCallback The callback to perform operations on get dataInfos success.
     * @return K_OK on success; the error code otherwise.
     */
    Status GetDataInfos(const std::string &objectKey,
                        const std::function<void(std::shared_ptr<ObjectDirectory> &directory)> &onGetCallback);

    /**
     * @brief Acknowledge get operation finished, which indicates location is not pinned.
     * @param[in] objectKey The object key.
     */
    void RemoveObjectDirectory(const std::string &objectKey);

    /**
     * @brief Delete the location, and then if the object is empty,
     * delete the object in object directory table
     * @param[in] objectKeySet Need to be delete object set
     * @param[in] npuidList Used to delete a location. the value
     * comes from the client id that is the same as the object key.
     * @param[in] addObjectKeySet Need to be return object key list, which should not be delete
     */
    void EraseObjectDirectoryWhenLocNone(const std::set<std::string> &objectKeySet,
                                         const std::set<std::string> &npuidList,
                                         std::set<std::string> &addObjectKeySet);

    /**
     * @brief Fill migrate data.
     * @param[out] req The migrate rpc request
     */
    void FillMigrateData(MigrateMetadataReqPb &req);

    /**
     * @brief Save migrate data.
     * @param[in] req The migrate rpc request
     */
    void SaveMigrateData(const MigrateMetadataReqPb &req);

    /**
     * @brief Clear NpuEvent table.
     */
    void Clear();

private:
    // ObjectKey -> (Locations, Properties).
    TbbObjDirTable objDirectoryTable_;
    std::shared_timed_mutex objDirTableMutex_;
};

struct GetDataInfosEntryParams {
    static std::shared_ptr<GetDataInfosEntryParams> ConstructGetDataInfosEntryParams(
        std::shared_ptr<ObjectDirectory> &objDirectory)
    {
        return std::make_shared<GetDataInfosEntryParams>(
            GetDataInfosEntryParams{ .dataInfos = objDirectory->GetDataInfos() });
    }

    const google::protobuf::RepeatedPtrField<DataInfoPb> dataInfos;
};
using GetDataInfosRequest = UnaryRequest<GetDataInfoReqPb, GetDataInfoRspPb, GetDataInfosEntryParams>;
using OnGetObjSatisfiedCallback =
    std::function<void(const std::shared_ptr<GetDataInfosEntryParams> &param, const std::string &objectKey,
                       const std::string &dstClientId, int32_t dstDeviceId)>;
class ObjectGetDataInfosReqSubscriptionTable {
public:
    explicit ObjectGetDataInfosReqSubscriptionTable() = default;

    /**
     * @brief Add GetDataInfos request to ObjectGetDataInfosReqSubscriptionTable.
     * @param[in] objectKey The object key.
     * @param[in] request The GetDataInfos request that is waiting on the object key.
     * @return Status of the call.
     */
    Status AddGetDataInfosRequest(const std::string &objectKey, std::shared_ptr<GetDataInfosRequest> &request);

    /**
     * @brief Remove the GetDataInfos request from the waiting requests table.
     * @param[in] request The request need to remove.
     */
    void RemoveGetDataInfosRequest(std::shared_ptr<GetDataInfosRequest> &request);

    /**
     * @brief Update the GetDataInfos request info after object is put.
     * @param[in] objectKey The object key.
     * @param[in] objDirectory The p2p metadata.
     * @return Status of the call.
     */
    Status UpdateGetDataInfosRequest(const std::string &objectKey, std::shared_ptr<ObjectDirectory> &objDirectory);

    /**
     * @brief Reply to client with the device GetDataInfos request.
     * @param[in] request The GetDataInfos request which to return.
     * @return Status of the call.
     */
    Status ReturnGetDataInfosRequest(std::shared_ptr<GetDataInfosRequest> request);

    /**
     * @brief Checks whether the DataInfos in a Get request is consistent with that in a Put request.
     * @param[in] dataInfosFromPut dataInfos carried in the Put request
     * @param[in] dataInfosFromGet dataInfos carried in the Get request
     * @return The concat result.
     */
    static Status VerifyDataInfos(const std::vector<DataInfo> &dataInfosFromPut,
                                  const std::vector<DataInfo> &dataInfosFromGet);

    /**
     * @brief Deletes a column from the subscription table based on the input value.
     * @param[in] objectKey The object key.
     */
    void EraseDataInfosReqSubscription(const std::string &objectKey);

private:
    RequestTable<GetDataInfosRequest> getDataInfosRequestTable_;
};

struct GetP2PMetaEntryParams {
    static std::shared_ptr<GetP2PMetaEntryParams> ConstructGetP2PMetaEntryParams(std::string srcClientId,
                                                                                 int32_t srcDeviceId,
                                                                                 std::string srcWorkerIP)
    {
        return std::make_shared<GetP2PMetaEntryParams>(GetP2PMetaEntryParams{
            .srcClientId_ = srcClientId, .srcDeviceId_ = srcDeviceId, .srcWorkerIP_ = srcWorkerIP });
    }

    std::string srcClientId_;
    int32_t srcDeviceId_;
    std::string srcWorkerIP_;
};

using GetP2PMetaRequest = UnaryRequest<GetP2PMetaReqPb, GetP2PMetaRspPb, GetP2PMetaEntryParams>;

class ObjectGetP2PMetaReqSubscriptionTable {
public:
    explicit ObjectGetP2PMetaReqSubscriptionTable() = default;

    /**
     * @brief Add GetP2PMeta request to ObjectGetP2PMetaReqSubscriptionTable.
     * @param[in] objectKey The object key.
     * @param[in] request The GetP2PMeta request that is waiting on the object key.
     * @return Status of the call.
     */
    Status AddGetP2PMetaRequest(const std::string &objectKey, std::shared_ptr<GetP2PMetaRequest> &request);

    /**
     * @brief Remove the GetP2PMeta request from the waiting requests table.
     * @param[in] request The request need to remove.
     */
    void RemoveGetP2PMetaRequest(std::shared_ptr<GetP2PMetaRequest> &request);
    /**
     * @brief Update the GetP2PMeta request info after object is put.
     * @param[in] objectKey The object key.
     * @param[in] srcClientId The src client ID.
     * @param[in] srcDeviceId The src device ID.
     * @return Status of the call.
     */
    Status UpdateGetP2PMetaRequest(
        const std::string &objectKey, std::string srcClientId, int32_t srcDeviceId, std::string srcWorkerIP,
        const std::unordered_map<std::shared_ptr<GetP2PMetaRequest>, Status> &requestVerificationResults);

    /**
     * @brief Find the GetP2PMeta request according to object key.
     * @param[in] objectKey The object key.
     * @param[in] request The GetP2PMeta request.
     */
    std::vector<std::shared_ptr<GetP2PMetaRequest>> GetAllP2PMetaRequest(const std::string &objectKey);

    /**
     * @brief Reply to client with the device GetP2PMeta request.
     * @param[in] request The GetP2PMeta request which to return.
     * @return Status of the call.
     */
    Status ReturnGetP2PMetaRequest(std::shared_ptr<GetP2PMetaRequest> request);

private:
    RequestTable<GetP2PMetaRequest> getP2PMetaRequestTable_;
};

/**
 * @brief Parse the DataInfo from DataInfoPb
 * @param[in] pb The DataInfoPb protobuf message.
 * @return The DataInfo object.
 */
DataInfo ParseDataInfoFromPb(const DataInfoPb &pb);

/**
 * @brief Parse the DataInfos from repeated field of DataInfoPb
 * @param[in] dataInfosRepeatedField The repeated field of DataInfoPb
 * @return The vector of DataInfo object.
 */
std::vector<DataInfo> ParseDataInfosFromRepeatedField(
    const google::protobuf::RepeatedPtrField<DataInfoPb> &dataInfosRepeatedField);
}  // namespace master
}  // namespace datasystem
#endif  // DATASYSTEM_MASTER_OBJECT_CACHE_DEVICE_MASTER_DEV_OC_DIRECTORY_H
