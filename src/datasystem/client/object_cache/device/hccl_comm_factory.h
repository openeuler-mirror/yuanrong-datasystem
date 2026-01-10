/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
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

#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_DEVICE_HCCL_COMM_FACTORY_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_DEVICE_HCCL_COMM_FACTORY_H

#include <future>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include <tbb/concurrent_hash_map.h>

#include "datasystem/client/object_cache/client_worker_api.h"
#include "datasystem/client/hetero_cache/device_util.h"
#include "datasystem/client/object_cache/device/hccl_comm_magr.h"
#include "datasystem/common/device/ascend/cann_types.h"
#include "datasystem/common/device/ascend/hccl_comm_wrapper.h"
#include "datasystem/common/device/ascend/p2phccl_comm_wrapper.h"
#include "datasystem/common/device/ascend/p2phccl_types.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {

enum class P2PEventType { SEND, RECV };

using TbbHcclCommTable = tbb::concurrent_hash_map<std::string, std::shared_ptr<CommWrapperBase>>;

class ClientDeviceCurd {
public:
    ClientDeviceCurd(std::shared_ptr<object_cache::IClientWorkerApi> workerApi)
        : aclImpl_(acl::AclDeviceManager::Instance()), clientWorkerApi_(std::move(workerApi))
    {
    }

    ClientDeviceCurd(const ClientDeviceCurd &) = delete;

    ClientDeviceCurd &operator=(const ClientDeviceCurd &) = delete;

    virtual void ShutDown(){};

    virtual ~ClientDeviceCurd() = default;

protected:
    acl::AclDeviceManager *aclImpl_;
    std::shared_ptr<object_cache::IClientWorkerApi> clientWorkerApi_;
};

class HcclCommFactory : public ClientDeviceCurd {
public:
    HcclCommFactory(std::shared_ptr<object_cache::IClientWorkerApi> workerApi, AclResourceManager *aclResourceMgr);

    void ShutDown();

    ~HcclCommFactory();

    /**
     * @brief Get the hccl communicator wrapper, or create it if not existed.
     * @param[in] eventType The p2p event type: SEND or RECV
     * @param[in] localDeviceId The local client device id.
     * @param[in] remoteClientId The client id of the remote client that ready to create communicator.
     * @param[in] remoteDeviceId The device id of the remote client.
     * @param[out] comm The hccl communicator wrapper.
     * @return The status of call.
     */
    Status GetOrCreateHcclComm(P2PEventType eventType, int32_t localDeviceId, const std::string &remoteClientId,
                               int32_t remoteDeviceId, bool isSameNode, bool enableP2Ptransfer,
                               std::shared_ptr<CommWrapperBase> &comm);

    /**
     * @brief Create the hccl communicator wrapper in the send side.
     * @param[in] localDeviceId The local client device id.
     * @param[in] remoteClientId The client id of the remote client that ready to create communicator.
     * @param[in] remoteDeviceId The device id of the remote client.
     * @param[out] comm The hccl communicator wrapper.
     */
    void CreateHcclCommInSend(int32_t localDeviceId, const std::string &remoteClientId, int32_t remoteDeviceId,
                              bool isSameNode, std::shared_ptr<CommWrapperBase> &comm);

    /**
     * @brief Create the hccl communicator wrapper in the recv side.
     * Attention! It should be thread safe to avoid hccl interface coredump.
     * @param[in] localDeviceId The local client device id.
     * @param[in] remoteClientId The client id of the remote client that ready to create communicator.
     * @param[in] remoteDeviceId The device id of the remote client.
     * @param[out] comm The hccl communicator wrapper.
     */
    void CreateHcclCommInRecv(int32_t localDeviceId, const std::string &remoteClientId, int32_t remoteDeviceId,
                              bool isSameNode, std::shared_ptr<CommWrapperBase> &comm);

    Status CreateHcclCommCheckError(std::shared_ptr<CommWrapperBase> &comm);

    /**
     * @brief Get all hccl communicator.
     * @return The list of hccl communicator wrapper.
     */
    std::vector<std::shared_ptr<CommWrapperBase>> GetAllHcclComm();

    /**
     * @brief Create the number of hccl communicator wrapper in table.
     * @return The number of hccl communicator
     */
    size_t GetHcclCommSize();

    /**
     * @brief Delete comm from commtable_ of factory for operation before comm destructor.
     * @param[in] commId The std::shared_ptr<HcclCommWrapper> id, can be find in comm->GetCommId()
     * @return Indicates status whether the deletion is successful.
     */
    Status DelComm(std::string commId);

    /**
     * @brief Destroy the hccl communicator with commId.
     * @param[in] commId The std::shared_ptr<HcclCommWrapper> id, can be find in comm->GetCommId()
     */
    void DestroyHcclComm(std::string commId);

    /**
     * @brief Used for status comparison. What status should be set for comm
     * @param[in] comm whose status needs to be set
     * @param[in] processStatus Work completion status, low weight because there are no error details
     * @param[in] processStatus Internal check status, high weight due to error details
     */
    void StatusComparisonWithSetStatus(std::shared_ptr<CommWrapperBase> &comm, Status processStatus,
                                       Status checkErrorStatus);

    /**
     * @brief Get the mix hcclCommKey by eventType, localDeviceId, remoteClientId and remoteDeviceId.
     * @param[in] eventType The p2p event type: SEND or RECV
     * @param[in] localDeviceId The local client device id.
     * @param[in] remoteClientId The client id of the remote client that ready to create communicator.
     * @param[in] remoteDeviceId The device id of the remote client.
     * @return The mix key of eventType, localDeviceId, remoteClientId and remoteDeviceId.
     */
    static std::string GetHcclCommKey(P2PEventType eventType, int32_t localDeviceId, const std::string &remoteClientId,
                                      int32_t remoteDeviceId);

private:
    /**
     * @brief Handle communicator creation errors and set detailed error state.
     *
     * This function checks if the given status indicates an error, and if so,
     * sets the detailed HCCL communication state on the communicator object
     * before returning the error status. If no error is detected, it returns OK status.
     *
     * @param[in] comm Pointer to the Communicator object to set error state on
     * @param[in] status The status to check for errors
     * @return Returns the original error status if status.IsError() is true,
     *         otherwise returns Status::OK()
     */
    Status SetStateIfError(std::shared_ptr<CommWrapperBase> &comm, Status status);

    /**
     * @brief Asynchronously retry an operation with timeout and error handling.
     * @param[in] comm The HCCL communicator wrapper shared pointer.
     * @param[in] processFunc The main processing function to be executed and retried.
     * @param[in] errorCheckFunc Function to check for errors in the communicator state before retrying.
     * @param[in] timeoutMs Maximum timeout in milliseconds for the entire retry operation.
     * @param[in] retryableErrors List of error status codes that should trigger a retry.
     * @param[in] finalHandler Callback function to handle final result (success, timeout, or fatal error).
     * @note This function will retry the processFunc for retryable errors until success or timeout.
     *       Non-retryable errors or timeout will trigger the finalHandler with appropriate status.
     */
    void AsyncRetryWithTimeout(std::shared_ptr<CommWrapperBase> comm, std::function<Status()> processFunc,
                               std::function<Status(std::shared_ptr<CommWrapperBase>)> errorCheckFunc,
                               int32_t timeoutMs, const std::vector<StatusCode> retryableErrors,
                               std::function<void(std::shared_ptr<CommWrapperBase>, Status, Status)> finalHandler);

    /**
     * Generates a formatted sub-communication ID string for the given communicator.
     *
     * Extracts the communicator ID from the provided CommWrapperBase object and formats it
     * by truncating to the specified length (7 characters) if necessary, then enclosing it
     * in square brackets.
     *
     * @param comm Shared pointer to the communicator wrapper object
     * @return Formatted sub-communication ID string in the format "[XXXXXXX]" where
     *         XXXXXXX represents the first 7 characters of the original comm ID if
     *         its length is 7 or more, or the full comm ID if shorter than 7 characters.
     */
    std::string GetSubCommIdForIdentifier(std::shared_ptr<CommWrapperBase> &comm);

    TbbHcclCommTable commTable_;
    // To prevent two threads from trying to create a communication domain at the same time.
    std::shared_timed_mutex mutex_;
    std::shared_ptr<HcclCommMagr> hcclThreadControl_;
    AclResourceManager *aclResourceMgr_;
};
}  // namespace datasystem

#endif
