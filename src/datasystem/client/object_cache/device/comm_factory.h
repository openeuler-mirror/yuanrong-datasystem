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

#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_DEVICE_COMM_FACTORY_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_DEVICE_COMM_FACTORY_H

#include <future>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include <tbb/concurrent_hash_map.h>

#include "datasystem/client/object_cache/client_worker_api/iclient_worker_api.h"
#include "datasystem/client/hetero_cache/device_util.h"
#include "datasystem/client/object_cache/device/hccl_comm_magr.h"
#include "datasystem/common/device/device_manager_base.h"
#include "datasystem/common/device/device_manager_factory.h"
#include "datasystem/common/device/comm_wrapper.h"

namespace datasystem {

enum class P2PEventType { SEND, RECV };

using TbbCommTable = tbb::concurrent_hash_map<std::string, std::shared_ptr<CommWrapperBase>>;

class ClientDeviceCurd {
public:
    ClientDeviceCurd(std::shared_ptr<object_cache::IClientWorkerApi> workerApi)
        : deviceImpl_(DeviceManagerFactory::GetDeviceManager()), clientWorkerApi_(std::move(workerApi))
    {
    }

    ClientDeviceCurd(const ClientDeviceCurd &) = delete;

    ClientDeviceCurd &operator=(const ClientDeviceCurd &) = delete;

    virtual void ShutDown() {};

    virtual ~ClientDeviceCurd() = default;

protected:
    DeviceManagerBase *deviceImpl_;
    std::shared_ptr<object_cache::IClientWorkerApi> clientWorkerApi_;
};

class CommFactory : public ClientDeviceCurd {
public:
    CommFactory(std::shared_ptr<object_cache::IClientWorkerApi> workerApi, DeviceResourceManager *resourceMgr);

    void ShutDown();

    ~CommFactory();

    /**
     * @brief Get the communicator wrapper, or create it if not existed.
     * @param[in] eventType The p2p event type: SEND or RECV
     * @param[in] localDeviceId The local client device id.
     * @param[in] remoteClientId The client id of the remote client that ready to create communicator.
     * @param[in] remoteDeviceId The device id of the remote client.
     * @param[out] comm The communicator wrapper.
     * @return The status of call.
     */
    Status GetOrCreateComm(P2PEventType eventType, int32_t localDeviceId, const std::string &remoteClientId,
                           int32_t remoteDeviceId, bool isSameNode, bool enableP2Ptransfer,
                           std::shared_ptr<CommWrapperBase> &comm);

    /**
     * @brief Create the communicator wrapper in the send side.
     * @param[in] localDeviceId The local client device id.
     * @param[in] remoteClientId The client id of the remote client that ready to create communicator.
     * @param[in] remoteDeviceId The device id of the remote client.
     * @param[out] comm The communicator wrapper.
     */
    void CreateCommInSend(int32_t localDeviceId, const std::string &remoteClientId, int32_t remoteDeviceId,
                          bool isSameNode, std::shared_ptr<CommWrapperBase> &comm);

    /**
     * @brief Create the communicator wrapper in the recv side.
     * Attention! It should be thread safe to avoid communication interface coredump.
     * @param[in] localDeviceId The local client device id.
     * @param[in] remoteClientId The client id of the remote client that ready to create communicator.
     * @param[in] remoteDeviceId The device id of the remote client.
     * @param[out] comm The communicator wrapper.
     */
    void CreateCommInRecv(int32_t localDeviceId, const std::string &remoteClientId, int32_t remoteDeviceId,
                          bool isSameNode, std::shared_ptr<CommWrapperBase> &comm);

    Status CreateCommCheckError(std::shared_ptr<CommWrapperBase> &comm);

    /**
     * @brief Get all communicators.
     * @return The list of communicator wrappers.
     */
    std::vector<std::shared_ptr<CommWrapperBase>> GetAllComm();

    /**
     * @brief Create the number of communicator wrapper in table.
     * @return The number of communicators
     */
    size_t GetCommSize();

    /**
     * @brief Delete comm from commtable_ of factory for operation before comm destructor.
     * @param[in] commId The std::shared_ptr<CommWrapper> id, can be find in comm->GetCommId()
     * @return Indicates status whether the deletion is successful.
     */
    Status DelComm(std::string commId);

    /**
     * @brief Destroy the communicator with commId.
     * @param[in] commId The std::shared_ptr<CommWrapper> id, can be find in comm->GetCommId()
     */
    void DestroyComm(std::string commId);

    /**
     * @brief Used for status comparison. What status should be set for comm
     * @param[in] comm whose status needs to be set
     * @param[in] processStatus Work completion status, low weight because there are no error details
     * @param[in] processStatus Internal check status, high weight due to error details
     */
    void StatusComparisonWithSetStatus(std::shared_ptr<CommWrapperBase> &comm, Status processStatus,
                                       Status checkErrorStatus);

    /**
     * @brief Get the mix CommKey by eventType, localDeviceId, remoteClientId and remoteDeviceId.
     * @param[in] eventType The p2p event type: SEND or RECV
     * @param[in] localDeviceId The local client device id.
     * @param[in] remoteClientId The client id of the remote client that ready to create communicator.
     * @param[in] remoteDeviceId The device id of the remote client.
     * @return The mix key of eventType, localDeviceId, remoteClientId and remoteDeviceId.
     */
    static std::string GetCommKey(P2PEventType eventType, int32_t localDeviceId, const std::string &remoteClientId,
                                  int32_t remoteDeviceId);

private:
    /**
     * @brief Handle communicator creation errors and set detailed error state.
     *
     * This function checks if the given status indicates an error, and if so,
     * sets the detailed communication state on the communicator object
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
     * @param[in] comm The communicator wrapper shared pointer.
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

    /**
     * @brief Process the send side comm creation logic.
     * @param[in] comm The comm wrapper shared pointer.
     * @param[in] localDeviceId Local device ID.
     * @param[in] remoteDeviceId Remote device ID.
     * @param[in] remoteClientId Remote client ID.
     * @param[in] isSameNode Whether on same node.
     * @param[in] traceId Trace ID for logging.
     * @return Status.
     */
    Status ProcessCommCreationInSend(std::shared_ptr<CommWrapperBase> comm, int32_t localDeviceId,
                                     int32_t remoteDeviceId, const std::string &remoteClientId, bool isSameNode,
                                     const std::string &traceId);

    TbbCommTable commTable_;
    // To prevent two threads from trying to create a communication domain at the same time.
    std::shared_timed_mutex mutex_;
    std::shared_ptr<HcclCommMagr> commThreadControl_;
    DeviceResourceManager *resourceMgr_;
};
}  // namespace datasystem

#endif
