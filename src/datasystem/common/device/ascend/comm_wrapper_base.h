/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
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

#ifndef DATASYSTEM_COMMON_DEVICE_COMM_WRAPPER_BASE_H
#define DATASYSTEM_COMMON_DEVICE_COMM_WRAPPER_BASE_H

#include "datasystem/client/hetero_cache/device_util.h"
#include "datasystem/client/object_cache/device/hccl_comm_magr.h"
#include "datasystem/common/device/ascend/acl_pipeline_p2p_task.h"
#include "datasystem/common/device/ascend/acl_pipeline_task.h"
#include "datasystem/common/device/ascend/acl_pointer_wrapper.h"
#include "datasystem/common/device/ascend/cann_types.h"
#include "datasystem/common/device/ascend/p2phccl_types.h"
#include "datasystem/common/device/device_manager_base.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/thread_pool.h"

namespace datasystem {
// This constant is used to configure the waiting time for destroying Steam when the RunP2PSendLoop and RunP2PRecvLoop
// threads exit, preventing long-time blocking caused by unfinished tasks in Stream.
const int32_t SYNC_STREAM_WAIT_TIMEOUT_MS = 10000;
enum class HcclCommState { UNCREATE, CREATING, VALID, INVALID, DESTROY };
enum class HcclCommDirection { SEND, RECV };
constexpr int WARM_UP_DATA_COUNT = 1;

class CommWrapperBase : public AclPointerWrapper {
public:
    explicit CommWrapperBase(const std::string &commId, int localDeviceId, int remoteDeviceId,
                             std::shared_ptr<HcclCommMagr> &threadControl, AclResourceManager *aclResourceMgr);

    ~CommWrapperBase();

    template <class F, class... Args>
    void Execute(F &&f, Args &&...args)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (hasShutDown_) {
            return;
        }
        pool_->Execute(std::forward<F>(f), std::forward<Args>(args)...);
    }

    /**
     * @brief Checks if the communicator is ready for collective operations.
     * @return true if communicator is initialized and ready, false otherwise.
     */
    bool IsCommReady() const;

    /**
     * @brief Sets the communicator ready state and triggers ready callbacks when becoming ready.
     * @param ready The new ready state to set.
     * @note If transitioning from not-ready to ready state, all registered ready callbacks will be executed.
     */
    void SetCommReady(bool ready);

    /**
     * @brief Executes all registered ready callbacks in a thread-safe manner.
     * @note Callbacks are moved to a local vector to minimize lock holding time.
     *       This ensures callbacks execute without holding the mutex.
     */
    void ExecuteReadyCallbacks();

    /**
     * @brief Adds a callback to be executed when communicator becomes ready.
     * @param callback The callback function to register.
     * @note If communicator is already ready, the callback is executed immediately.
     *       Otherwise, it's queued for execution when SetCommReady(true) is called.
     */
    void AddReadyCallback(std::function<void()> callback);

    /**
     * @brief Get AclrtStream
     * @return The AclrtStream
     */
    aclrtStream GetStream();

    /**
     * @brief Set the status of hcclcomm.
     * @param[in] commStatus The status of hcclcomm.
     */
    void SetStatus(const Status &commStatus);

    /**
     * @brief Get the status of hcclcomm.
     * @return The status of hcclcomm.
     */
    Status GetDetailStatus() const;

    /**
     * @brief Get the lifetime state of hcclcomm.
     * @return The lifetime state of hcclcomm.
     */
    HcclCommState GetCommStatus() const;

    /**
     * @brief Get the local device id.
     * @return The local device id.
     */
    int GetLocalDeviceId() const;

    /**
     * @brief Get the remote device id.
     * @return The remote device id.
     */
    int GetRemoteDeviceId() const;

    /**
     * @brief Sets the specific fault cause.
     * @param[in] result The status of Hccl invocation
     */
    void SetHcclDetailState(Status result);

    /**
     * @brief Check HcclComm health.
     * @param[in] createTimeoutMs The timeout of create HcclComm.
     */
    Status CheckHealth(uint32_t createTimeoutMs);

    /**
     * @brief Returns the ID of the current comm.
     * return ID of the current comm.
     */
    std::string GetCommId() const;

    /**
     * @brief Get the communicator init timestamp.
     * @return The communicator init timestamp.
     */
    std::chrono::steady_clock::time_point GetInitTimeStamp() const;

    /**
     * @brief P2P send the data to the receiving side.
     * @param[in] blobs[in] The list of the blob info.
     * @param[in] comm[in] The hccl communicator.
     * @param[in] stream[in] The stream of acl context.
     * @return Status of the call
     */
    virtual Status P2PSend(const std::vector<Blob> &blobs, const std::shared_ptr<AclRtEventWrapper> &event,
                           aclrtStream stream) = 0;

    /**
     * @brief P2P recv the data from the sending side.
     * @param[in] blobs The list of the blob info.
     * @param[in] comm The hccl communicator.
     * @param[in] stream The stream of acl context.
     * @return Status of the call
     */
    virtual Status P2PRecv(const std::vector<Blob> &blobs, const std::shared_ptr<AclRtEventWrapper> &event,
                           aclrtStream stream) = 0;

    /**
     * @brief Queries whether an error occurs in the communication domain.
     * @return The status of Hccl invocation
     */
    virtual Status HcclGetCommAsyncError() = 0;

    /**
     * @brief Init hccl communicator.
     * @param[in] rootInfo The root info.
     * @param[in] direction own transmission direction.
     * @return Status of the call.
     */
    virtual Status InitCommunicator(CommRootInfo &rootInfo, const HcclCommDirection direction, bool isSameNode) = 0;

    /**
     * @brief Warm up the hccl communicator wrapper in the send side.
     * Attention! The HCCL interface has limitations.
     * Suppose thread A creates communicator a1, but does not call the hccl send/recv interfaces.
     * Thread B also creates communicator b1 and calls the send/recv interface,
     * then communicator a1 will not work properly.
     * So we need to call send/recv immediately after creating the communicator to establish a socket,
     * and ensure that the communication domain can be used normally.
     * @param[in] eventType The p2p event type: SEND or RECV
     * @return The status of call.
     */
    virtual Status WarmUpComm(HcclCommDirection eventType) = 0;

    /**
     * @brief Creating hccl rootinfo.
     * @param[in] rootInfo Transfer a blank rootinfo, create a reference, and transfer a value.
     * @return Status of the call.
     */
    virtual Status CreateRootInfo(CommRootInfo &rootInfo) = 0;

    std::shared_ptr<acl::TwoPhaseAclPipeLineResource> GetP2PResource()
    {
        return resource_;
    }

    Status InitPipeline(HcclCommDirection direction);
    Status SubmitPipelineTask(acl::P2PSendTask task);
    Status SubmitPipelineTask(acl::P2PRecvTask task);

private:
    /**
     * @brief Check if the communication pointer is valid and return corresponding error status if null.
     * @param[in] pointer The communication pointer to be checked (sender_ or receiver_).
     * @param[in] pointerName The name of the pointer for error message identification.
     * @return Status::OK() if pointer is valid, otherwise returns error status with detailed message.
     * @note This function is used to validate HCCL communication pointers that should be initialized
     *       during communication domain creation. A null pointer typically indicates HCCL communication
     *       domain creation failure.
     */
    Status CheckTranPointer(const void *pointer, const std::string &pointerName);

    DeviceManagerBase *deviceImpl_;
    AclResourceManager *aclResourceMgr_;
    std::shared_ptr<acl::TwoPhaseAclPipeLineResource> resource_;
    std::unique_ptr<acl::PipeLineP2PSend> sender_;
    std::unique_ptr<acl::PipeLineP2PRecv> receiver_;
    std::string commId_;
    int localDeviceIdx_;
    int remoteDeviceIdx_;
    std::shared_ptr<ThreadPool> pool_;
    std::chrono::steady_clock::time_point commConnectTimestamp_;
    std::atomic<HcclCommState> hcclCommState_;
    Status hcclDetailState_;
    mutable std::mutex hcclDetailStateMutex_; // protect hcclDetailState_
    std::shared_ptr<HcclCommMagr> hcclThreadControl_;
    int bindThreadId_;
    std::mutex mutex_;
    bool hasShutDown_ = false;

    std::atomic<bool> commReady_{false};
    std::mutex callbackMutex_;  // Mutex ensuring callbacks are executed in the order they were added
    std::vector<std::function<void()>> readyCallbacks_;

    friend class HcclCommWrapper;
    friend class P2PHcclCommWrapper;
};

}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_DEVICE_COMM_WRAPPER_BASE_H
