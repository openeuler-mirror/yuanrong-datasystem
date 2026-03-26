/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Defines the ascend device manager.
 */

#ifndef DATASYSTEM_COMMON_DEVICE_ACL_PIPELINE_TASK_H
#define DATASYSTEM_COMMON_DEVICE_ACL_PIPELINE_TASK_H

#include <condition_variable>
#include <mutex>

#include "datasystem/common/device/ascend/acl_device_manager.h"
#include "datasystem/common/device/ascend/callback_thread.h"
#include "datasystem/common/device/ascend/ffts_dispatcher.h"
#include "datasystem/common/device/device_manager_factory.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/inject/inject_point.h"

#define CHECK_ACL_RESULT(aclRet, apiName)                                                             \
    do {                                                                                              \
        int _aclRet = (aclRet);                                                                       \
        if (_aclRet != 0) {                                                                           \
            std::string errMsg = FormatString("%s api failed with error code %d ", apiName, _aclRet); \
            return Status(StatusCode::K_ACL_ERROR, __LINE__, __FILE__, errMsg);                       \
        }                                                                                             \
    } while (false)

namespace datasystem {
namespace acl {
const size_t TWO_PHASE_TASK_PIPELINE = 2;
const size_t TWO_PHASE_TASK_PHASE_COUNT = 2;

template <size_t TASK_PIPELINE, size_t TASK_PHASE_COUNT>
class AclPipelineResource {
public:
    AclPipelineResource()
    {
        Reset();
    }

    ~AclPipelineResource()
    {
        Release();
    }

    AclPipelineResource(const AclPipelineResource &) = delete;
    AclPipelineResource &operator=(const AclPipelineResource &) = delete;
    Status Init(uint32_t devId)
    {
        deviceImpl_ = DeviceManagerFactory::GetDeviceManager();
        deviceId = devId;

        bool skipFfts = DeviceManagerFactory::ProbeBackend() != DeviceBackend::NPU;

        INJECT_POINT("NO_USE_FFTS", [&skipFfts]() {
            skipFfts = true;
            return Status::OK();
        });

        // Create streams using abstract device interface (works for both NPU and GPU)
        for (size_t taskIndex = 0; taskIndex < TASK_PHASE_COUNT; taskIndex++) {
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(deviceImpl_->CreateStream(&stream[taskIndex]), "CreateStream failed");
        }

        if (skipFfts) {
            return Status::OK();
        }

        // NPU-specific FFTS initialization (not available on GPU)
        auto aclApi = acl::AclDeviceManager::Instance();
        for (size_t taskIndex = 0; taskIndex < TASK_PHASE_COUNT; taskIndex++) {
            for (size_t pipelineIndex = 0; pipelineIndex < TASK_PIPELINE; pipelineIndex++) {
                RETURN_IF_NOT_OK_PRINT_ERROR_MSG(aclApi->RtNotifyCreate(deviceId, &notifier[taskIndex][pipelineIndex]),
                                                 "RtNotifyCreate failed");
            }
        }
        fftsDispatcher = std::make_unique<ffts::FftsDispatcher>(deviceId, aclApi);

        CHECK_ACL_RESULT(fftsDispatcher->Init(), "FftsDispatcher init");
        CHECK_ACL_RESULT(fftsDispatcher->CreateFftsCtxs(1), "FftsDispatcher CreateFftsCtxs");
        CHECK_ACL_RESULT(fftsDispatcher->SetFftsCtx(0), "FftsDispatcher SetFftsCtx");
        callbackThread = std::make_unique<CallbackThread>();
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(callbackThread->SubscribeStream(PrimaryStream()), "SubscribeStream failed");
        return Status::OK();
    }
    void Reset()
    {
        deviceId = 0;
        submitTaskCount = 0;
        callbackThread = nullptr;
        for (size_t taskIndex = 0; taskIndex < TASK_PHASE_COUNT; taskIndex++) {
            stream[taskIndex] = nullptr;
            for (size_t pipelineIndex = 0; pipelineIndex < TASK_PIPELINE; pipelineIndex++) {
                notifier[taskIndex][pipelineIndex] = nullptr;
            }
        }
    }
    void Release()
    {
        // GPU mode or NO_USE_FFTS injection: only destroy streams, skip FFTS/Notify cleanup
        bool skipFfts = DeviceManagerFactory::ProbeBackend() != DeviceBackend::NPU;

        INJECT_POINT("NO_USE_FFTS", [&skipFfts]() {
            skipFfts = true;
        });

        if (skipFfts) {
            for (size_t taskIndex = 0; taskIndex < TASK_PHASE_COUNT; taskIndex++) {
                if (stream[taskIndex] != nullptr) {
                    LOG_IF_ERROR(deviceImpl_->DestroyStream(stream[taskIndex]), "DestroyStream failed");
                }
            }
            return;
        }

        // NPU-specific FFTS cleanup (not available on GPU)
        auto aclApi = acl::AclDeviceManager::Instance();
        LOG(INFO) << "Release AclPipelineResource";
        if (callbackThread) {
            LOG_IF_ERROR(callbackThread->UnSubscribeStream(PrimaryStream()), "UnSubscribeStream failed");
        }
        for (size_t taskIndex = 0; taskIndex < TASK_PHASE_COUNT; taskIndex++) {
            for (size_t pipelineIndex = 0; pipelineIndex < TASK_PIPELINE; pipelineIndex++) {
                if (notifier[taskIndex][pipelineIndex] != nullptr) {
                    LOG_IF_ERROR(aclApi->RtNotifyDestroy(notifier[taskIndex][pipelineIndex]), "RtNotifyDestroy failed");
                }
            }
            if (stream[taskIndex] != nullptr) {
                LOG_IF_ERROR(aclApi->RtDestroyStream(stream[taskIndex]), "RtDestroyStream failed");
            }
        }
        Reset();
    }

    aclrtStream PrimaryStream()
    {
        return stream[TASK_PHASE_COUNT - 1];
    }

    Status NotifyStart()
    {
        // GPU mode or NO_USE_FFTS injection: skip RtNotifyRecord (NPU-specific API)
        bool skipFfts = DeviceManagerFactory::ProbeBackend() != DeviceBackend::NPU;
        INJECT_POINT("NO_USE_FFTS", [&skipFfts]() {
            skipFfts = true;
            return Status::OK();
        });
        if (skipFfts) {
            return Status::OK();
        }

        // NPU-specific: record notify signals for FFTS pipeline synchronization
        auto aclApi = acl::AclDeviceManager::Instance();
        for (size_t i = 0; i < TASK_PIPELINE; i++) {
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(aclApi->RtNotifyRecord(notifier[TASK_PHASE_COUNT - 1][i], stream[0]),
                                             "RtNotifyRecord failed.");
        }
        return Status::OK();
    }

    uint32_t deviceId;
    DeviceManagerBase *deviceImpl_{ nullptr };  // Abstract device manager for stream operations
    aclrtStream stream[TASK_PHASE_COUNT];
    rtNotify_t notifier[TASK_PHASE_COUNT][TASK_PIPELINE];
    std::unique_ptr<ffts::FftsDispatcher> fftsDispatcher;
    std::unique_ptr<CallbackThread> callbackThread;
    uint32_t submitTaskCount;
};

template <typename Derived, typename Task, size_t TASK_PIPELINE, size_t TASK_PHASE_COUNT>
class AclPipeLineBase {
public:
    AclPipeLineBase() = default;
    ~AclPipeLineBase() = default;

    Status Init(std::shared_ptr<AclPipelineResource<TASK_PIPELINE, TASK_PHASE_COUNT>> resource)
    {
        RETURN_RUNTIME_ERROR_IF_NULL(resource);
        deviceImpl_ = DeviceManagerFactory::GetDeviceManager();
        // Keep aclApi_ for NPU-specific FFTS operations (RtNotifyWait/RtNotifyRecord)
        aclApi_ = acl::AclDeviceManager::Instance();
        resource_ = resource;
        return Status::OK();
    }

    Status Add(Task &&task)
    {
        std::unique_lock<std::mutex> locker(mutex_);
        tasks_.emplace_back(std::move(task));
        taskCount_++;
        cv_.notify_all();
        return Status::OK();
    }

    Status Submit(Task &&task)
    {
        Add(std::move(task));
        return SubmitToStream();
    }

    Status WaitSubmit(size_t expectTaskCount)
    {
        RETURN_IF_NOT_OK(PreProcess());
        while (true) {
            if (WaitTask(expectTaskCount)) {
                break;
            }
            RETURN_IF_NOT_OK(SubmitToStream());
        }
        return PostProcess();
    }

    aclrtStream PrimaryStream()
    {
        return resource_->PrimaryStream();
    }

    std::shared_ptr<AclPipelineResource<TASK_PIPELINE, TASK_PHASE_COUNT>> GetResource()
    {
        return resource_;
    }

protected:
    Status PreProcess()
    {
        return static_cast<Derived *>(this)->PreProcessImpl();
    }

    Status PostProcess()
    {
        submitTasks_.clear();
        return static_cast<Derived *>(this)->PostProcessImpl();
    }

    Status RunTask(size_t pipelineIndex, size_t taskPhaseId, const Task &task, aclrtStream stream)
    {
        return static_cast<Derived *>(this)->RunTaskImpl(pipelineIndex, taskPhaseId, task, stream);
    }

    Status PostTaskProcess(const Task &task)
    {
        return static_cast<Derived *>(this)->PostTaskProcessImpl(task);
    }

    Status SubmitToStream()
    {
        {
            std::unique_lock<std::mutex> locker(mutex_);
            std::swap(tasks_, submitTasks_);
        }

        for (size_t i = 0; i < submitTasks_.size(); i++) {
            size_t pipelineIndex = resource_->submitTaskCount % TASK_PIPELINE;
            resource_->submitTaskCount++;

            for (size_t taskPhaseId = 0; taskPhaseId < TASK_PHASE_COUNT; taskPhaseId++) {
                auto prePhaseId = taskPhaseId > 0 ? taskPhaseId - 1 : TASK_PHASE_COUNT - 1;
                auto stream = resource_->stream[taskPhaseId];
                auto waitFor = resource_->notifier[prePhaseId][pipelineIndex];
                auto recordTo = resource_->notifier[taskPhaseId][pipelineIndex];
                RETURN_IF_NOT_OK(aclApi_->RtNotifyWait(waitFor, stream));
                RETURN_IF_NOT_OK(RunTask(pipelineIndex, taskPhaseId, submitTasks_[i], stream));
                RETURN_IF_NOT_OK(aclApi_->RtNotifyRecord(recordTo, stream));
            }
            // post process
            RETURN_IF_NOT_OK(PostTaskProcess(submitTasks_[i]));
        }
        submitTasks_.clear();
        return Status::OK();
    }

    bool WaitTask(size_t expectTaskCount)
    {
        std::unique_lock<std::mutex> locker(mutex_);
        cv_.wait(locker, [this, expectTaskCount] { return !tasks_.empty() || taskCount_ >= expectTaskCount; });
        if (taskCount_ >= expectTaskCount && tasks_.empty()) {
            return true;
        }
        return false;
    }

    friend Derived;
    uint32_t deviceId_;
    DeviceManagerBase *deviceImpl_;  // Abstract device manager for stream operations
    acl::AclDeviceManager *aclApi_;  // NPU-specific API for FFTS operations
    std::shared_ptr<AclPipelineResource<TASK_PIPELINE, TASK_PHASE_COUNT>> resource_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<Task> tasks_;
    std::vector<Task> submitTasks_;
    size_t taskCount_{ 0 };
};

using TwoPhaseAclPipeLineResource = AclPipelineResource<TWO_PHASE_TASK_PIPELINE, TWO_PHASE_TASK_PHASE_COUNT>;

template <typename Derived, typename Task>
class TwoPhaseAclPipeLineBase : public AclPipeLineBase<TwoPhaseAclPipeLineBase<Derived, Task>, Task,
                                                       TWO_PHASE_TASK_PIPELINE, TWO_PHASE_TASK_PHASE_COUNT> {
public:
    enum class TwoPhaseTaskId : size_t { TASK_PHASE_ONE = 0, TASK_PHASE_TWO };

protected:
    friend class AclPipeLineBase<TwoPhaseAclPipeLineBase<Derived, Task>, Task, TWO_PHASE_TASK_PIPELINE,
                                 TWO_PHASE_TASK_PHASE_COUNT>;
    Status PreProcessImpl()
    {
        return static_cast<Derived *>(this)->PreProcessImpl();
    }

    Status PostProcessImpl()
    {
        return static_cast<Derived *>(this)->PostProcessImpl();
    }

    bool WaitTaskImpl(size_t expectTaskCount)
    {
        return static_cast<Derived *>(this)->WaitTaskImpl(expectTaskCount);
    }

    Status RunTaskImpl(size_t pipelineIndex, size_t taskPhaseId, const Task &task, aclrtStream stream)
    {
        switch (static_cast<TwoPhaseTaskId>(taskPhaseId)) {
            case TwoPhaseTaskId::TASK_PHASE_ONE:
                RETURN_IF_NOT_OK(RunTaskPhaseOne(pipelineIndex, task, stream));
                break;
            case TwoPhaseTaskId::TASK_PHASE_TWO:
                RETURN_IF_NOT_OK(RunTaskPhaseTwo(pipelineIndex, task, stream));
                break;
            default:
                RETURN_STATUS(K_RUNTIME_ERROR, FormatString("Task Phase %d not implement", taskPhaseId));
        }
        return Status::OK();
    }

    Status PostTaskProcessImpl(const Task &task)
    {
        return static_cast<Derived *>(this)->PostTaskProcessImpl(task);
    }

    Status RunTaskPhaseOne(size_t pipelineIndex, const Task &task, aclrtStream stream)
    {
        return static_cast<Derived *>(this)->RunTaskPhaseOneImpl(pipelineIndex, task, stream);
    }

    Status RunTaskPhaseTwo(size_t pipelineIndex, const Task &task, aclrtStream stream)
    {
        return static_cast<Derived *>(this)->RunTaskPhaseTwoImpl(pipelineIndex, task, stream);
    }
};
}  // namespace acl
}  // namespace datasystem
#endif
