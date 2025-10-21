/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Client state.
 */
#ifndef DATASYSTEM_CLIENT_CLIENT_STATE_H
#define DATASYSTEM_CLIENT_CLIENT_STATE_H

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>
#include <thread>
#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/utils/status.h"

namespace datasystem {
// The client may be in a superimposed state, as shown below:
// INITIALIZED + INTERMEDIATE: means initting.
// EXITED + INTERMEDIATE: means exitting.
// INITIALIZED + FAILED: means init failed.
// EXITED + FAILED: means exit failed.
enum class ClientState : uint16_t {
    UNINITIALIZED = 0,  // The client has just been created and is not initialized.
    INITIALIZED = 1u,  // The client has been initted success.
    EXITED = 1u << 1,  // The client has been shutted down success.
    INTERMEDIATE = 1u << 2,  // It means that init/shutdown is being executed and has not yet been completed.
    FAILED = 1u << 3  // It means that init/shutdown has been executed but failed.
};

static constexpr uint16_t INITTING = (uint16_t)ClientState::INITIALIZED | (uint16_t)ClientState::INTERMEDIATE;
static constexpr uint16_t EXITTING = (uint16_t)ClientState::EXITED | (uint16_t)ClientState::INTERMEDIATE;
static constexpr uint16_t INIT_FAILED = (uint16_t)ClientState::INITIALIZED | (uint16_t)ClientState::FAILED;
static constexpr uint16_t EXIT_FAILED = (uint16_t)ClientState::EXITED | (uint16_t)ClientState::FAILED;

class ClientStateManager {
public:
    ClientStateManager() = default;

    ~ClientStateManager() = default;

    /**
     * @brief Process init.
     * @param[out] needRollbackState If the client status is successfully changed to INTERMEDIATE,
     * the status needs to be rolled back based on the completion status when the request is completed.
     * @return Status of the call.
     */
    Status ProcessInit(bool &needRollbackState)
    {
        Timer timer;
        int maxWaitTimeSecond = 60;
        int intervalMs = 100;
        uint16_t expected = (uint16_t)ClientState::UNINITIALIZED;
        while (!clientState_.compare_exchange_weak(expected, INITTING)) {
            // The intermediate state does not allow active state changes.
            if (expected & (uint16_t)ClientState::INTERMEDIATE) {
                if (timer.ElapsedSecond() > maxWaitTimeSecond) {
                    needRollbackState = false;
                    LOG(WARNING) << "The client state cannot be changed for a long time, "
                                    "client state: " << clientState_;
                    return Status(StatusCode::K_RUNTIME_ERROR, "The client state cannot be changed for a long time");
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
                expected = (uint16_t)ClientState::EXITED;
                continue;
            }
            if (expected == INIT_FAILED) {
                continue;
            }
            if (expected == (uint16_t)ClientState::INITIALIZED) {
                needRollbackState = false;
                LOG(WARNING) << "Init has been processed, client state: " << clientState_;
                return Status::OK();
            }
            if (expected == EXIT_FAILED) {
                needRollbackState = false;
                LOG(WARNING) << "Shutdown failed and needs to be re-closed successfully before initting."
                                "client state: " << clientState_;
                return Status(StatusCode::K_RUNTIME_ERROR, "Exit failed, try shutdown again.");
            }
            if (expected == (uint16_t)ClientState::EXITED) {
                continue;
            }
        }
        needRollbackState = true;
        return Status::OK();
    };

    /**
     * @brief Process shutdown.
     * @param[out] needRollbackState If the client status is successfully changed to INTERMEDIATE,
     * the status needs to be rolled back based on the completion status when the request is completed.
     * @param[in] isDestruct Since shutdown will also be called during client's destruction,
     * this parameter is used to avoid redundant log printing in the destruction scenario.
     * @return Status of the call.
     */
    Status ProcessShutdown(bool &needRollbackState, bool isDestruct = false)
    {
        Timer timer;
        int maxWaitTimeSecond = 60;
        int intervalMs = 100;
        uint16_t expected = (uint16_t)ClientState::INITIALIZED;
        while (!clientState_.compare_exchange_weak(expected, EXITTING)) {
            // The intermediate state does not allow active state changes.
            if (expected & (uint16_t)ClientState::INTERMEDIATE) {
                if (timer.ElapsedSecond() > maxWaitTimeSecond) {
                    needRollbackState = false;
                    LOG(WARNING) << "The client state cannot be changed for a long time, "
                                    "client state: " << clientState_;
                    return Status(StatusCode::K_RUNTIME_ERROR, "The client state cannot be changed for a long time");
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
                expected = (uint16_t)ClientState::INITIALIZED;
                continue;
            }
            if (expected == INIT_FAILED) {
                needRollbackState = false;
                LOG_IF(WARNING, !isDestruct) << "Initialization failed and needs to be re-initialized successfully"
                                                " before closing. client state: " << clientState_;
                return Status(StatusCode::K_RUNTIME_ERROR, "Init failed, try init.");
            }
            if (expected == (uint16_t)ClientState::INITIALIZED) {
                continue;
            }
            if (expected == EXIT_FAILED) {
                continue;
            }
            if (expected == (uint16_t)ClientState::EXITED) {
                needRollbackState = false;
                LOG_IF(WARNING, !isDestruct) << "Shutdown has been processed, client state: " << clientState_;
                return Status::OK();
            }
            if (expected == (uint16_t)ClientState::UNINITIALIZED) {
                needRollbackState = false;
                LOG_IF(WARNING, !isDestruct) << "Client has not been initialized yet, please initialize it first."
                                                "client state: " << clientState_;
                return Status(StatusCode::K_RUNTIME_ERROR, "Client has not been initialized yet, try init.");
            }
        }
        needRollbackState = true;
        return Status::OK();
    };

    /**
     * @brief Process destruct.
     * @param[in] shutdown Shutdown func.
     */
    void ProcessDestruct(const std::function<Status()> &shutdown)
    {
        (void)shutdown();
    }

    /**
     * @brief Get client state.
     * @return Client state.
     */
    uint16_t GetState()
    {
        return clientState_;
    }

    /**
     * @brief Init/Shutdown complete handler.
     * @param[in] failed Init/Shutdown success or not.
     * @param[out] needRollbackState If the client status is successfully changed to INTERMEDIATE,
     * the status needs to be rolled back based on the completion status when the request is completed.
     */
    void CompleteHandler(bool failed, bool needRollbackState)
    {
        if (!needRollbackState) {
            return;
        }
        if (failed) {
            MarkFailed();
        } else {
            MarkSuccess();
        }
    }

    /**
     * @brief Get client state msg for user.
     * @param[in] state client state.
     * @return Client state msg.
     */
    std::string ToStringForUser(uint16_t state)
    {
        std::string msg;
        switch (state) {
            case (uint16_t)ClientState::INITIALIZED:
                msg = "Init success.";
                break;
            case INITTING:
                msg = "The client is initting, wait and try again.";
                break;
            case INIT_FAILED:
                msg = "The client initialization failed and needs to be reinitialized.";
                break;
            case (uint16_t)ClientState::EXITED:
                msg = "The client has been exitted and needs to be reinitialized.";
                break;
            case EXITTING:
                msg = "The client is exiting and needs to wait for the exit to complete before reinitializing.";
                break;
            case EXIT_FAILED:
                msg = "The client failed to exit and needs to exit again.";
                break;
            case (uint16_t)ClientState::UNINITIALIZED:
                msg = "The client has not been initialized yet";
                break;
            default:
                msg = "unreasonable state: " + std::to_string(state);
        }
        return msg;
    }

private:
    /**
     * @brief Mark success.
     */
    void MarkSuccess()
    {
        clientState_.fetch_xor((uint16_t)ClientState::INTERMEDIATE);
    }

    /**
     * @brief Mark failed.
     */
    void MarkFailed()
    {
        clientState_.fetch_or((uint16_t)ClientState::FAILED);
        clientState_.fetch_xor((uint16_t)ClientState::INTERMEDIATE);
    }

    std::atomic<uint16_t> clientState_{ (uint16_t)ClientState::UNINITIALIZED };
};
}  // namespace datasystem
#endif  // DATASYSTEM_CLIENT_CLIENT_STATE_H