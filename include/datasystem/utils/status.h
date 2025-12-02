/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: base batch executor define
 */
#ifndef DATASYSTEM_UTILS_STATUS_H
#define DATASYSTEM_UTILS_STATUS_H

#include <memory>
#include <string>
#include <utility>

namespace datasystem {
enum StatusCode : uint32_t {
    // common error code, range: [0, 1000)
    K_OK = 0,
    K_DUPLICATED = 1,
    K_INVALID = 2,
    K_NOT_FOUND = 3,
    K_KVSTORE_ERROR = 4,
    K_RUNTIME_ERROR = 5,
    K_OUT_OF_MEMORY = 6,
    K_IO_ERROR = 7,
    K_NOT_READY = 8,
    K_NOT_AUTHORIZED = 9,
    K_UNKNOWN_ERROR = 10,
    K_INTERRUPTED = 11,
    K_OUT_OF_RANGE = 12,
    K_NO_SPACE = 13,
    K_NOT_LEADER_MASTER = 14,
    K_RECOVERY_ERROR = 15,
    K_RECOVERY_IN_PROGRESS = 16,
    K_FILE_NAME_TOO_LONG = 17,
    K_FILE_LIMIT_REACHED = 18,
    K_TRY_AGAIN = 19,
    K_DATA_INCONSISTENCY = 20,
    K_SHUTTING_DOWN = 21,
    K_WORKER_ABNORMAL = 22,
    K_CLIENT_WORKER_DISCONNECT = 23,
    K_WORKER_DEADLOCK = 24,
    K_MASTER_TIMEOUT = 25,
    K_NOT_FOUND_IN_L2CACHE = 26,
    K_REPLICA_NOT_READY = 27,
    K_CLIENT_WORKER_VERSION_MISMATCH = 28,
    K_SERVER_FD_CLOSED = 29,
    K_RETRY_IF_LEAVING = 30,
    K_SCALE_DOWN = 31,
    K_SCALING = 32,
    K_CLIENT_DEADLOCK = 33,
    K_LRU_HARD_LIMIT = 34,
    K_LRU_SOFT_LIMIT = 35,

    // rpc error code, range: [1000, 2000)
    K_RPC_CANCELLED = 1000,
    K_RPC_DEADLINE_EXCEEDED = 1001,
    K_RPC_UNAVAILABLE = 1002,
    K_RPC_STREAM_END = 1003,
    K_URMA_ERROR = 1004,
    K_RDMA_ERROR = 1005,

    // object error code, range: [2000, 3000)
    K_OC_ALREADY_SEALED = 2000,
    K_OC_OBJECT_NOT_IN_USED = 2001,
    K_OC_REMOTE_GET_NOT_ENOUGH = 2002,
    K_WRITE_BACK_QUEUE_FULL = 2003,
    K_OC_KEY_ALREADY_EXIST = 2004,
    K_WORKER_PULL_OBJECT_NOT_FOUND = 2005,

    // stream error code, range: [3000, 4000)
    K_SC_STREAM_NOT_FOUND = 3000,
    K_SC_PRODUCER_NOT_FOUND = 3001,
    K_SC_CONSUMER_NOT_FOUND = 3002,
    K_SC_END_OF_PAGE = 3003,
    K_SC_STREAM_IN_RESET_STATE = 3004,
    K_SC_WORKER_WAS_LOST = 3005,
    K_SC_STREAM_IN_USE = 3006,
    K_SC_STREAM_DELETE_IN_PROGRESS = 3007,
    K_SC_STREAM_RESOURCE_ERROR = 3008,
    K_SC_ALREADY_CLOSED = 3009,
    K_SC_STREAM_NOTIFICATION_PENDING = 3010,

    // Heterogeneous error code, range: [5000, 6000]
    K_ACL_ERROR = 5000,
    K_HCCL_ERROR = 5001,
    K_FUTURE_TIMEOUT = 5002
};

/**
 * @brief Get the StatusCode by name
 * @param name The name of status code.
 * @return StatusCode
 */
StatusCode GetStatusCodeByName(const std::string &name);

class Status {
public:
    Status() noexcept;

    Status(const Status &other) noexcept;

    Status(Status &&other) noexcept;

    Status &operator=(const Status &other) noexcept;

    Status &operator=(Status &&other) noexcept;

    ~Status() noexcept = default;

    /**
     * @brief Set code and message to Status.
     * @param[in] code Return code.
     * @param[in] msg Return msg.
     */
    Status(StatusCode code, std::string msg) noexcept;

    /**
     * @brief Set return info of Status.
     * @param[in] code Return code.
     * @param[in] lineOfCode Line of code.
     * @param[in] fileName File name.
     * @param[in] extra Extra message for return msg.
     */
    Status(StatusCode code, int lineOfCode, const std::string &fileName, const std::string &extra = "");

    /**
     * @brief Return K_OK means program running fine.
     * @return K_OK of Status;
     */
    static Status OK()
    {
        return Status();
    }

    /**
     * @brief Concatenate code and msg
     */
    std::string ToString() const;

    /**
     * @brief Get status code.
     * @return the program running status code;
     */
    StatusCode GetCode() const;

    /**
     * @brief Get status message.
     * @return the program running message;
     */
    std::string GetMsg() const;

    /**
     * @brief Append extra info to status message.
     */
    void AppendMsg(const std::string &appendMsg);

    friend std::ostream &operator<<(std::ostream &os, const Status &s);

    /**
     * @brief Verify whether the code is K_OK.
     * @return true when code is K_OK, otherwise return false;
     */
    explicit operator bool() const
    {
        return (GetCode() == StatusCode::K_OK);
    }

    /**
     * @brief Compares the code of different objects for equality
     * @return true when code is equal, otherwise return false;
     */
    bool operator==(const Status &other) const
    {
        return (this->GetCode() == other.GetCode());
    }

    /**
     * @brief Compares the code of different objects for equality
     * @return true when code is not equal, otherwise return false;
     */
    bool operator!=(const Status &other) const
    {
        return !(*this == other);
    }

    /**
     * @brief Verify whether the code is K_OK.
     * @return true when code is K_OK, otherwise return false;
     */
    bool IsOk() const
    {
        return (GetCode() == StatusCode::K_OK);
    }

    /**
     * @brief Verify whether the code is K_OK.
     * @return true when code is not K_OK, otherwise return false;
     */
    bool IsError() const
    {
        return !IsOk();
    }

    /**
     * @brief Get code name.
     * @return code name;
     */
    static std::string StatusCodeName(StatusCode code);

private:
    void Assign(const Status &other) noexcept;

    struct State {
        StatusCode code;
        std::string errMsg;
    };
    std::unique_ptr<State> state_{ nullptr };
};
}  // namespace datasystem

#endif  // DATASYSTEM_UTILS_STATUS_H
