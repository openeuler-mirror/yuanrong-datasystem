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
 * Description: The helper macro of Status
 */
#ifndef DATASYSTEM_UTILS_STATUS_HELPER_H
#define DATASYSTEM_UTILS_STATUS_HELPER_H

#include "datasystem/common/log/log.h"
#include "datasystem/common/log/log_helper.h"
#include "datasystem/common/util/format.h"
#include "datasystem/utils/status.h"

#define RETURN_IF_NOT_OK(statement_) \
    do {                             \
        Status rc_ = (statement_);   \
        if (rc_.IsError()) {         \
            return rc_;              \
        }                            \
    } while (false)

#define RETURN_STATUS(code_, e_)                      \
    do {                                              \
        return Status(code_, __LINE__, __FILE__, e_); \
    } while (false)

#define CHECK_FAIL_RETURN_STATUS(condition_, code_, e_)   \
    do {                                                  \
        if (!(condition_)) {                              \
            return Status(code_, __LINE__, __FILE__, e_); \
        }                                                 \
    } while (false)

#define RETURN_IF_NOT_OK_EXCEPT(statement_, code_)       \
    do {                                                 \
        Status rc_ = (statement_);                       \
        if (rc_.IsError() && rc_.GetCode() != (code_)) { \
            return rc_;                                  \
        }                                                \
    } while (false)

#define RETURN_IF_NOT_OK_APPEND_MSG(statement_, msg_) \
    do {                                              \
        Status rc_ = (statement_);                    \
        if (rc_.IsError()) {                          \
            rc_.AppendMsg(msg_);                      \
            return rc_;                               \
        }                                             \
    } while (false)

#define RETURN_IF_NOT_OK_PRINT_ERROR_MSG(statement_, msg_)                                                           \
    do {                                                                                                             \
        Status rc_ = (statement_);                                                                                   \
        if (rc_.IsError()) {                                                                                         \
            std::string msg(msg_);                                                                                   \
            LOG(ERROR) << msg << ((!msg.empty() && msg.back() == '.') ? " " : ". ") << "Detail: " << rc_.ToString(); \
            return rc_;                                                                                              \
        }                                                                                                            \
    } while (false)

#define ASSIGN_IF_NOT_OK_PRINT_ERROR_MSG(lastRc_, statement_, msg_)                    \
    do {                                                                               \
        Status rc_ = (statement_);                                                     \
        if (rc_.IsError()) {                                                           \
            lastRc_ = rc_;                                                             \
            std::string msg(msg_);                                                                                   \
            LOG(ERROR) << msg << ((!msg.empty() && msg.back() == '.') ? " " : ". ") << "Detail: " << rc_.ToString(); \
        }                                                                              \
    } while (false)

#define RETURN_STATUS_LOG_ERROR(code_, e_)            \
    do {                                              \
        LOG(ERROR) << (e_);                           \
        return Status(code_, __LINE__, __FILE__, e_); \
    } while (false)

#define CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(condition_, code_, e_) \
    do {                                                            \
        if (!(condition_)) {                                        \
            RETURN_STATUS_LOG_ERROR(code_, e_);                     \
        }                                                           \
    } while (false)

#define RETURN_RUNTIME_ERROR_IF_NULL(ptr_)                                            \
    do {                                                                              \
        if ((ptr_) == nullptr) {                                                      \
            std::string errMsg = "The pointer [" + std::string(#ptr_) + "] is null."; \
            return Status(StatusCode::K_RUNTIME_ERROR, __LINE__, __FILE__, errMsg);   \
        }                                                                             \
    } while (false)

#define RETURN_OK_IF_TRUE(condition_) \
    do {                              \
        if (condition_) {             \
            return Status::OK();      \
        }                             \
    } while (false)

#define GET_VALUE_FROM_ANYS(list, index, var)                                                       \
    do {                                                                                            \
        auto index_ = (index);                                                                      \
        CHECK_FAIL_RETURN_STATUS((list).size() > (index_), StatusCode::K_INVALID, "invalid size."); \
        auto value_ = (list).at((index_));                                                          \
        if (!value_.Is<std::remove_reference_t<decltype(var)>>()) {                                 \
            RETURN_STATUS(StatusCode::K_INVALID, "invalid message type.");                          \
        }                                                                                           \
        value_.UnpackTo(&(var));                                                                    \
    } while (false)

// To handle possible exceptions occurred when initializing a thread pool
#define RETURN_IF_EXCEPTION_OCCURS(statement_)                                                 \
    do {                                                                                       \
        try {                                                                                  \
            (statement_);                                                                      \
        } catch (std::system_error & sysErr) {                                                 \
            std::string errMsg = std::string(sysErr.what()) + ", cannot acquire resources";    \
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, errMsg);                                \
        } catch (std::bad_alloc & badAlloc) {                                                  \
            std::string errMsg = std::string(badAlloc.what()) + ", cannot allocate resources"; \
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, errMsg);                                \
        }                                                                                      \
    } while (false)

#define LOG_IF_ERROR(statement_, msg_)                                                                               \
    do {                                                                                                             \
        Status rc_ = (statement_);                                                                                   \
        if (rc_.IsError()) {                                                                                         \
            std::string msg(msg_);                                                                                   \
            LOG(ERROR) << msg << ((!msg.empty() && msg.back() == '.') ? " " : ". ") << "Detail: " << rc_.ToString(); \
        }                                                                                                            \
    } while (false)

#define LOG_IF_ERROR_EXCEPT(statement_, msg_, code_)                                                                 \
    do {                                                                                                             \
        Status rc_ = (statement_);                                                                                   \
        if (rc_.IsError() && rc_.GetCode() != (code_)) {                                                             \
            std::string msg(msg_);                                                                                   \
            LOG(ERROR) << msg << ((!msg.empty() && msg.back() == '.') ? " " : ". ") << "Detail: " << rc_.ToString(); \
        }                                                                                                            \
    } while (false)

#define WARN_IF_ERROR(statement_, msg_)                                                                                \
    do {                                                                                                               \
        Status rc_ = (statement_);                                                                                     \
        if (rc_.IsError()) {                                                                                           \
            std::string msg(msg_);                                                                                     \
            LOG(WARNING) << msg << ((!msg.empty() && msg.back() == '.') ? " " : ". ") << "Detail: " << rc_.ToString(); \
        }                                                                                                              \
    } while (false)

#define DLOG_IF_ERROR(statement_, msg_)                                                                               \
    do {                                                                                                              \
        Status rc_ = (statement_);                                                                                    \
        if (rc_.IsError()) {                                                                                          \
            std::string msg(msg_);                                                                                    \
            DLOG(ERROR) << msg << ((!msg.empty() && msg.back() == '.') ? " " : ". ") << "Detail: " << rc_.ToString(); \
        }                                                                                                             \
    } while (false)

#define RETRY_ON_EINTR(statement)                                                                                   \
    do {                                                                                                            \
        int cnt_ = 0;                                                                                               \
        int ret_;                                                                                                   \
        do {                                                                                                        \
            ret_ = (statement);                                                                                     \
            cnt_++;                                                                                                 \
        } while (ret_ != 0 && errno == EINTR && cnt_ <= 10);                                                        \
        LOG_IF(ERROR, ret_ != 0) << FormatString("Get error after retry, retry count: %d, errno: %d", cnt_, errno); \
    } while (0)

#define RETRY_ON_EINTR_WITH_RET(statement, ret)             \
    do {                                                    \
        int cnt_ = 0;                                       \
        do {                                                \
            ret = (statement);                              \
            cnt_++;                                         \
        } while (ret != 0 && errno == EINTR && cnt_ <= 10); \
    } while (0)

#define RETRY_WHEN_DEADLOCK(statement_, rc_)                                                                   \
    do {                                                                                                       \
        rc_ = (statement_);                                                                                    \
        if (rc_.GetCode() == K_WORKER_DEADLOCK) {                                                              \
            const static int MAX_LOOP = 10;                                                                    \
            for (int i = 0; i < MAX_LOOP; i++) {                                                               \
                LOG(ERROR) << "Remote worker may deadlock, try again.";                                        \
                const uint64_t delayMs = RandomData().GetRandomUint64(RETRY_DELAY_MIN_MS, RETRY_DELAY_MAX_MS); \
                std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));                               \
                rc_ = (statement_);                                                                            \
                if (rc_.GetCode() != K_WORKER_DEADLOCK) {                                                      \
                    break;                                                                                     \
                }                                                                                              \
            }                                                                                                  \
        }                                                                                                      \
    } while (0)

#define RETURN_IF_NOT_OK_API(statement_, api_)                                   \
    do {                                                                         \
        Status rc_ = (statement_);                                               \
        if (rc_.IsError()) {                                                     \
            Status rc2 = api_->SendStatus(rc_);                                  \
            if (rc2.IsError()) {                                                 \
                DLOG(ERROR) << "Send status failed. Detail: " << rc2.ToString(); \
            }                                                                    \
            return rc_;                                                          \
        }                                                                        \
    } while (false)
#endif  // DATASYSTEM_UTILS_STATUS_H
