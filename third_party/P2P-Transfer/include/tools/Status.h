/*
* Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef P2P_STATUS_H
#define P2P_STATUS_H
#include <string>
#include <iostream>
#include <optional>
#include <sstream>

enum class ErrorCode {
    SUCCESS,
    TCP_ERROR,
    NUMA_ERROR,
    NOT_FOUND,
    OUT_OF_RANGE,
    NOT_INITIALIZED,
    REPEAT_INITIALIZE,
    NOT_SUPPORTED,
    REPEAT_SUBSCRIBE,
    INVALID_ENV,
    ACL_ERROR,
    ERR_SOCK_EADDRINUSE,
    SOCKET_ERROR,
    TIMEOUT,
    PROTOBUF_ERROR,
    INVALID_INPUT,
    INTERNAL_ERROR
};

class Status {
public:
    explicit Status(ErrorCode code = ErrorCode::SUCCESS, const std::string &message = "")
        : code_(code),
          message_(message)
    {
    }

    bool IsSuccess() const
    {
        return code_ == ErrorCode::SUCCESS;
    }

    ErrorCode Code() const
    {
        return code_;
    }

    std::string Message() const
    {
        return message_;
    }

    static Status Success()
    {
        return Status(ErrorCode::SUCCESS);
    }

    static Status Error(ErrorCode code, const std::string &message = "")
    {
        return Status(code, message);
    }

    std::string ToString() const
    {
        std::ostringstream oss;
        oss << "Status: ";
        if (IsSuccess()) {
            oss << "Success";
        } else {
            oss << "Error Code: " << static_cast<int>(code_) << ", Message: " << message_;
        }
        return oss.str();
    }

    friend std::ostream &operator<<(std::ostream &os, const Status &s);

private:
    ErrorCode code_;
    std::string message_;
};

#endif  // P2P_STATUS_H