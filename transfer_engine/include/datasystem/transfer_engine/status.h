#ifndef TRANSFER_ENGINE_STATUS_H
#define TRANSFER_ENGINE_STATUS_H

#include <cstdint>
#include <ostream>
#include <sstream>
#include <string>

namespace datasystem {

enum class ErrorCode : int32_t {
    kOk = 0,
    kInvalid = 2,
    kNotFound = 3,
    kRuntimeError = 5,
    kNotReady = 8,
    kNotAuthorized = 9,
    kNotSupported = 36,
};

class Result {
public:
    Result() : code_(ErrorCode::kOk) {}
    Result(ErrorCode code, std::string msg, int line = 0, const char *file = nullptr)
        : code_(code), msg_(std::move(msg)), line_(line), file_(file == nullptr ? "" : file)
    {
    }

    static Result OK() { return Result(); }

    bool IsOk() const { return code_ == ErrorCode::kOk; }
    bool IsError() const { return !IsOk(); }
    ErrorCode GetCode() const { return code_; }
    const std::string &GetMsg() const { return msg_; }
    int GetLine() const { return line_; }
    const std::string &GetFile() const { return file_; }
    std::string ToString() const
    {
        std::ostringstream oss;
        oss << "code=" << static_cast<int32_t>(code_) << ", msg=" << msg_;
        if (!file_.empty() && line_ > 0) {
            oss << ", file=" << file_ << ", line=" << line_;
        }
        return oss.str();
    }

    friend std::ostream &operator<<(std::ostream &os, const Result &status)
    {
        os << status.ToString();
        return os;
    }

private:
    ErrorCode code_;
    std::string msg_;
    int line_ = 0;
    std::string file_;
};

}  // namespace datasystem

#endif  // TRANSFER_ENGINE_STATUS_H
