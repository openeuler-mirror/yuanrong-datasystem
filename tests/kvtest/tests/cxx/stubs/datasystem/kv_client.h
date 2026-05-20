#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace datasystem {

class Status {
public:
    enum Code { K_OK = 0, K_NOT_FOUND = 1, K_RUNTIME_ERROR = 2 };
    Status() : code_(K_OK) {}
    Status(Code c, const std::string &msg = "") : code_(c), msg_(msg) {}
    bool IsOk() const { return code_ == K_OK; }
    std::string GetMsg() const { return msg_; }
    static Status OK() { return Status(); }
    static Status NotFound(const std::string &msg = "") { return Status(K_NOT_FOUND, msg); }
    static Status Error(const std::string &msg = "") { return Status(K_RUNTIME_ERROR, msg); }
private:
    Code code_;
    std::string msg_;
};

enum class WriteMode {
    NONE_L2_CACHE = 0,
    NONE_L2_CACHE_EVICT = 1,
};

struct SetParam {
    WriteMode writeMode = WriteMode::NONE_L2_CACHE;
};

class StringView {
public:
    StringView() : data_(""), size_(0) {}
    StringView(const std::string &s) : data_(s.data()), size_(s.size()) {}
    StringView(const char *d, size_t s) : data_(d), size_(s) {}
    const char *data() const { return data_; }
    size_t size() const { return size_; }
private:
    const char *data_;
    size_t size_;
};

template <typename T>
class Optional {
public:
    Optional() : has_(false) {}
    Optional(const T &v) : has_(true), val_(v) {}
    explicit operator bool() const { return has_; }
    T& operator*() { return val_; }
    const T& operator*() const { return val_; }
    T* operator->() { return &val_; }
    const T* operator->() const { return &val_; }
private:
    bool has_;
    T val_;
};

class Buffer {
public:
    Buffer() : size_(0) {}
    Status MemoryCopy(const char *data, uint64_t size) {
        data_.assign(data, size);
        size_ = size;
        return Status::OK();
    }
    int64_t GetSize() const { return static_cast<int64_t>(size_); }
    const char *ImmutableData() const { return data_.data(); }
    void RLatch() {}
    void UnRLatch() {}
private:
    std::string data_;
    uint64_t size_;
};

class ReadOnlyBuffer {
public:
    const char *ImmutableData() const { return ""; }
    int64_t GetSize() const { return 0; }
    void RLatch() {}
    void UnRLatch() {}
};

class KVClient {
public:
    KVClient() = default;
    Status Init() { return Status::OK(); }
    Status Set(const std::string &, const StringView &, const SetParam & = SetParam()) { return Status::OK(); }
    Status Set(const std::shared_ptr<Buffer> &) { return Status::OK(); }
    Status Get(const std::string &, Optional<Buffer> &opt) { opt = Buffer(); return Status::OK(); }
    Status Get(const std::vector<std::string> &keys, std::vector<Optional<Buffer>> &results) {
        results.assign(keys.size(), Optional<Buffer>(Buffer()));
        return Status::OK();
    }
    Status Exist(const std::vector<std::string> &keys, std::vector<bool> &exists) {
        exists.assign(keys.size(), true);
        return Status::OK();
    }
    Status Create(const std::string &, uint64_t size, const SetParam &, std::shared_ptr<Buffer> &buf) {
        buf = std::make_shared<Buffer>();
        return Status::OK();
    }
    Status MCreate(const std::vector<std::string> &keys, const std::vector<uint64_t> &, const SetParam &,
                   std::vector<std::shared_ptr<Buffer>> &bufs) {
        bufs.assign(keys.size(), std::make_shared<Buffer>());
        return Status::OK();
    }
    Status MSet(const std::vector<std::shared_ptr<Buffer>> &) { return Status::OK(); }
    Status Del(const std::vector<std::string> &) { return Status::OK(); }
};

struct ConnectOptions {};
struct ServiceDiscoveryOptions {};
class ServiceDiscovery {
public:
    ServiceDiscovery(const ServiceDiscoveryOptions &) {}
    Status Init() { return Status::OK(); }
};

}  // namespace datasystem
