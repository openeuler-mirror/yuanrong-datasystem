#pragma once
#include <cstdint>
#include <atomic>
#include <memory>
#include <string>
#include <vector>
#include <mutex>
#include <unordered_map>
#include <datasystem/utils/cuda_funcs.h>

namespace datasystem {

enum StatusCode { K_OK = 0, K_NOT_FOUND = 1, K_RUNTIME_ERROR = 2, K_INVALID = 3 };

class Status {
public:
    using Code = StatusCode;
    Status() : code_(K_OK) {}
    Status(Code c, const std::string &msg = "") : code_(c), msg_(msg) {}
    bool IsOk() const { return code_ == K_OK; }
    std::string GetMsg() const { return msg_; }
    Code GetCode() const { return code_; }
    std::string ToString() const { return msg_; }
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
    uint32_t ttlSecond = 0;
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
    Buffer(std::string key, uint64_t size) : data_(size, '\0'), size_(size), key_(std::move(key)) {}
    Status MemoryCopy(const char *data, uint64_t size) {
        data_.assign(data, size);
        size_ = size;
        return Status::OK();
    }
    int64_t GetSize() const { return static_cast<int64_t>(size_); }
    const void *ImmutableData() const { return data_.data(); }
    void *MutableData() { return data_.data(); }
    const std::string &Key() const { return key_; }
    void RLatch() {}
    void UnRLatch() {}
private:
    std::string data_;
    uint64_t size_;
    std::string key_;
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
    Status Set(const std::shared_ptr<Buffer> &buffer) {
        std::lock_guard<std::mutex> lock(mutex_);
        values_[buffer->Key()] = *buffer;
        return Status::OK();
    }
    Status Get(const std::string &key, Optional<Buffer> &opt) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = values_.find(key);
        if (it == values_.end()) return Status::NotFound();
        opt = it->second;
        return Status::OK();
    }
    Status Get(const std::vector<std::string> &keys, std::vector<Optional<Buffer>> &results) {
        results.resize(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) (void)Get(keys[i], results[i]);
        return Status::OK();
    }
    Status Exist(const std::vector<std::string> &keys, std::vector<bool> &exists) {
        exists.assign(keys.size(), true);
        return Status::OK();
    }
    Status Create(const std::string &key, uint64_t size, const SetParam &, std::shared_ptr<Buffer> &buf) {
        buf = std::make_shared<Buffer>(key, size);
        return Status::OK();
    }
    Status MCreate(const std::vector<std::string> &keys, const std::vector<uint64_t> &sizes, const SetParam &,
                   std::vector<std::shared_ptr<Buffer>> &bufs) {
        bufs.clear();
        for (size_t i = 0; i < keys.size(); ++i) bufs.push_back(std::make_shared<Buffer>(keys[i], sizes[i]));
        return Status::OK();
    }
    Status MSet(const std::vector<std::shared_ptr<Buffer>> &buffers) {
        for (const auto &buffer : buffers) Set(buffer);
        return Status::OK();
    }
    Status Del(const std::vector<std::string> &) { return Status::OK(); }
    Status Del(const std::string &key) {
        std::lock_guard<std::mutex> lock(mutex_);
        values_.erase(key);
        ++deletedKeys;
        return Status::OK();
    }
    inline static int deletedKeys = 0;
    static void RegisterCudaFuncs(const CudaFuncs &funcs) { callbacks = funcs; ++registrations; }
    Status DsCudaMemcpyAsync(void *dst, const void *src, size_t size, DsCudaMemcpyKind kind, void *stream) {
        if (!callbacks.memcpyAsync) return Status::Error("callback not registered");
        ++dsCopies;
        if (splitCopies) {
            const size_t first = size / 2;
            int rc = callbacks.memcpyAsync(dst, src, first, kind, stream);
            if (rc == 0) rc = callbacks.memcpyAsync(static_cast<char *>(dst) + first,
                static_cast<const char *>(src) + first, size - first, kind, stream);
            return rc == 0 ? Status::OK() : Status::Error("injected fragment failure");
        }
        const int rc = callbacks.memcpyAsync(dst, src, size, kind, stream);
        return rc == 0 ? Status::OK() : Status::Error("injected CUDA failure");
    }
    inline static CudaFuncs callbacks;
    inline static int registrations = 0;
    inline static std::atomic<int> dsCopies{0};
    inline static bool splitCopies = false;
private:
    std::mutex mutex_;
    std::unordered_map<std::string, Buffer> values_;
};

struct ConnectOptions {};
struct ServiceDiscoveryOptions {};
class ServiceDiscovery {
public:
    ServiceDiscovery(const ServiceDiscoveryOptions &) {}
    Status Init() { return Status::OK(); }
};

}  // namespace datasystem
