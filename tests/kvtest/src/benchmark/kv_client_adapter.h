#pragma once
#include <chrono>
#include <datasystem/kv_client.h>
#include <datasystem/utils/string_view.h>
#include "benchmark/benchmark_result.h"
#include "pipeline/pipeline.h"
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

class KVClientAdapter {
public:
    explicit KVClientAdapter(std::shared_ptr<datasystem::KVClient> client,
                             datasystem::SetParam param)
        : client_(std::move(client)), param_(param) {}

    bool Set(const std::string &key, const std::string &data) {
        return SetWithStatus(key, data).success;
    }

    /** @brief Set one key and preserve status categories used by benchmark diagnostics. */
    BenchmarkOpResult SetWithStatus(const std::string &key, const std::string &data) {
        return ToResult(client_->Set(key, datasystem::StringView(data), param_));
    }

    bool GetVerify(const std::string &key) {
        return GetWithStatus(key).success;
    }

    /** @brief Get one key and preserve status categories used by benchmark diagnostics. */
    BenchmarkOpResult GetWithStatus(const std::string &key) {
        if (!IsKvtestClientInitialized()) {
            return { true, false, false };
        }
        datasystem::Optional<datasystem::Buffer> buf;
        return ToResult(client_->Get(key, buf));
    }

    bool CreateAndSet(const std::string &key, uint64_t size, const std::string &data) {
        return CreateAndSetWithStatus(key, size, data).success;
    }

    /**
     * @brief Create and publish one buffer while optionally measuring only Set.
     * @param[in] key Object key.
     * @param[in] size Object size.
     * @param[in] data Object data.
     * @param[out] setTiming Set interval, or null when separate timing is not needed.
     * @return Benchmark operation result.
     */
    BenchmarkOpResult CreateAndSetWithStatus(const std::string &key, uint64_t size, const std::string &data,
                                             BenchmarkOpTiming *setTiming = nullptr) {
        if (setTiming != nullptr) {
            *setTiming = {};
        }
        datasystem::SetParam cparam = param_;
        std::shared_ptr<datasystem::Buffer> buffer;
        auto rc = client_->Create(key, size, cparam, buffer);
        if (!rc.IsOk()) return ToResult(rc);
        (void)data;
        (void)size;
        // No-copy benchmark: publish the freshly created Buffer directly.
        // Restore the write below when content validation is needed again.
        // buffer->WLatch();
        // buffer->MemoryCopy(data.data(), size);
        // buffer->UnWLatch();
        if (setTiming != nullptr) {
            setTiming->startNs = SteadyNowNs();
        }
        rc = client_->Set(buffer);
        if (setTiming != nullptr) {
            setTiming->endNs = SteadyNowNs();
        }
        return ToResult(rc);
    }

    bool CreateAndSetRaw(const std::string &key, uint64_t size, const std::string &data) {
        return CreateAndSetRawWithStatus(key, size, data).success;
    }

    /**
     * @brief Create and publish one raw buffer while optionally measuring only Set.
     * @param[in] key Object key.
     * @param[in] size Object size.
     * @param[in] data Object data.
     * @param[out] setTiming Set interval, or null when separate timing is not needed.
     * @return Benchmark operation result.
     */
    BenchmarkOpResult CreateAndSetRawWithStatus(const std::string &key, uint64_t size, const std::string &data,
                                                BenchmarkOpTiming *setTiming = nullptr) {
        if (setTiming != nullptr) {
            *setTiming = {};
        }
        datasystem::SetParam cparam = param_;
        std::shared_ptr<datasystem::Buffer> buffer;
        auto rc = client_->Create(key, size, cparam, buffer);
        if (!rc.IsOk()) return ToResult(rc);
        (void)data;
        (void)size;
        // No-copy benchmark: publish the freshly created Buffer directly.
        // Restore the write below when content validation is needed again.
        // memcpy(buffer->MutableData(), data.data(), size);
        if (setTiming != nullptr) {
            setTiming->startNs = SteadyNowNs();
        }
        rc = client_->Set(buffer);
        if (setTiming != nullptr) {
            setTiming->endNs = SteadyNowNs();
        }
        return ToResult(rc);
    }

    bool MSet(const std::vector<std::string> &keys, const std::string &data) {
        std::vector<datasystem::StringView> vals;
        vals.reserve(keys.size());
        for (size_t i = 0; i < keys.size(); i++) {
            vals.emplace_back(datasystem::StringView(data));
        }
        datasystem::MSetParam mParam;
        mParam.writeMode = param_.writeMode;
        mParam.ttlSecond = param_.ttlSecond;
        std::vector<std::string> failedKeys;
        auto rc = client_->MSet(keys, vals, failedKeys, mParam);
        return rc.IsOk() && failedKeys.empty();
    }

    bool MGetVerify(const std::vector<std::string> &keys) {
        if (!IsKvtestClientInitialized()) return true;
        std::vector<datasystem::Optional<datasystem::Buffer>> buffers;
        auto rc = client_->Get(keys, buffers);
        if (!rc.IsOk()) return false;
        for (auto &b : buffers) {
            if (!b) return false;
        }
        return true;
    }

    bool Del(const std::vector<std::string> &keys) {
        return DelWithStatus(keys).success;
    }

    /** @brief Delete keys and treat partial deletion as an operation failure. */
    BenchmarkOpResult DelWithStatus(const std::vector<std::string> &keys,
                                    std::vector<std::string> *failedKeysOut = nullptr) {
        std::vector<std::string> failedKeys;
        auto rc = client_->Del(keys, failedKeys);
        if (failedKeysOut != nullptr) {
            *failedKeysOut = failedKeys;
        }
        if (!rc.IsOk()) return ToResult(rc);
        if (!failedKeys.empty()) {
            return { false, false, false };
        }
        return { true, false, false };
    }

    datasystem::KVClient *RawClient() { return client_.get(); }

private:
    static int64_t SteadyNowNs() {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }

    static BenchmarkOpResult ToResult(const datasystem::Status &rc) {
        const auto code = rc.GetCode();
        return { rc.IsOk(), code == datasystem::StatusCode::K_NOT_FOUND,
                 code == datasystem::StatusCode::K_RPC_DEADLINE_EXCEEDED };
    }

    std::shared_ptr<datasystem::KVClient> client_;
    datasystem::SetParam param_;
};
