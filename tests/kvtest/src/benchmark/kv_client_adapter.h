#pragma once
#include <datasystem/kv_client.h>
#include <datasystem/utils/string_view.h>
#include <cstring>
#include <string>
#include <vector>

class KVClientAdapter {
public:
    explicit KVClientAdapter(std::shared_ptr<datasystem::KVClient> client,
                             datasystem::SetParam param)
        : client_(std::move(client)), param_(param) {}

    bool Set(const std::string &key, const std::string &data) {
        auto rc = client_->Set(key, datasystem::StringView(data), param_);
        return rc.IsOk();
    }

    bool GetVerify(const std::string &key) {
        datasystem::Optional<datasystem::Buffer> buf;
        auto rc = client_->Get(key, buf);
        return rc.IsOk() && bool(buf);
    }

    bool CreateAndSet(const std::string &key, uint64_t size, const std::string &data) {
        datasystem::SetParam cparam = param_;
        std::shared_ptr<datasystem::Buffer> buffer;
        auto rc = client_->Create(key, size, cparam, buffer);
        if (!rc.IsOk()) return false;
        buffer->WLatch();
        buffer->MemoryCopy(data.data(), size);
        buffer->UnWLatch();
        rc = client_->Set(buffer);
        return rc.IsOk();
    }

    bool CreateAndSetRaw(const std::string &key, uint64_t size, const std::string &data) {
        datasystem::SetParam cparam = param_;
        std::shared_ptr<datasystem::Buffer> buffer;
        auto rc = client_->Create(key, size, cparam, buffer);
        if (!rc.IsOk()) return false;
        memcpy(buffer->MutableData(), data.data(), size);
        rc = client_->Set(buffer);
        return rc.IsOk();
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
        std::vector<datasystem::Optional<datasystem::Buffer>> buffers;
        auto rc = client_->Get(keys, buffers);
        if (!rc.IsOk()) return false;
        for (auto &b : buffers) {
            if (!b) return false;
        }
        return true;
    }

    bool Del(const std::vector<std::string> &keys) {
        std::vector<std::string> failedKeys;
        auto rc = client_->Del(keys, failedKeys);
        return rc.IsOk() && failedKeys.empty();
    }

    datasystem::KVClient *RawClient() { return client_.get(); }

private:
    std::shared_ptr<datasystem::KVClient> client_;
    datasystem::SetParam param_;
};
