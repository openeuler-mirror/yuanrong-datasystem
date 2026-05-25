#pragma once
#include <datasystem/kv_client.h>
#include <datasystem/utils/string_view.h>
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

    bool Get(const std::string &key, std::string &out) {
        datasystem::Optional<datasystem::Buffer> buf;
        auto rc = client_->Get(key, buf);
        if (!rc.IsOk() || !buf) return false;
        auto size = buf->GetSize();
        out.assign(static_cast<const char *>(buf->ImmutableData()),
                   size > 0 ? static_cast<size_t>(size) : 0);
        return true;
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

    bool Del(const std::vector<std::string> &keys) {
        std::vector<std::string> failedKeys;
        auto rc = client_->Del(keys, failedKeys);
        return rc.IsOk();
    }

    datasystem::KVClient *RawClient() { return client_.get(); }

private:
    std::shared_ptr<datasystem::KVClient> client_;
    datasystem::SetParam param_;
};
