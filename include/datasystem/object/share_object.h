#pragma once

#include <string>

#include "datasystem/utils/status.h"
#include "datasystem/utils/optional.h"
#include "datasystem/kv_client.h"
namespace datasystem {

class ShareBuffer;

class ShareObject {
public:
    ShareObject(std::shared_ptr<KVClient> kvClient);

    Status Create(const std::string &key, size_t size, Optional<ShareBuffer> &shareBuffer);

    Status Import(const std::string &key, Optional<ShareBuffer> &shareBuffer);

    Status Publish(const ShareBuffer &shareBuffer);

    Status Del(const std::vector<std::string> &keys);

private:
    std::shared_ptr<KVClient> kvClient_;
};

class ShareBuffer {
public:
    ShareBuffer() = default;
    ShareBuffer(std::shared_ptr<Buffer> buffer);
    ShareBuffer(Optional<Buffer> buffer);

    void *Data() const;

    size_t Size() const;

    Status Write(size_t offset, const void *data, size_t len);

    Status Read(size_t offset, size_t len, StringView &buffer) const;

    friend ShareObject;

private:
    std::shared_ptr<Buffer> buffer_;
};

};  // namespace datasystem