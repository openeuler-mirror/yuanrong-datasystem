#include <string>

#include "datasystem/utils/status.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/optional.h"
#include "datasystem/kv_client.h"
#include "datasystem/object/share_object.h"

namespace datasystem {

ShareObject::ShareObject(std::shared_ptr<KVClient> kvClient) : kvClient_(std::move(kvClient))
{
}

Status ShareObject::Create(const std::string &key, size_t size, Optional<ShareBuffer> &shareBuffer)
{
    std::shared_ptr<Buffer> buffer;
    RETURN_IF_NOT_OK(kvClient_->Create(key, size, {}, buffer));
    shareBuffer = Optional<ShareBuffer>(std::move(buffer));
    return Status::OK();
}

Status ShareObject::Import(const std::string &key, Optional<ShareBuffer> &shareBuffer)
{
    Optional<Buffer> buffer;
    RETURN_IF_NOT_OK(kvClient_->Get(key, buffer));
    shareBuffer = Optional<ShareBuffer>(std::move(buffer));
    return Status::OK();
}

Status ShareObject::Publish(const ShareBuffer &shareBuffer)
{
    RETURN_IF_NOT_OK(kvClient_->Set(shareBuffer.buffer_));
    return Status::OK();
}

Status ShareObject::Del(const std::vector<std::string> &keys)
{
    std::vector<std::string> failedKeys;
    RETURN_IF_NOT_OK(kvClient_->Del(keys, failedKeys));
    return Status::OK();
}

ShareBuffer::ShareBuffer(std::shared_ptr<Buffer> buffer) : buffer_(std::move(buffer))
{
}

ShareBuffer::ShareBuffer(Optional<Buffer> buffer) : buffer_(std::make_shared<Buffer>(std::move(buffer.value())))
{
}

void *ShareBuffer::Data() const
{
    return buffer_->MutableData();
}

size_t ShareBuffer::Size() const
{
    return buffer_->GetSize();
}

Status ShareBuffer::Write(size_t offset, const void *data, size_t len)
{
    std::memcpy(buffer_->MutableData() + offset, data, len);
    return Status::OK();
}

Status ShareBuffer::Read(size_t offset, size_t len, StringView &buffer) const
{
    buffer = StringView(static_cast<const char *>(buffer_->ImmutableData()) + offset, len);
    return Status::OK();
}

}  // namespace datasystem