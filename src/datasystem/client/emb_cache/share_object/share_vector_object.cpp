#include <string>
#include <vector>
#include <memory>
#include <cassert>

#include "datasystem/utils/status.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/string_view.h"
#include "datasystem/object/buffer.h"
#include "datasystem/object/share_object.h"
#include "datasystem/object/share_vector_object.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
#include "datasystem/kv_client.h"

namespace datasystem {

ShareVectorObject::ShareVectorObject(const std::string &key, std::shared_ptr<KVClient> kvClient)
    : key_(key), kvClient_(std::move(kvClient)), shareObject_(kvClient_)
{
    isInit_ = false;
}

ShareVectorObject::ShareVectorObject(const std::string &key, uint64_t dimSize, std::shared_ptr<KVClient> kvClient,
                                     uint64_t shareObjectSize, uint64_t vectorCapacity)
    : key_(key),
      dimSize_(dimSize),
      shareObjectSize_(shareObjectSize),
      kvClient_(std::move(kvClient)),
      shareObject_(kvClient_)
{
    objectCapacity_ = shareObjectSize_ / dimSize_;
    vectorCapacity_ = 0;
    if (objectCapacity_ * dimSize_ != shareObjectSize_) {
        LOG(WARNING) << FormatString("dimSize %llu is not aligned with shareObjectSize %llu", dimSize_,
                                     shareObjectSize_);
    }

    if (vectorCapacity > 0) {
        Reserve(vectorCapacity, 0);
    }

    isInit_ = true;
}

Status ShareVectorObject::Reserve(uint64_t vectorCapacity, uint64_t oldCapacity)
{
    if (vectorCapacity <= vectorCapacity_.load()) {
        LOG(WARNING) << FormatString("Vector [%s]: new vectorCapacity %llu is less than old one %llu", key_.c_str(),
                                     vectorCapacity, vectorCapacity_.load());
        return Status::OK();
    }

    std::unique_lock<std::shared_mutex> lock(rw_mutex_);

    auto curBufferSize = shareBuffers_.size();
    auto oldShareBufferNum = ShareBufferNum(oldCapacity);
    auto newShareBufferNum = ShareBufferNum(vectorCapacity);
    auto newVectorCapacity = newShareBufferNum * objectCapacity_;

    shareBuffers_.resize(newShareBufferNum);
    vectorCapacity_.store(newVectorCapacity);

    lock.unlock();

    // [curBufferSize, oldShareBufferNum) is created by other process
    for (size_t i = curBufferSize; i < oldShareBufferNum; i++) {
        Optional<ShareBuffer> buffer;
        Status createStatus = shareObject_.Import(Index2ShareObjectName(i), buffer);
        if (!createStatus.IsOk()) {
            auto errMsg = FormatString("Import ShareObject %s failed, status: %s", createStatus.ToString().c_str());
            LOG(ERROR) << errMsg;
            return Status(StatusCode::K_IO_ERROR, errMsg);
        }
        shareBuffers_[i] = std::move(buffer.value());
    }

    // [oldShareBufferNum, newShareBufferNum) will be created here
    for (size_t i = oldShareBufferNum; i < newShareBufferNum; i++) {
        Optional<ShareBuffer> buffer;
        Status createStatus = shareObject_.Create(Index2ShareObjectName(i), shareObjectSize_, buffer);
        // Publish instantly
        Status publisheStatus = shareObject_.Publish(buffer.value());
        if (!createStatus.IsOk() || !publisheStatus.IsOk()) {
            auto errMsg = FormatString("Create ShareObject %s failed, status: %s", createStatus.ToString().c_str());
            LOG(ERROR) << errMsg;
            return Status(StatusCode::K_IO_ERROR, errMsg);
        }
        shareBuffers_[i] = std::move(buffer.value());
    }

    LOG(INFO) << FormatString("Upscale ShareVectorObject %s %.1fMB, has %zu ShareObjects", key_.c_str(),
                              1.0 * vectorCapacity_ * dimSize_ / 1024 / 1024, shareBuffers_.size());

    // RETURN_IF_NOT_OK(UpdateMeta(SerializeMeta()));

    return Status::OK();
}

Status ShareVectorObject::Set(uint64_t index, const void *value)
{
    if (index >= vectorCapacity_) {
        auto errMsg = FormatString("Vector %s: set index overflow %llu >= %llu", key_.c_str(), index, vectorCapacity_);
        LOG(ERROR) << errMsg;
        return Status(StatusCode::K_OUT_OF_RANGE, errMsg);
    }
    auto [objectIndex, objectOffset] = IndexOffset(index);

    RETURN_IF_NOT_OK(shareBuffers_[objectIndex].Write(objectOffset * dimSize_, value, dimSize_));
    return Status::OK();
}

Status ShareVectorObject::Set(uint64_t index, uint64_t value)
{
    if (index >= vectorCapacity_) {
        auto errMsg = FormatString("Vector %s: set index overflow %llu >= %llu", key_.c_str(), index, vectorCapacity_);
        LOG(ERROR) << errMsg;
        return Status(StatusCode::K_OUT_OF_RANGE, errMsg);
    }
    auto [objectIndex, objectOffset] = IndexOffset(index);

    std::shared_lock<std::shared_mutex> lock(rw_mutex_);
    RETURN_IF_NOT_OK(shareBuffers_[objectIndex].Write(objectOffset * dimSize_, &value, dimSize_));
    return Status::OK();
}

Status ShareVectorObject::Lookup(uint64_t index, StringView &buffer) const
{
    if (index >= vectorCapacity_) {
        auto errMsg =
            FormatString("Vector %s: lookup index overflow %llu >= %llu", key_.c_str(), index, vectorCapacity_);
        LOG(ERROR) << errMsg;
        return Status(StatusCode::K_OUT_OF_RANGE, errMsg);
    }
    size_t objectIndex = index / objectCapacity_;
    size_t objectOffset = index % objectCapacity_;

    std::shared_lock<std::shared_mutex> lock(rw_mutex_);
    RETURN_IF_NOT_OK(shareBuffers_[objectIndex].Read(objectOffset * dimSize_, dimSize_, buffer));

    return Status::OK();
}

Status ShareVectorObject::RestoreShareObject(uint64_t dimSize, uint64_t dataNum, uint64_t shareObjectSize)
{
    // shareObjectSize_, vectorCapacity_, dimSize_

    shareObjectSize_ = shareObjectSize;
    objectCapacity_ = shareObjectSize / dimSize;
    dimSize_ = dimSize;
    auto nObjects = ShareBufferNum(dataNum);
    vectorCapacity_ = nObjects * objectCapacity_;

    shareBuffers_.reserve(nObjects);

    for (size_t i = 0; i < nObjects; i++) {
        Optional<ShareBuffer> buffer;
        Status createStatus = shareObject_.Import(Index2ShareObjectName(i), buffer);
        if (!createStatus.IsOk() || buffer.value().Size() != shareObjectSize_) {
            LOG(ERROR) << FormatString("Import ShareObject %s failed, status: %s", Index2ShareObjectName(i),
                                       createStatus.ToString().c_str());
            return createStatus;
        }
        shareBuffers_.emplace_back(std::move(buffer.value()));
    }

    isInit_ = true;

    LOG(INFO) << FormatString("Restore Vector [%s] success. dim size %llu, vectorCapacity %.1fMB", key_.c_str(),
                              dimSize_, 1.0 * dimSize_ * vectorCapacity_.load() / 1024 / 1024);

    return Status::OK();
}

Status ShareVectorObject::Publish()
{
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);

    for (const auto &buffer : shareBuffers_) {
        RETURN_IF_NOT_OK(shareObject_.Publish(buffer));
    }

    return Status::OK();
}

Status ShareVectorObject::ExportToVector(std::vector<uint64_t> &buffer, size_t size)
{
    assert(dimSize_ == sizeof(uint64_t));

    buffer.resize(size);

    size_t nFullObject = size / objectCapacity_;
    size_t nPartialObject = size % objectCapacity_;

    for (size_t i = 0; i < nFullObject; i++) {
        std::memcpy(reinterpret_cast<uint8_t *>(buffer.data()) + i * objectCapacity_ * dimSize_,
                    shareBuffers_[i].Data(), shareBuffers_[i].Size());
    }

    if (nPartialObject > 0) {
        std::memcpy(reinterpret_cast<uint8_t *>(buffer.data()) + nFullObject * objectCapacity_ * dimSize_,
                    shareBuffers_[nFullObject].Data(), nPartialObject * dimSize_);
    }

    return Status::OK();
}

const std::string &ShareVectorObject::GetKey() const
{
    return key_;
}

std::string ShareVectorObject::Index2ShareObjectName(size_t index) const
{
    return key_ + "-vec-" + std::to_string(index);
}

std::pair<size_t, size_t> ShareVectorObject::IndexOffset(size_t index) const
{
    return std::make_pair(index / objectCapacity_, index % objectCapacity_);
}

std::string ShareVectorObject::SerializeMeta() const
{
    // shareObjectSize_, objectCapacity_, vectorCapacity_, dimSize_, key_
    size_t metaSize = sizeof(uint64_t) * 4 + key_.length();
    std::string meta(metaSize, '\0');

    size_t offset = 0;
    std::memcpy(meta.data(), &shareObjectSize_, sizeof(uint64_t));
    offset += sizeof(uint64_t);

    std::memcpy(meta.data() + offset, &objectCapacity_, sizeof(uint64_t));
    offset += sizeof(uint64_t);

    std::memcpy(meta.data() + offset, &vectorCapacity_, sizeof(uint64_t));
    offset += sizeof(uint64_t);

    std::memcpy(meta.data() + offset, &dimSize_, sizeof(uint64_t));
    offset += sizeof(uint64_t);

    std::memcpy(meta.data() + offset, key_.data(), key_.length());

    return meta;
}

Status ShareVectorObject::DeserializeMeta(const std::string &serializedMeta)
{
    size_t metaSize = sizeof(uint64_t) * 4 + key_.length();

    if (serializedMeta.size() != metaSize
        || std::string_view(serializedMeta.data() + (metaSize - key_.length()), key_.length()) != key_) {
        auto errMsg = FormatString("Key [%s] Meta is invalid.", key_.c_str());
        LOG(ERROR) << errMsg;
        return Status(StatusCode::K_INVALID, errMsg);
    }

    auto addr = serializedMeta.data();
    size_t offset = 0;
    uint64_t tempVal;

    std::memcpy(&tempVal, addr + offset, sizeof(tempVal));
    shareObjectSize_ = tempVal;
    offset += sizeof(tempVal);

    std::memcpy(&tempVal, addr + offset, sizeof(tempVal));
    objectCapacity_ = tempVal;
    offset += sizeof(tempVal);

    std::memcpy(&tempVal, addr + offset, sizeof(tempVal));
    vectorCapacity_ = tempVal;
    offset += sizeof(tempVal);

    std::memcpy(&tempVal, addr + offset, sizeof(tempVal));
    dimSize_ = tempVal;

    LOG(INFO) << FormatString("ShareVectorObject [%s] meta: size %llu dimsize %llu ", key_.c_str(),
                              vectorCapacity_.load(), dimSize_);

    return Status::OK();
}

Status ShareVectorObject::QueryMeta(std::string &buffer) const
{
    Status status = kvClient_->Get(GetMetaKey(), buffer);
    if (!status.IsOk()) {
        LOG(ERROR) << FormatString("ShareVectorObject [%s] query meta failed, err msg: %s", key_.c_str(),
                                   status.ToString());
        return status;
    }

    return Status::OK();
}

Status ShareVectorObject::UpdateMeta(const std::string &serializedMeta) const
{
    Status status = kvClient_->Set(GetMetaKey(), serializedMeta);
    if (!status.IsOk()) {
        LOG(ERROR) << FormatString("ShareVectorObject [%s] update meta failed, err msg: %s", key_.c_str(),
                                   status.ToString());
        return status;
    }

    return Status::OK();
}

}  // namespace datasystem