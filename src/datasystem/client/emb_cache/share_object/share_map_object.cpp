#include <string>
#include <vector>
#include <memory>

#include "datasystem/utils/status.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/parallel/parallel_for.h"
#include "datasystem/utils/string_view.h"
#include "datasystem/object/buffer.h"
#include "datasystem/object/share_object.h"
#include "datasystem/object/share_vector_object.h"
#include "datasystem/object/share_map_object.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
#include "datasystem/kv_client.h"

namespace datasystem {

ShareMapObject::ShareMapObject(const std::string &bucketKey, std::shared_ptr<KVClient> kvClient, uint64_t dimSize)
    : key_(bucketKey), kvClient_(std::move(kvClient)), shareObject_(kvClient_), dimSize_(dimSize)
{
    std::string serializedMeta;
    Status metaStatus = QueryMeta(serializedMeta);
    if (metaStatus.IsOk()) {
        Status restoreStatus = Restore(serializedMeta);
        if (!restoreStatus.IsOk()) {
            LOG(ERROR) << FormatString("Restore ShareMapObject [%s] failed, err msg: %s", key_,
                                       restoreStatus.ToString());
        } else {
            LOG(INFO) << FormatString("Restore ShareMapObject [%s] success.", key_.c_str());
        }
    } else {
        shareKey_ = std::make_unique<ShareVectorObject>(GetVectorKey(SHARE_KEY_SUFFIX), STORED_KEY_SIZE, kvClient_);
        shareValue_ = std::make_unique<ShareVectorObject>(GetVectorKey(SHARE_VALUE_SUFFIX), dimSize_, kvClient_);
        std::string serializedMeta = SerializeMeta();
        metaStatus = UpdateMeta(serializedMeta, true);
        if (!metaStatus.IsOk()) {
            LOG(ERROR) << FormatString("Init ShareMapObject [%s] failed, err msg: %s", key_, metaStatus.ToString());
        } else {
            LOG(INFO) << FormatString("Create ShareMapObject [%s] success.", key_.c_str());
        }
    }
}

Status ShareMapObject::Insert(const std::vector<uint64_t> &keys, const std::vector<StringView> &values)
{
    if (keys.size() != values.size()) {
        auto errMsg = FormatString("Table [%s]: number of keys %zu is not equal to number of values %zu", key_.c_str(),
                                   keys.size(), values.size());
        return Status(StatusCode::K_DATA_INCONSISTENCY, errMsg);
    }

    if (!shareKey_ || !shareValue_) {
        return Status(StatusCode::K_RECOVERY_ERROR, FormatString("Table [%s] vector is null", key_.c_str()));
    }

    auto insertSize = keys.size();

    // @todo: Acquire Etcd lock here.
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);

    // @todo: fetch latest meta and update local size_
    auto oldSize = size_.fetch_add(insertSize);
    auto newSize = oldSize + insertSize;

    LOG(INFO) << FormatString("Table [%s] upscale to new size %llu", key_.c_str(), newSize);
    RETURN_IF_NOT_OK(shareKey_->Reserve(newSize, oldSize));
    RETURN_IF_NOT_OK(shareValue_->Reserve(newSize, oldSize));

    lock.unlock();
    // @todo: update local meta to remote
    // @todo: release Etcd lock.

    for (size_t i = 0; i < insertSize; i++) {
        RETURN_IF_NOT_OK(shareKey_->Set(i + oldSize, keys[i]));
        RETURN_IF_NOT_OK(shareValue_->Set(i + oldSize, values[i].data()));
        // LOG(INFO) << FormatString("Table %s set index %zu size %zu value %s", key_, i, values[i].size(),
        // std::string(values[i].data(), values[i].size()).c_str());
    }

    return Status::OK();
}

Status ShareMapObject::Lookup(const std::vector<uint64_t> &keys, std::vector<StringView> &buffers) const
{
    static constexpr size_t parallelThreshold = 128;
    static constexpr size_t parallism = 4;

    if (!shareKey_ || !shareValue_ || !shareIndex2VecIndex_) {
        return Status(StatusCode::K_RECOVERY_ERROR, FormatString("Table [%s] vector is null", key_.c_str()));
    }

    buffers.reserve(keys.size());
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);

    buffers.resize(keys.size());

    auto lookup = [&, this](size_t start, size_t end) {
        StringView keyBuffer;
        StringView valueBuffer;
        for (size_t i = start; i < end; i++) {
            size_t index = hash_index(keys[i]);
            // LOG(INFO) << FormatString("Key %llu is at %llu", keys[i], index);
            Status valueRes = shareValue_->Lookup(index, valueBuffer);
#if !defined(NDEBUG)
            Status keyRes = shareKey_->Lookup(index, keyBuffer);
            if (std::memcmp(keyBuffer.data(), &keys[i], sizeof(keys[i])) != 0) {
                LOG(ERROR) << FormatString("Lookup key mismatch, expected %llu, got %llu.", keys[i],
                                           *reinterpret_cast<const uint64_t *>(keyBuffer.data()));
            }
#endif
            buffers[i] = valueBuffer;
        }
        return Status::OK();
    };

    bool isParallel = keys.size() > parallelThreshold;
    if (!isParallel) {
        return lookup(0, keys.size());
    }

    return Parallel::ParallelFor<size_t>(0, keys.size(), lookup, 0, parallism);
}

Status ShareMapObject::BuildIndex()
{
    // @todo: Etcd lock
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);

    uint64_t version = version_.fetch_add(1);
    version += 1;
    auto size = size_.load();
    // RETURN_IF_NOT_OK(shareKey_->Reserve(size));
    // RETURN_IF_NOT_OK(shareValue_->Reserve(size));

    shareIndex2VecIndex_ =
        std::make_unique<ShareVectorObject>(GetVectorKey(SHARE_VEC_INDEX_SUFFIX, version), sizeof(uint64_t), kvClient_);
    RETURN_IF_NOT_OK(shareIndex2VecIndex_->Reserve(size, shareIndex2VecIndex_->vectorCapacity_));

    build_hash_index(size, version);

    // shareKey_->Publish();
    // shareValue_->Publish();
    // shareIndex2VecIndex_->Publish();

    RETURN_IF_NOT_OK(UpdateMeta(SerializeMeta(), false));
    return Status::OK();
}

std::string ShareMapObject::GetVectorKey(std::string_view suffix) const
{
    return std::string(key_).append(suffix);
}

std::string ShareMapObject::GetVectorKey(std::string_view suffix, uint64_t version) const
{
    return std::string(key_).append(suffix).append("-v" + std::to_string(version));
}

uint64_t ShareMapObject::hash_index(const uint64_t key) const
{
    uint64_t phfIndex = shareIndex_->lookup(key);
    // @[Warning]: check address is aligned to 8 Bytes?
    StringView buffer;
    Status status = shareIndex2VecIndex_->Lookup(phfIndex, buffer);

    if (!status.IsOk() || buffer.size() != sizeof(key)) {
        LOG(ERROR) << FormatString("Table %s: Calc key[%llu] hash index failed, detail: %s", key_.c_str(), key,
                                   status.ToString().c_str());
        return 0;
    }
    uint64_t vecIndex = *reinterpret_cast<const uint64_t *>(buffer.data());

    return vecIndex;
}

Status ShareMapObject::build_hash_index(size_t size, uint64_t version)
{
    std::vector<uint64_t> keys;
    RETURN_IF_NOT_OK(shareKey_->ExportToVector(keys, size));
    shareIndex_ = std::make_unique<PHF>(keys.size(), boomphf::range(keys.begin(), keys.end()), 1, 2.0, false);

    // export shareIndex to SHM
    export_phf(version);
    for (size_t i = 0; i < keys.size(); i++) {
        uint64_t phfIndex = shareIndex_->lookup(keys[i]);
        RETURN_IF_NOT_OK(shareIndex2VecIndex_->Set(phfIndex, &i));
    }

    return Status::OK();
}

Status ShareMapObject::export_phf(uint64_t version)
{
    std::stringstream ss;
    shareIndex_->save(ss);
    std::string buffer = ss.str();
    const auto &shm_key = GetVectorKey(SHARE_PHF_INDEX_SUFFIX, version);
    Optional<ShareBuffer> shareBuffer;
    Status status = shareObject_.Create(shm_key, buffer.size(), shareBuffer);  // @TODO check if is first time Create
    if (!status.IsOk()) {
        return status;
    }

    RETURN_IF_NOT_OK(shareBuffer.value().Write(/* offset = */ 0, buffer.data(), buffer.size()));
    shareObject_.Publish(shareBuffer.value());
    LOG(INFO) << FormatString("[Table %s] exporting BBHash to SHM %s", key_.c_str(), shm_key.c_str());

    return Status::OK();
}

Status ShareMapObject::import_phf(uint64_t version)
{
    shareIndex_ = std::make_unique<PHF>();
    const auto &shm_key = GetVectorKey(SHARE_PHF_INDEX_SUFFIX, version);
    Optional<ShareBuffer> shareBuffer;
    Status status = shareObject_.Import(shm_key, shareBuffer);
    if (!status.IsOk()) {
        LOG(ERROR) << FormatString("Table [%s] import bbhash [%s] failed", key_.c_str(), shm_key.c_str());
        return status;
    }
    std::string buffer(static_cast<const char *>(shareBuffer.value().Data()), shareBuffer.value().Size());
    std::stringstream ss(buffer);
    shareIndex_->load(ss);

    return Status::OK();
}

Status ShareMapObject::Restore(const std::string &serializedMeta)
{
    // 1. Unpack meta
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);

    RETURN_IF_NOT_OK(DeserializeMeta(serializedMeta));
    uint64_t version = version_.load();

    // 2. Construct ShareVectorObject and restore from ShareMemory
    shareKey_ = std::make_unique<ShareVectorObject>(GetVectorKey(SHARE_KEY_SUFFIX), kvClient_);
    shareValue_ = std::make_unique<ShareVectorObject>(GetVectorKey(SHARE_VALUE_SUFFIX), kvClient_);
    shareIndex2VecIndex_ =
        std::make_unique<ShareVectorObject>(GetVectorKey(SHARE_VEC_INDEX_SUFFIX, version), kvClient_);

    // uint64_t vectorCapacity_, uint64_t dimSize_
    auto size = size_.load();
    RETURN_IF_NOT_OK(shareKey_->RestoreShareObject(STORED_KEY_SIZE, size));
    RETURN_IF_NOT_OK(shareValue_->RestoreShareObject(dimSize_, size));
    RETURN_IF_NOT_OK(shareIndex2VecIndex_->RestoreShareObject(sizeof(uint64_t), size));

    // 3. Load BBHash
    RETURN_IF_NOT_OK(import_phf(version));

    return Status::OK();
}

std::string ShareMapObject::SerializeMeta() const
{
    // size_, version_, dimSize_, key_
    size_t metaSize = sizeof(uint64_t) * 3 + key_.length();
    std::string meta(metaSize, '\0');
    uint64_t size = size_.load();
    uint64_t version = version_.load();
    size_t offset = 0;
    std::memcpy(meta.data(), &size, sizeof(uint64_t));
    offset += sizeof(uint64_t);

    std::memcpy(meta.data() + offset, &version, sizeof(uint64_t));
    offset += sizeof(uint64_t);

    std::memcpy(meta.data() + offset, &dimSize_, sizeof(uint64_t));
    offset += sizeof(uint64_t);

    std::memcpy(meta.data() + offset, key_.data(), key_.length());

    return meta;
}

Status ShareMapObject::DeserializeMeta(const std::string &serializedMeta)
{
    // size_, version_, dimSize_, key_
    size_t metaSize = sizeof(uint64_t) * 3 + key_.length();

    if (serializedMeta.size() != metaSize) {
        auto errMsg = FormatString(
            "Key [%s] Meta is invalid. Get meta size %zu, expected %zu. Get meta key: %s, expected: %s.", key_.c_str(),
            serializedMeta.size(), metaSize,
            std::string(serializedMeta.data() + (metaSize - key_.length()), key_.length()).c_str(), key_.c_str());
        LOG(ERROR) << errMsg;
        return Status(StatusCode::K_INVALID, errMsg);
    }

    auto addr = serializedMeta.data();
    size_t offset = 0;
    uint64_t tempSize;
    uint64_t tempVersion;
    uint64_t tempDim;

    std::memcpy(&tempSize, addr + offset, sizeof(tempSize));
    size_.store(tempSize);
    offset += sizeof(tempSize);

    std::memcpy(&tempVersion, addr + offset, sizeof(tempVersion));
    version_.store(tempVersion);
    offset += sizeof(tempVersion);

    std::memcpy(&tempDim, addr + offset, sizeof(tempDim));
    dimSize_ = tempDim;

    LOG(INFO) << FormatString("ShareMapObject [%s] meta: size %llu version %llu dimsize %llu ", key_.c_str(), tempSize,
                              tempVersion, tempDim);

    if (std::string_view(serializedMeta.data() + (metaSize - key_.length()), key_.length()) != key_) {
        auto errMsg = FormatString(
            "Key [%s] Meta is invalid. Get meta size %zu, expected %zu. Get meta key: %s, expected: %s.", key_.c_str(),
            serializedMeta.size(), metaSize,
            std::string(serializedMeta.data() + (metaSize - key_.length()), key_.length()).c_str(), key_.c_str());
        LOG(ERROR) << errMsg;
        return Status(StatusCode::K_INVALID, errMsg);
    }

    return Status::OK();
}

// @TODO: [Optimize] use StringView to avoid data copy
Status ShareMapObject::QueryMeta(std::string &buffer)
{
    const auto &metaKey = GetVectorKey(META_SUFFIX);
    Status status = shareObject_.Import(metaKey, metaBuffer_);
    if (!status.IsOk()) {
        LOG(ERROR) << FormatString("Table [%s] query meta failed, err msg: %s", key_.c_str(), status.ToString());
        return status;
    }
    buffer = std::string(reinterpret_cast<const char *>(metaBuffer_->Data()), metaBuffer_->Size());

    return Status::OK();
}

Status ShareMapObject::UpdateMeta(const std::string &serializedMeta, bool isCreate)
{
    const auto &metaKey = GetVectorKey(META_SUFFIX);
    if (isCreate) {
        Status status = shareObject_.Create(metaKey, serializedMeta.size(), metaBuffer_);
        if (!status.IsOk()) {
            LOG(ERROR) << FormatString("Table [%s] create meta failed, err msg: %s", key_.c_str(), status.ToString());
            return status;
        }
        RETURN_IF_NOT_OK(metaBuffer_->Write(0, serializedMeta.data(), serializedMeta.size()));
        RETURN_IF_NOT_OK(shareObject_.Publish(metaBuffer_.value()));
    } else {
        if (!metaBuffer_) {
            Status status = shareObject_.Import(metaKey, metaBuffer_);
            if (!status.IsOk()) {
                LOG(ERROR) << FormatString("Table [%s] create meta failed, err msg: %s", key_.c_str(),
                                           status.ToString());
                return status;
            }
        }
        Status status = metaBuffer_->Write(0, serializedMeta.data(), serializedMeta.size());
        if (!status.IsOk()) {
            LOG(ERROR) << FormatString("Table [%s] update meta failed, err msg: %s", key_.c_str(), status.ToString());
            return status;
        }
        LOG(INFO) << FormatString("Table [%s] update meta success version %llu", key_.c_str(), version_.load());
    }

    return Status::OK();
}

Status ShareMapObject::SyncIfOutdated(uint64_t remoteVersion)
{
    uint64_t localVer = version_.load(std::memory_order_acquire);
    if (localVer >= remoteVersion) {
        return Status::OK();
    }

    LOG(INFO) << FormatString("[SyncThread %s] Detected update. Fetching new data...", key_.c_str());

    std::unique_lock<std::shared_mutex> lock(rw_mutex_);

    if (version_.load(std::memory_order_relaxed) >= remoteVersion) {
        return Status::OK();
    }

    // @TODO: Fetch Latest Data

    return Status::OK();
}

};  // namespace datasystem