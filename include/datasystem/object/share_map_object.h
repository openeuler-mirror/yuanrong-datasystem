#pragma once

#include <string>
#include <vector>
#include <atomic>

#include "datasystem/utils/status.h"
#include "datasystem/utils/string_view.h"
#include "datasystem/object/buffer.h"
#include "datasystem/object/share_object.h"
#include "datasystem/object/share_vector_object.h"
#include "datasystem/object/BooPHF.h"

namespace datasystem {
class __attribute((visibility("default"))) ShareMapObject {
public:
    ShareMapObject(const std::string &bucketKey, std::shared_ptr<KVClient> kvClient, uint64_t dimSize = 0);

    Status Insert(const std::vector<uint64_t> &keys, const std::vector<StringView> &values);

    Status Lookup(const std::vector<uint64_t> &keys, std::vector<StringView> &buffers) const;

    Status BuildIndex();

private:
    uint64_t hash_index(const uint64_t key) const;

    Status build_hash_index(size_t size, uint64_t version);

    Status export_phf(uint64_t version);
    Status import_phf(uint64_t version);

    Status Restore(const std::string& serializedMeta);
    std::string SerializeMeta() const;
    Status DeserializeMeta(const std::string& serializedMeta);
    Status QueryMeta(std::string& buffer);
    Status UpdateMeta(const std::string& buffer, bool isCreate);
    Status SyncIfOutdated(uint64_t version);

    std::string GetVectorKey(std::string_view suffix) const;
    std::string GetVectorKey(std::string_view suffix, uint64_t version) const;

    static constexpr std::string_view SHARE_KEY_SUFFIX = "-map-share_key";
    static constexpr std::string_view SHARE_VALUE_SUFFIX = "-map-share_value";
    static constexpr std::string_view SHARE_PHF_INDEX_SUFFIX = "-map-share_phf_index";
    static constexpr std::string_view SHARE_VEC_INDEX_SUFFIX = "-map-share_vec_index";
    static constexpr std::string_view META_SUFFIX = "-meta";

    static constexpr uint64_t STORED_KEY_SIZE = sizeof(uint64_t);

    using HasherType = boomphf::SingleHashFunctor<uint64_t>;
    using PHF = boomphf::mphf<uint64_t, HasherType>;

    std::string key_;
    std::unique_ptr<ShareVectorObject> shareKey_;
    std::unique_ptr<ShareVectorObject> shareValue_;
    // 2-level index
    std::unique_ptr<PHF> shareIndex_;
    std::unique_ptr<ShareVectorObject> shareIndex2VecIndex_;

    std::shared_ptr<KVClient> kvClient_;
    ShareObject shareObject_;
    uint64_t dimSize_;
    std::atomic<uint64_t> size_ {0};
    std::atomic<uint64_t> version_ {12345};

    Optional<ShareBuffer> metaBuffer_;

    mutable std::shared_mutex rw_mutex_;
};

}  // namespace datasystem
