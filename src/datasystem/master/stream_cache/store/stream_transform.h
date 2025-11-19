/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Interface to search stream meta from RocksDB.
 */
#ifndef DATASYSTEM_MASTER_STREAM_CACHE_STORE_STREAM_TRANSFORM_H
#define DATASYSTEM_MASTER_STREAM_CACHE_STORE_STREAM_TRANSFORM_H

#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"

#include "datasystem/common/log/log.h"

namespace datasystem {
namespace master {
namespace stream_cache {
static constexpr char PREFIX_SPLITTER[] = "_";
class StreamTransform : public rocksdb::SliceTransform {
public:
    StreamTransform() = default;

    const char *Name() const override
    {
        return "StreamTransform";
    }

    rocksdb::Slice Transform(const rocksdb::Slice &src) const override
    {
        DCHECK(InDomain(src));
        return GetPrefix(src);
    }

    bool InDomain(const rocksdb::Slice &src) const override
    {
        return !GetPrefix(src).empty();
    }

    bool SameResultWhenAppended(const rocksdb::Slice &prefix) const override
    {
        return InDomain(prefix);
    }

private:
    static rocksdb::Slice GetPrefix(const rocksdb::Slice &src)
    {
        std::string splitter(PREFIX_SPLITTER);
        std::string::size_type sz =
            std::find_end(src.data(), src.data() + src.size(), splitter.begin(), splitter.end()) - src.data();
        return { src.data(), sz };
    }
};
}  // namespace stream_cache
}  // namespace master
}  // namespace datasystem
#endif