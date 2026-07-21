/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Coordination-backed object metadata reader.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_OBJECT_METADATA_COORDINATION_READER_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_OBJECT_METADATA_COORDINATION_READER_H

#include "datasystem/worker/object_cache/service/object_metadata_reader.h"

namespace datasystem {
namespace cluster {
class ICoordinationBackend;
}
namespace object_cache {

class CoordinationObjectMetadataReader final : public IObjectMetadataReader {
public:
    explicit CoordinationObjectMetadataReader(cluster::ICoordinationBackend *backend);
    ~CoordinationObjectMetadataReader() override = default;

    Status QueryObjectMetadata(const std::string &objectKey, int32_t timeoutMs,
                               master::QueryMetaInfoPb &queryMeta) override;

private:
    cluster::ICoordinationBackend *backend_;
};

}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_OBJECT_METADATA_COORDINATION_READER_H
