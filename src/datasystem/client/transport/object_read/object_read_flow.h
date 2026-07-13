/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

/** Description: Defines the multi-key object-read operation flow. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_OBJECT_READ_OBJECT_READ_FLOW_H
#define DATASYSTEM_CLIENT_TRANSPORT_OBJECT_READ_OBJECT_READ_FLOW_H

#include <memory>

#include "datasystem/client/transport/metadata/object_metadata_client.h"
#include "datasystem/client/transport/object_read/object_read_types.h"
#include "datasystem/client/transport/object_read/replica_reader.h"
#include "datasystem/common/util/thread_pool.h"

namespace datasystem {
namespace client {
class ObjectReadFlow {
public:
    ObjectReadFlow(std::shared_ptr<ObjectMetadataClient> metadata, std::shared_ptr<ReplicaReader> replicas,
                   std::shared_ptr<ThreadPool> taskPool);

    ~ObjectReadFlow() = default;

    /** @brief Execute grouped metadata lookup and per-key data reads. */
    Status Run(const ObjectReadRequest &request, ObjectReadResult &result);

private:
    std::shared_ptr<ObjectMetadataClient> metadata_;
    std::shared_ptr<ReplicaReader> replicas_;
    std::shared_ptr<ThreadPool> taskPool_;
};
}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_OBJECT_READ_OBJECT_READ_FLOW_H
