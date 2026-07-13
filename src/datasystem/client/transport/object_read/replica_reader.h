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

/** Description: Defines fixed-location replica polling for object reads. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_OBJECT_READ_REPLICA_READER_H
#define DATASYSTEM_CLIENT_TRANSPORT_OBJECT_READ_REPLICA_READER_H

#include <memory>

#include "datasystem/client/transport/common/deadline_retry.h"
#include "datasystem/client/transport/data_plane/data_plane_executor.h"
#include "datasystem/client/transport/object_read/object_read_types.h"
#include "datasystem/protos/master_object.pb.h"

namespace datasystem {
namespace client {
class ReplicaReader {
public:
    ReplicaReader(std::shared_ptr<DataPlaneExecutor> executor, std::shared_ptr<DeadlineRetry> retry);
    virtual ~ReplicaReader() = default;

    /** @brief Poll the fixed metadata locations until one read succeeds or the API deadline expires. */
    virtual Status Read(const master::ObjectLocationInfoPb &location, ObjectReadItemResult &result);

private:
    bool IsRetryableLocationError(const Status &status) const;

    std::shared_ptr<DataPlaneExecutor> executor_;
    std::shared_ptr<DeadlineRetry> retry_;
};
}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_OBJECT_READ_REPLICA_READER_H
