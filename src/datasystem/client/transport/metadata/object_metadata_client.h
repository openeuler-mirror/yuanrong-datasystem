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

/** Description: Defines batched object metadata access used by transport operation flows. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_METADATA_OBJECT_METADATA_CLIENT_H
#define DATASYSTEM_CLIENT_TRANSPORT_METADATA_OBJECT_METADATA_CLIENT_H

#include <memory>
#include <string>
#include <vector>

#include "datasystem/client/transport/common/deadline_retry.h"
#include "datasystem/client/transport/data_plane/data_plane_manager.h"
#include "datasystem/protos/master_object.pb.h"

namespace datasystem {
namespace client {
/** @brief Mutable metadata state for one object-read item. */
struct ObjectMetadataItem {
    std::string objectKey;
    Status status = Status(K_NOT_READY, "Object metadata is not resolved");
    master::ObjectLocationInfoPb location;
};

/** @brief Non-owning metadata batch; pointed items must outlive the query. */
using ObjectMetadataBatch = std::vector<ObjectMetadataItem *>;

class ObjectMetadataClient {
public:
    ObjectMetadataClient(std::shared_ptr<DataPlaneManager> manager, std::shared_ptr<DeadlineRetry> retry);
    virtual ~ObjectMetadataClient() = default;

    /**
     * @brief Query a metadata-owner group and resolve one redirect layer.
     * @param[in] address Initial metadata owner.
     * @param[in,out] items Ordered object metadata states belonging to address.
     * @return K_OK after every item has an independent result; a group-wide error otherwise.
     */
    virtual Status QueryAndGet(const HostPort &address, const ObjectMetadataBatch &items);

private:
    Status QueryWithRetry(const HostPort &address, const ObjectMetadataBatch &items, bool allowRedirect,
                          master::QueryAndGetRspPb &response);
    Status ResolveRedirectBatch(const HostPort &address, const ObjectMetadataBatch &items);
    Status InvokeQueryAndGet(const HostPort &address, master::QueryAndGetReqPb &request,
                             master::QueryAndGetRspPb &response);
    Status ApplyLocations(const ObjectMetadataBatch &items, const master::QueryAndGetRspPb &response) const;

    std::shared_ptr<DataPlaneManager> manager_;
    std::shared_ptr<DeadlineRetry> retry_;
};
}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_METADATA_OBJECT_METADATA_CLIENT_H
