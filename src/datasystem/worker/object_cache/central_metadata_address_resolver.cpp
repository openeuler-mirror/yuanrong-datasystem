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
 * Description: Centralized metadata endpoint resolver.
 */
#include "datasystem/worker/object_cache/central_metadata_address_resolver.h"

#include "datasystem/cluster/coordination_backend/coordination_backend.h"
#include "datasystem/common/kvstore/coordination_keys.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace object_cache {

CentralMetadataAddressResolver::CentralMetadataAddressResolver(cluster::ICoordinationBackend &backend)
    : backend_(backend)
{
}

Status CentralMetadataAddressResolver::EnsureTable()
{
    RETURN_IF_NOT_OK_EXCEPT(
        backend_.CreateTable(COORDINATION_MASTER_ADDRESS_TABLE, COORDINATION_MASTER_ADDRESS_TABLE), K_DUPLICATED);
    return Status::OK();
}

Status CentralMetadataAddressResolver::ClaimOrRead(const std::string &localAddress, std::string &address)
{
    auto rc = backend_.CAS(COORDINATION_MASTER_ADDRESS_TABLE, COORDINATION_MASTER_ADDRESS_KEY, "", localAddress);
    if (rc.IsOk()) {
        address = localAddress;
        return Status::OK();
    }
    return backend_.Get(COORDINATION_MASTER_ADDRESS_TABLE, COORDINATION_MASTER_ADDRESS_KEY, address);
}

}  // namespace object_cache
}  // namespace datasystem
