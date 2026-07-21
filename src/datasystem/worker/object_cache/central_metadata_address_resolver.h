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
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_CENTRAL_METADATA_ADDRESS_RESOLVER_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_CENTRAL_METADATA_ADDRESS_RESOLVER_H

#include <string>

#include "datasystem/utils/status.h"

namespace datasystem {
namespace cluster {
class ICoordinationBackend;
}
namespace object_cache {

class CentralMetadataAddressResolver {
public:
    explicit CentralMetadataAddressResolver(cluster::ICoordinationBackend &backend);
    ~CentralMetadataAddressResolver() = default;

    CentralMetadataAddressResolver(const CentralMetadataAddressResolver &) = delete;
    CentralMetadataAddressResolver &operator=(const CentralMetadataAddressResolver &) = delete;

    Status EnsureTable();
    Status ClaimOrRead(const std::string &localAddress, std::string &address);

private:
    cluster::ICoordinationBackend &backend_;
};

}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_CENTRAL_METADATA_ADDRESS_RESOLVER_H
