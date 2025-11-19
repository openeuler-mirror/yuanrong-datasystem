/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Datasystem all client management.
 */

#ifndef DATASYSTEM_DATASYSTEM_H
#define DATASYSTEM_DATASYSTEM_H

#include <memory>

#include "datasystem/hetero_client.h"
#include "datasystem/object_client.h"
#include "datasystem/kv_client.h"
#include "datasystem/stream_client.h"
#include "datasystem/utils/status.h"

namespace datasystem {
class __attribute((visibility("default"))) DsClient {
public:
    /// \brief Construct DsClient that can use object cache, kv cache, and hetero cache at the same time.
    ///
    /// \param[in] connectOptions the client connect options.
    explicit DsClient(const ConnectOptions &connectOptions = {});

    ~DsClient() = default;

    /// \brief Init the ds client. Initialize the three cache capabilities at the same time.
    ///
    /// \return K_OK on success; the error code otherwise.
    Status Init();

    /// \brief Shutdown the ds client.
    ///
    /// \return K_OK on success; the error code otherwise.
    Status ShutDown();

    /// \brief Obtains the API of the kv cache.
    ///
    /// \return KVClient for kv cache.
    std::shared_ptr<KVClient> KV();

    /// \brief Obtains the API of the hetero cache.
    ///
    /// \return HeteroClient for hetero cache.
    std::shared_ptr<HeteroClient> Hetero();

    /// \brief Obtains the API of the object cache.
    ///
    /// \return ObjectClient for object cache.
    std::shared_ptr<ObjectClient> Object();

private:
    std::shared_ptr<KVClient> kvClient_;
    std::shared_ptr<HeteroClient> heteroClient_;
    std::shared_ptr<ObjectClient> objectClient_;
};
}  // namespace datasystem

#endif  // DATASYSTEM_DATASYSTEM_H
