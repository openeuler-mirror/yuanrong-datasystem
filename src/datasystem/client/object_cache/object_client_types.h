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

#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_OBJECT_CLIENT_TYPES_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_OBJECT_CLIENT_TYPES_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <bthread/mutex.h>

#include "datasystem/client/object_cache/client_worker_api/iclient_worker_api.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/utils/string_view.h"

namespace datasystem {
namespace object_cache {

enum class SetFailureStage { CREATE, TRANSFER, PUBLISH };

struct SetRouteContext {
    HostPort worker;
    std::shared_ptr<IClientWorkerApi> directWorkerApi;
    std::unique_ptr<Raii> invokeGuard;
};

struct MSetRouteGroup {
    HostPort worker;
    std::vector<std::string> keys;
    std::vector<StringView> values;
};

struct ShmRecoveryState {
    enum class Stage : uint8_t {
        IDLE = 0,
        CLEANUP_REQUIRED,
        REGISTER_REQUIRED,
        REBUILD_REQUIRED,
    };

    bthread::Mutex mutex;
    Stage stage{ Stage::IDLE };
};

}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_OBJECT_CACHE_OBJECT_CLIENT_TYPES_H
