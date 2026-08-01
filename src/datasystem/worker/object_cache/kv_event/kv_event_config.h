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

/**
 * Description: KV event publisher configuration.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_KV_EVENT_CONFIG_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_KV_EVENT_CONFIG_H

#include <cstdint>
#include <string>

namespace datasystem {
namespace object_cache {

struct KvEventConfig {
    // Whether to enable KV event publishing. Disabled publisher is a no-op.
    bool enabled{ false };

    // ZMQ PUB bind endpoint, for example "tcp://0.0.0.0:5557".
    std::string bindEndpoint;

    // Model name in the event envelope. Empty string is encoded as null.
    std::string modelName;

    // Backend identity in the event envelope.
    std::string backendId;

    // Tenant id in the event envelope; used as fallback when object key does not carry a tenant.
    std::string tenantId{ "default" };

    // Hash namespace salt in the event envelope. Empty string is encoded as null.
    std::string additionalSalt;

    // LoRA name in the event envelope. Empty string is encoded as null.
    std::string loraName;

    // KV block size in the event envelope. 0 is encoded as null.
    uint32_t blockSize{ 0 };

    // Data parallel rank in the event envelope and payload wrapper.
    uint32_t dpRank{ 0 };

    // Whether to emit vLLM/SGLang compatibility fields: type, block_hashes, parent_block_hash.
    bool emitLegacyCompatFields{ true };

    // Bounded pending-event queue capacity. Additional events are dropped.
    uint32_t queueCapacity{ 65536 };
};

KvEventConfig BuildKvEventConfigFromJsonString(const std::string &jsonConfig);

}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_KV_EVENT_CONFIG_H
