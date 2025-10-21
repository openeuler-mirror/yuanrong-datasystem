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
#ifndef DATASYSTEM_WORKER_HASH_RING_PB_UTILS_H
#define DATASYSTEM_WORKER_HASH_RING_PB_UTILS_H
#include "datasystem/protos/hash_ring.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace worker {
Status HashRingPbToJson(const HashRingPb &hashRing, std::string &jsonStr, bool format = false);
Status HashRingPbFromJson(const std::string &jsonStr, HashRingPb &hashRing);

Status WriteToJsonFile(const std::string &file, const HashRingPb &hashRing);
Status ReadFromJsonFile(const std::string &file, HashRingPb &hashRing);
}  // namespace worker
}  // namespace datasystem
#endif
