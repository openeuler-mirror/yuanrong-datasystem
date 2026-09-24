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
 * Description: Shared hash-ring token derivation contract.
 */
#ifndef DATASYSTEM_COMMON_UTIL_HASH_RING_TOKEN_H
#define DATASYSTEM_COMMON_UTIL_HASH_RING_TOKEN_H

#include <cstdint>
#include <string>
#include <vector>

namespace datasystem {

inline constexpr uint32_t MAX_HASH_RING_TOKEN_SEEDS = 10'000;
inline constexpr uint32_t MAX_HASH_RING_TOKENS_PER_MEMBER = 4'096;
// Balanced placement searches this many seed candidates per token; must stay within the probe budget.
// Landing error is ring/(2K) against ideal ring/(members*tokens), so quality holds while members*tokens
// stays well below 2K; K=1000 keeps the validated parity envelope (<=1.1x up to 25 members at the 32-token
// default) at a quarter of the planning cost, and keeps the serialized topology small at large ring sizes.
inline constexpr uint32_t BALANCED_PLACEMENT_SEED_CANDIDATES = 1'000;

uint32_t MakeHashRingToken(const std::string &address, uint32_t index, uint32_t seed);

void MakeHashRingTokens(const std::string &address, const std::vector<uint32_t> &seeds,
                        std::vector<uint32_t> &tokens);

// Derives MakeHashRingToken(address, index, seed) for seed in [0, candidateSeeds) with one shared buffer.
void MakeHashRingTokenCandidates(const std::string &address, uint32_t index, uint32_t candidateSeeds,
                                 std::vector<uint32_t> &tokens);

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_UTIL_HASH_RING_TOKEN_H
