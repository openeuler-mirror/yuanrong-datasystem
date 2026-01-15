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
#ifndef DATASYSTEM_CACHE_HIT_INFO_H
#define DATASYSTEM_CACHE_HIT_INFO_H
#include <string>
#include <atomic>
namespace datasystem {
namespace object_cache {
class CacheHitInfo {
public:
    CacheHitInfo() = default;
    ~CacheHitInfo() = default;
    static CacheHitInfo &Instance();
    /**
     * @brief Increase the mem hit num.
     * @param[in] n  The increase num, default is 1.
     */
    void IncMemHit(size_t n = 1);

    /**
     * @brief Increase the mem hit num.
     * @param[in] n  The increase num, default is 1.
     */
    void IncDiskHit(size_t n = 1);

    /**
     * @brief Increase the l2 hit num.
     * @param[in] n  The increase num, default is 1.
     */
    void IncL2Hit(size_t n = 1);
    /**
     * @brief Increase the remote hit num.
     * @param[in] n  The increase num, default is 1.
     */

    void IncRemoteHit(size_t n = 1);
    /**
     * @brief Increase the miss hit num.
     * @param[in] n  The increase num, default is 1.
     */
    void IncMissHit(size_t n = 1);

    /**
     * @brief Get the formatted hit info string, the string is formatted as
     * memHitNum_/diskHitNum_/l2HitNum_/remoteHitNum_/missNum_.
     * @return The formatted hit info.
     */
    std::string GetHitInfo();

private:
    std::atomic<uint64_t> memHitNum_ = 0;
    std::atomic<uint64_t> diskHitNum_ = 0;
    std::atomic<uint64_t> l2HitNum_ = 0;
    std::atomic<uint64_t> remoteHitNum_ = 0;
    std::atomic<uint64_t> missNum_ = 0;
};
}  // namespace object_cache
}  // namespace datasystem
#endif