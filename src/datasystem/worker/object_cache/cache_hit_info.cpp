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

#include "datasystem/worker/object_cache/cache_hit_info.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/inject/inject_point.h"
namespace datasystem {
namespace object_cache {
void CacheHitInfo::IncMemHit(size_t n)
{
    memHitNum_.fetch_add(n);
}
void CacheHitInfo::IncDiskHit(size_t n)
{
    diskHitNum_.fetch_add(n);
}
void CacheHitInfo::IncL2Hit(size_t n)
{
    l2HitNum_.fetch_add(n);
}
void CacheHitInfo::IncRemoteHit(size_t n)
{
    remoteHitNum_.fetch_add(n);
}
void CacheHitInfo::IncMissHit(size_t n)
{
    missNum_.fetch_add(n);
}
std::string CacheHitInfo::GetHitInfo()
{
    auto res = FormatString("%ld/%ld/%ld/%ld/%ld", memHitNum_, diskHitNum_, l2HitNum_, remoteHitNum_, missNum_);
    INJECT_POINT("hitinfo.prefix", [&res]() {
        res = "hit_info:" + res;
        return res;
    });
    return res;
}
}  // namespace object_cache
}  // namespace datasystem