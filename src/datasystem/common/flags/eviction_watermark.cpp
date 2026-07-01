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

#include "datasystem/common/flags/eviction_watermark.h"

#include "datasystem/common/flags/flags.h"

DS_DECLARE_double(eviction_high_watermark_ratio);
DS_DECLARE_double(eviction_low_watermark_ratio);
DS_DECLARE_double(spill_high_watermark_ratio);
DS_DECLARE_double(spill_low_watermark_ratio);

namespace datasystem {
namespace {
double g_evictionHighWaterFactor = 0.9;
double g_evictionLowWaterFactor = 0.8;
double g_spillHighWaterFactor = 0.8;
double g_spillLowWaterFactor = 0.6;
}  // namespace

void RefreshWatermarkFactors()
{
    g_evictionHighWaterFactor = FLAGS_eviction_high_watermark_ratio;
    g_evictionLowWaterFactor = FLAGS_eviction_low_watermark_ratio;
    g_spillHighWaterFactor = FLAGS_spill_high_watermark_ratio;
    g_spillLowWaterFactor = FLAGS_spill_low_watermark_ratio;
}

double GetEvictionHighWaterFactor()
{
    return g_evictionHighWaterFactor;
}

double GetEvictionLowWaterFactor()
{
    return g_evictionLowWaterFactor;
}

double GetSpillHighWaterFactor()
{
    return g_spillHighWaterFactor;
}

double GetSpillLowWaterFactor()
{
    return g_spillLowWaterFactor;
}

}  // namespace datasystem
