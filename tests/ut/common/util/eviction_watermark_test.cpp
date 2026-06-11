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
 * Description: Unit tests for evict/spill watermark gflags and accessors.
 */
#include "datasystem/common/util/gflag/eviction_watermark.h"

#include "datasystem/common/flags/flags.h"
#include "ut/common.h"

DS_DECLARE_double(eviction_high_watermark_ratio);
DS_DECLARE_double(eviction_low_watermark_ratio);
DS_DECLARE_double(spill_high_watermark_ratio);
DS_DECLARE_double(spill_low_watermark_ratio);

namespace datasystem {
namespace ut {
namespace {
struct WatermarkFlagSnapshot {
    double evictionHigh;
    double evictionLow;
    double spillHigh;
    double spillLow;
};

void RestoreWatermarkFlags(const WatermarkFlagSnapshot &snapshot)
{
    FLAGS_eviction_high_watermark_ratio = snapshot.evictionHigh;
    FLAGS_eviction_low_watermark_ratio = snapshot.evictionLow;
    FLAGS_spill_high_watermark_ratio = snapshot.spillHigh;
    FLAGS_spill_low_watermark_ratio = snapshot.spillLow;
}

WatermarkFlagSnapshot SaveWatermarkFlags()
{
    return WatermarkFlagSnapshot{ FLAGS_eviction_high_watermark_ratio, FLAGS_eviction_low_watermark_ratio,
                                  FLAGS_spill_high_watermark_ratio, FLAGS_spill_low_watermark_ratio };
}
}  // namespace

class EvictionWatermarkTest : public CommonTest {};

TEST_F(EvictionWatermarkTest, DefaultEvictionFactorsMatchLegacyConstants)
{
    auto saved = SaveWatermarkFlags();
    FLAGS_eviction_high_watermark_ratio = 0.9;
    FLAGS_eviction_low_watermark_ratio = 0.8;
    RefreshWatermarkFactors();
    EXPECT_DOUBLE_EQ(GetEvictionHighWaterFactor(), 0.9);
    EXPECT_DOUBLE_EQ(GetEvictionLowWaterFactor(), 0.8);
    RestoreWatermarkFlags(saved);
    RefreshWatermarkFactors();
}

TEST_F(EvictionWatermarkTest, EvictionFactorsFollowFlagRatio)
{
    auto saved = SaveWatermarkFlags();
    FLAGS_eviction_high_watermark_ratio = 0.85;
    FLAGS_eviction_low_watermark_ratio = 0.70;
    RefreshWatermarkFactors();
    EXPECT_DOUBLE_EQ(GetEvictionHighWaterFactor(), 0.85);
    EXPECT_DOUBLE_EQ(GetEvictionLowWaterFactor(), 0.70);
    RestoreWatermarkFlags(saved);
    RefreshWatermarkFactors();
}

TEST_F(EvictionWatermarkTest, DefaultSpillFactorsMatchLegacyConstants)
{
    auto saved = SaveWatermarkFlags();
    FLAGS_spill_high_watermark_ratio = 0.8;
    FLAGS_spill_low_watermark_ratio = 0.6;
    RefreshWatermarkFactors();
    EXPECT_DOUBLE_EQ(GetSpillHighWaterFactor(), 0.8);
    EXPECT_DOUBLE_EQ(GetSpillLowWaterFactor(), 0.6);
    RestoreWatermarkFlags(saved);
    RefreshWatermarkFactors();
}

TEST_F(EvictionWatermarkTest, SpillFactorsFollowFlagRatio)
{
    auto saved = SaveWatermarkFlags();
    FLAGS_spill_high_watermark_ratio = 0.75;
    FLAGS_spill_low_watermark_ratio = 0.55;
    RefreshWatermarkFactors();
    EXPECT_DOUBLE_EQ(GetSpillHighWaterFactor(), 0.75);
    EXPECT_DOUBLE_EQ(GetSpillLowWaterFactor(), 0.55);
    RestoreWatermarkFlags(saved);
    RefreshWatermarkFactors();
}

TEST_F(EvictionWatermarkTest, ActiveSpillHwmThresholdUsesHighFactorNotLowFactor)
{
    auto saved = SaveWatermarkFlags();
    FLAGS_spill_high_watermark_ratio = 0.9;
    FLAGS_spill_low_watermark_ratio = 0.55;
    RefreshWatermarkFactors();
    EXPECT_GT(GetSpillHighWaterFactor(), GetSpillLowWaterFactor());
    const uint64_t spillLimit = 1000;
    const size_t usageAt60Percent = 600;
    EXPECT_FALSE(usageAt60Percent >= spillLimit * GetSpillHighWaterFactor());
    EXPECT_TRUE(usageAt60Percent >= spillLimit * GetSpillLowWaterFactor());
    RestoreWatermarkFlags(saved);
    RefreshWatermarkFactors();
}
}  // namespace ut
}  // namespace datasystem
