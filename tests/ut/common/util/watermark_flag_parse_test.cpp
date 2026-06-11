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
 * Description: Parsing-layer tests for evict/spill watermark gflags pair validation.
 */
#include <cstdio>
#include <string>

#include "datasystem/common/flags/flags.h"
#include "datasystem/utils/embedded_config.h"
#include "datasystem/worker/worker_update_flag_check.h"
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

void ParseTwoFlagsInOrder(const char *highFlag, const char *highValue, const char *lowFlag, const char *lowValue,
                          bool highFirst)
{
    char arg0[] = "./program";
    char highArg[128];
    char lowArg[128];
    (void)snprintf(highArg, sizeof(highArg), "--%s=%s", highFlag, highValue);
    (void)snprintf(lowArg, sizeof(lowArg), "--%s=%s", lowFlag, lowValue);
    char *argvHighFirst[] = { arg0, highArg, lowArg };
    char *argvLowFirst[] = { arg0, lowArg, highArg };
    if (highFirst) {
        ParseCommandLineFlags(3, argvHighFirst);
    } else {
        ParseCommandLineFlags(3, argvLowFirst);
    }
}

void ExpectParsedPairValid(const char *highFlag, const char *highValue, const char *lowFlag, const char *lowValue,
                           bool highFirst, double expectedHigh, double expectedLow)
{
    ParseTwoFlagsInOrder(highFlag, highValue, lowFlag, lowValue, highFirst);
    EXPECT_TRUE(ValidateWatermarkFlags());
    if (std::string(highFlag) == "eviction_high_watermark_ratio") {
        EXPECT_DOUBLE_EQ(FLAGS_eviction_high_watermark_ratio, expectedHigh);
        EXPECT_DOUBLE_EQ(FLAGS_eviction_low_watermark_ratio, expectedLow);
    } else {
        EXPECT_DOUBLE_EQ(FLAGS_spill_high_watermark_ratio, expectedHigh);
        EXPECT_DOUBLE_EQ(FLAGS_spill_low_watermark_ratio, expectedLow);
    }
}
}  // namespace

class WatermarkFlagParseTest : public CommonTest {
protected:
    void SetUp() override
    {
        saved_ = SaveWatermarkFlags();
    }

    void TearDown() override
    {
        RestoreWatermarkFlags(saved_);
    }

    WatermarkFlagSnapshot saved_;
};

TEST_F(WatermarkFlagParseTest, EvictionBelowDefaultHighBeforeLow)
{
    ExpectParsedPairValid("eviction_high_watermark_ratio", "0.75", "eviction_low_watermark_ratio", "0.70", true, 0.75,
                          0.70);
}

TEST_F(WatermarkFlagParseTest, EvictionBelowDefaultLowBeforeHigh)
{
    ExpectParsedPairValid("eviction_high_watermark_ratio", "0.75", "eviction_low_watermark_ratio", "0.70", false, 0.75,
                          0.70);
}

TEST_F(WatermarkFlagParseTest, EvictionAboveDefaultHighBeforeLow)
{
    ExpectParsedPairValid("eviction_high_watermark_ratio", "0.95", "eviction_low_watermark_ratio", "0.85", true, 0.95,
                          0.85);
}

TEST_F(WatermarkFlagParseTest, EvictionAboveDefaultLowBeforeHigh)
{
    ExpectParsedPairValid("eviction_high_watermark_ratio", "0.95", "eviction_low_watermark_ratio", "0.85", false, 0.95,
                          0.85);
}

TEST_F(WatermarkFlagParseTest, SpillBelowDefaultHighBeforeLow)
{
    ExpectParsedPairValid("spill_high_watermark_ratio", "0.70", "spill_low_watermark_ratio", "0.55", true, 0.70, 0.55);
}

TEST_F(WatermarkFlagParseTest, SpillBelowDefaultLowBeforeHigh)
{
    ExpectParsedPairValid("spill_high_watermark_ratio", "0.70", "spill_low_watermark_ratio", "0.55", false, 0.70, 0.55);
}

TEST_F(WatermarkFlagParseTest, SpillAboveDefaultHighBeforeLow)
{
    ExpectParsedPairValid("spill_high_watermark_ratio", "0.90", "spill_low_watermark_ratio", "0.75", true, 0.90, 0.75);
}

TEST_F(WatermarkFlagParseTest, SpillAboveDefaultLowBeforeHigh)
{
    ExpectParsedPairValid("spill_high_watermark_ratio", "0.90", "spill_low_watermark_ratio", "0.75", false, 0.90, 0.75);
}

TEST_F(WatermarkFlagParseTest, AllWatermarksBelowDefaultMixedArgOrder)
{
    char arg0[] = "./program";
    char spillLow[] = "--spill_low_watermark_ratio=0.55";
    char evictionHigh[] = "--eviction_high_watermark_ratio=0.75";
    char spillHigh[] = "--spill_high_watermark_ratio=0.70";
    char evictionLow[] = "--eviction_low_watermark_ratio=0.70";
    char *argv[] = { arg0, spillLow, evictionHigh, spillHigh, evictionLow };
    ParseCommandLineFlags(5, argv);
    EXPECT_DOUBLE_EQ(FLAGS_eviction_high_watermark_ratio, 0.75);
    EXPECT_DOUBLE_EQ(FLAGS_eviction_low_watermark_ratio, 0.70);
    EXPECT_DOUBLE_EQ(FLAGS_spill_high_watermark_ratio, 0.70);
    EXPECT_DOUBLE_EQ(FLAGS_spill_low_watermark_ratio, 0.55);
    EXPECT_TRUE(ValidateWatermarkFlags());
}

TEST_F(WatermarkFlagParseTest, InvalidEvictionPairFailsAfterParse)
{
    ParseTwoFlagsInOrder("eviction_high_watermark_ratio", "0.80", "eviction_low_watermark_ratio", "0.85", true);
    EXPECT_DOUBLE_EQ(FLAGS_eviction_high_watermark_ratio, 0.80);
    EXPECT_DOUBLE_EQ(FLAGS_eviction_low_watermark_ratio, 0.85);
    EXPECT_FALSE(ValidateWatermarkFlags());
}

TEST_F(WatermarkFlagParseTest, EmbeddedConfigEvictionBelowDefaultPairValid)
{
    EmbeddedConfig config;
    config.SetArgs({ { "eviction_high_watermark_ratio", "0.75" }, { "eviction_low_watermark_ratio", "0.70" } });
    std::string errMsg;
    EXPECT_TRUE(ParseCommandLineFlags(config, errMsg));
    EXPECT_TRUE(errMsg.empty());
    EXPECT_DOUBLE_EQ(FLAGS_eviction_high_watermark_ratio, 0.75);
    EXPECT_DOUBLE_EQ(FLAGS_eviction_low_watermark_ratio, 0.70);
    EXPECT_TRUE(ValidateWatermarkFlags());
}
}  // namespace ut
}  // namespace datasystem
