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

DS_DECLARE_uint32(eviction_high_watermark_percent);
DS_DECLARE_uint32(eviction_low_watermark_percent);
DS_DECLARE_uint32(spill_high_watermark_percent);
DS_DECLARE_uint32(spill_low_watermark_percent);

namespace datasystem {
namespace ut {
namespace {
struct WatermarkFlagSnapshot {
    uint32_t evictionHigh;
    uint32_t evictionLow;
    uint32_t spillHigh;
    uint32_t spillLow;
};

void RestoreWatermarkFlags(const WatermarkFlagSnapshot &snapshot)
{
    FLAGS_eviction_high_watermark_percent = snapshot.evictionHigh;
    FLAGS_eviction_low_watermark_percent = snapshot.evictionLow;
    FLAGS_spill_high_watermark_percent = snapshot.spillHigh;
    FLAGS_spill_low_watermark_percent = snapshot.spillLow;
}

WatermarkFlagSnapshot SaveWatermarkFlags()
{
    return WatermarkFlagSnapshot{ FLAGS_eviction_high_watermark_percent, FLAGS_eviction_low_watermark_percent,
                                  FLAGS_spill_high_watermark_percent, FLAGS_spill_low_watermark_percent };
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
                           bool highFirst, uint32_t expectedHigh, uint32_t expectedLow)
{
    ParseTwoFlagsInOrder(highFlag, highValue, lowFlag, lowValue, highFirst);
    EXPECT_TRUE(ValidateWatermarkFlags());
    if (std::string(highFlag) == "eviction_high_watermark_percent") {
        EXPECT_EQ(FLAGS_eviction_high_watermark_percent, expectedHigh);
        EXPECT_EQ(FLAGS_eviction_low_watermark_percent, expectedLow);
    } else {
        EXPECT_EQ(FLAGS_spill_high_watermark_percent, expectedHigh);
        EXPECT_EQ(FLAGS_spill_low_watermark_percent, expectedLow);
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
    ExpectParsedPairValid("eviction_high_watermark_percent", "75", "eviction_low_watermark_percent", "70", true, 75,
                          70);
}

TEST_F(WatermarkFlagParseTest, EvictionBelowDefaultLowBeforeHigh)
{
    ExpectParsedPairValid("eviction_high_watermark_percent", "75", "eviction_low_watermark_percent", "70", false, 75,
                          70);
}

TEST_F(WatermarkFlagParseTest, EvictionAboveDefaultHighBeforeLow)
{
    ExpectParsedPairValid("eviction_high_watermark_percent", "95", "eviction_low_watermark_percent", "85", true, 95,
                          85);
}

TEST_F(WatermarkFlagParseTest, EvictionAboveDefaultLowBeforeHigh)
{
    ExpectParsedPairValid("eviction_high_watermark_percent", "95", "eviction_low_watermark_percent", "85", false, 95,
                          85);
}

TEST_F(WatermarkFlagParseTest, SpillBelowDefaultHighBeforeLow)
{
    ExpectParsedPairValid("spill_high_watermark_percent", "70", "spill_low_watermark_percent", "55", true, 70, 55);
}

TEST_F(WatermarkFlagParseTest, SpillBelowDefaultLowBeforeHigh)
{
    ExpectParsedPairValid("spill_high_watermark_percent", "70", "spill_low_watermark_percent", "55", false, 70, 55);
}

TEST_F(WatermarkFlagParseTest, SpillAboveDefaultHighBeforeLow)
{
    ExpectParsedPairValid("spill_high_watermark_percent", "90", "spill_low_watermark_percent", "75", true, 90, 75);
}

TEST_F(WatermarkFlagParseTest, SpillAboveDefaultLowBeforeHigh)
{
    ExpectParsedPairValid("spill_high_watermark_percent", "90", "spill_low_watermark_percent", "75", false, 90, 75);
}

TEST_F(WatermarkFlagParseTest, AllWatermarksBelowDefaultMixedArgOrder)
{
    char arg0[] = "./program";
    char spillLow[] = "--spill_low_watermark_percent=55";
    char evictionHigh[] = "--eviction_high_watermark_percent=75";
    char spillHigh[] = "--spill_high_watermark_percent=70";
    char evictionLow[] = "--eviction_low_watermark_percent=70";
    char *argv[] = { arg0, spillLow, evictionHigh, spillHigh, evictionLow };
    ParseCommandLineFlags(5, argv);
    EXPECT_EQ(FLAGS_eviction_high_watermark_percent, 75u);
    EXPECT_EQ(FLAGS_eviction_low_watermark_percent, 70u);
    EXPECT_EQ(FLAGS_spill_high_watermark_percent, 70u);
    EXPECT_EQ(FLAGS_spill_low_watermark_percent, 55u);
    EXPECT_TRUE(ValidateWatermarkFlags());
}

TEST_F(WatermarkFlagParseTest, InvalidEvictionPairFailsAfterParse)
{
    ParseTwoFlagsInOrder("eviction_high_watermark_percent", "80", "eviction_low_watermark_percent", "85", true);
    EXPECT_EQ(FLAGS_eviction_high_watermark_percent, 80u);
    EXPECT_EQ(FLAGS_eviction_low_watermark_percent, 85u);
    EXPECT_FALSE(ValidateWatermarkFlags());
}

TEST_F(WatermarkFlagParseTest, EmbeddedConfigEvictionBelowDefaultPairValid)
{
    EmbeddedConfig config;
    config.SetArgs({ { "eviction_high_watermark_percent", "75" }, { "eviction_low_watermark_percent", "70" } });
    std::string errMsg;
    EXPECT_TRUE(ParseCommandLineFlags(config, errMsg));
    EXPECT_TRUE(errMsg.empty());
    EXPECT_EQ(FLAGS_eviction_high_watermark_percent, 75u);
    EXPECT_EQ(FLAGS_eviction_low_watermark_percent, 70u);
    EXPECT_TRUE(ValidateWatermarkFlags());
}
}  // namespace ut
}  // namespace datasystem
