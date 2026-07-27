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
 * Description: Parsing-layer tests for scale_in_collect_window_ms gflag validation.
 */
#include "datasystem/common/flags/flags.h"
#include "datasystem/worker/worker_update_flag_check.h"
#include "ut/common.h"

DS_DECLARE_uint32(scale_in_collect_window_ms);

namespace datasystem {
namespace ut {
namespace {

struct ScaleInCollectWindowSnapshot {
    uint32_t value;
};

ScaleInCollectWindowSnapshot SnapshotScaleInCollectWindow()
{
    return { FLAGS_scale_in_collect_window_ms };
}

void RestoreScaleInCollectWindow(const ScaleInCollectWindowSnapshot &snapshot)
{
    FLAGS_scale_in_collect_window_ms = snapshot.value;
}

TEST(ScaleInCollectWindowFlagTest, ValidateAcceptsZeroAndBound)
{
    const auto snapshot = SnapshotScaleInCollectWindow();
    EXPECT_TRUE(WorkerValidateScaleInCollectWindowMs(0));
    EXPECT_TRUE(WorkerValidateScaleInCollectWindowMs(1));
    EXPECT_TRUE(WorkerValidateScaleInCollectWindowMs(5'000));
    RestoreScaleInCollectWindow(snapshot);
}

TEST(ScaleInCollectWindowFlagTest, ValidateRejectsAboveBound)
{
    const auto snapshot = SnapshotScaleInCollectWindow();
    EXPECT_FALSE(WorkerValidateScaleInCollectWindowMs(5'001));
    EXPECT_FALSE(WorkerValidateScaleInCollectWindowMs(6'000));
    RestoreScaleInCollectWindow(snapshot);
}

TEST(ScaleInCollectWindowFlagTest, AdjustClampsAboveBound)
{
    const auto snapshot = SnapshotScaleInCollectWindow();
    FLAGS_scale_in_collect_window_ms = 6'000;
    AdjustScaleInCollectWindowMs();
    EXPECT_EQ(FLAGS_scale_in_collect_window_ms, 5'000u);
    RestoreScaleInCollectWindow(snapshot);
}

TEST(ScaleInCollectWindowFlagTest, AdjustKeepsValidValues)
{
    const auto snapshot = SnapshotScaleInCollectWindow();
    FLAGS_scale_in_collect_window_ms = 0;
    AdjustScaleInCollectWindowMs();
    EXPECT_EQ(FLAGS_scale_in_collect_window_ms, 0u);

    FLAGS_scale_in_collect_window_ms = 1'000;
    AdjustScaleInCollectWindowMs();
    EXPECT_EQ(FLAGS_scale_in_collect_window_ms, 1'000u);

    FLAGS_scale_in_collect_window_ms = 5'000;
    AdjustScaleInCollectWindowMs();
    EXPECT_EQ(FLAGS_scale_in_collect_window_ms, 5'000u);
    RestoreScaleInCollectWindow(snapshot);
}

}  // namespace
}  // namespace ut
}  // namespace datasystem
