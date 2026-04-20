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
 * Description: Shared setup/teardown and helpers for shm_leak_metrics* unit tests.
 */

#ifndef DATASYSTEM_TESTS_UT_COMMON_METRICS_SHM_LEAK_METRICS_TEST_BASE_H_
#define DATASYSTEM_TESTS_UT_COMMON_METRICS_SHM_LEAK_METRICS_TEST_BASE_H_

#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/metrics/metrics.h"
#include "ut/common.h"

namespace datasystem {
namespace ut {

class ShmLeakMetricsTestBase : public CommonTest {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        metrics::ResetKvMetricsForTest();
        DS_ASSERT_OK(metrics::InitKvMetrics());
    }

    void TearDown() override
    {
        metrics::ResetKvMetricsForTest();
    }

    static metrics::Counter Cnt(metrics::KvMetricId id)
    {
        return metrics::GetCounter(static_cast<uint16_t>(id));
    }

    static metrics::Gauge Gge(metrics::KvMetricId id)
    {
        return metrics::GetGauge(static_cast<uint16_t>(id));
    }
};

}  // namespace ut
}  // namespace datasystem

#endif  // DATASYSTEM_TESTS_UT_COMMON_METRICS_SHM_LEAK_METRICS_TEST_BASE_H_
