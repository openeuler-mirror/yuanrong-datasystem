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
 * Description: Worker startup flag value validation tests.
 */
#include <cstdint>
#include <string>

#include <gtest/gtest.h>

#include "ut/common.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/logging.h"

DS_DECLARE_int32(v);
DS_DECLARE_uint32(max_log_size);
DS_DECLARE_int32(rocksdb_background_threads);

namespace datasystem {
namespace ut {
namespace {
#define ASSERT_FLAG_REJECTED(name, set_value, expect_value)                       \
    do {                                                                          \
        std::string errMsg;                                                       \
        ASSERT_FALSE(SetCommandLineOption(#name, set_value, errMsg));             \
        EXPECT_TRUE(errMsg.find("failed validation") != errMsg.npos) << errMsg;   \
        ASSERT_EQ(FLAGS_##name, expect_value);                                    \
    } while (0)

#define ASSERT_FLAG_ACCEPTED(name, set_value, expect_value)                       \
    do {                                                                          \
        std::string errMsg;                                                       \
        ASSERT_TRUE(SetCommandLineOption(#name, set_value, errMsg)) << errMsg;    \
        ASSERT_EQ(FLAGS_##name, expect_value);                                    \
    } while (0)

class WorkerParamFlagValidateTest : public CommonTest {
protected:
    void SetUp() override
    {
        // Linker anchor: keeps the DS_DEFINE_validator static initializers in
        // common_flags_validate.cpp from being discarded when dynamic_flag_config
        // is linked as a static library.
        LinkCommonFlagsValidators();
        savedV_ = FLAGS_v;
        savedMaxLogSize_ = FLAGS_max_log_size;
        savedUrmaPollSize_ = FLAGS_urma_poll_size;
        savedLogMonitorIntervalMs_ = FLAGS_log_monitor_interval_ms;
        savedExporter_ = FLAGS_log_monitor_exporter;
        savedOcPort_ = FLAGS_oc_worker_worker_direct_port;
        savedScPort_ = FLAGS_sc_worker_worker_direct_port;
        savedRocksdbThreads_ = FLAGS_rocksdb_background_threads;
    }

    void TearDown() override
    {
        FLAGS_v = savedV_;
        FLAGS_max_log_size = savedMaxLogSize_;
        FLAGS_urma_poll_size = savedUrmaPollSize_;
        FLAGS_log_monitor_interval_ms = savedLogMonitorIntervalMs_;
        FLAGS_log_monitor_exporter = savedExporter_;
        FLAGS_oc_worker_worker_direct_port = savedOcPort_;
        FLAGS_sc_worker_worker_direct_port = savedScPort_;
        FLAGS_rocksdb_background_threads = savedRocksdbThreads_;
    }

    int32_t savedV_;
    uint32_t savedMaxLogSize_;
    uint32_t savedUrmaPollSize_;
    int32_t savedLogMonitorIntervalMs_;
    std::string savedExporter_;
    int32_t savedOcPort_;
    int32_t savedScPort_;
    int32_t savedRocksdbThreads_;
};

// v must be in [0, 3]
TEST_F(WorkerParamFlagValidateTest, VLogLevelRejectsNegativeAndAboveRange)
{
    ASSERT_FLAG_REJECTED(v, "-1", savedV_);
    ASSERT_FLAG_REJECTED(v, "4", savedV_);
    ASSERT_FLAG_ACCEPTED(v, "0", 0);
    ASSERT_FLAG_ACCEPTED(v, "3", 3);
}

// urma_poll_size must be in [1, 16]
TEST_F(WorkerParamFlagValidateTest, UrmaPollSizeRejectsZeroAndAboveDeviceLimit)
{
    ASSERT_FLAG_REJECTED(urma_poll_size, "0", savedUrmaPollSize_);
    ASSERT_FLAG_REJECTED(urma_poll_size, "17", savedUrmaPollSize_);
    ASSERT_FLAG_ACCEPTED(urma_poll_size, "1", 1u);
    ASSERT_FLAG_ACCEPTED(urma_poll_size, "16", 16u);
}

TEST_F(WorkerParamFlagValidateTest, LogMonitorIntervalRejectsNonPositiveValue)
{
    ASSERT_FLAG_REJECTED(log_monitor_interval_ms, "0", savedLogMonitorIntervalMs_);
    ASSERT_FLAG_REJECTED(log_monitor_interval_ms, "-1", savedLogMonitorIntervalMs_);
    ASSERT_FLAG_ACCEPTED(log_monitor_interval_ms, "1000", 1000);
}

// log_monitor_exporter only supports 'harddisk'.
TEST_F(WorkerParamFlagValidateTest, LogMonitorExporterRejectsInvalidValue)
{
    ASSERT_FLAG_REJECTED(log_monitor_exporter, "invalid_exporter", savedExporter_);
    ASSERT_FLAG_ACCEPTED(log_monitor_exporter, "harddisk", std::string("harddisk"));
}

// oc_worker_worker_direct_port must be in [0, 65535]
TEST_F(WorkerParamFlagValidateTest, OcWorkerDirectPortRejectsNegativeAndOverflow)
{
    ASSERT_FLAG_REJECTED(oc_worker_worker_direct_port, "-1", savedOcPort_);
    ASSERT_FLAG_REJECTED(oc_worker_worker_direct_port, "65536", savedOcPort_);
    ASSERT_FLAG_ACCEPTED(oc_worker_worker_direct_port, "0", 0);
    ASSERT_FLAG_ACCEPTED(oc_worker_worker_direct_port, "65535", 65535);
}

// sc_worker_worker_direct_port must be in [0, 65535]
TEST_F(WorkerParamFlagValidateTest, ScWorkerDirectPortRejectsNegativeAndOverflow)
{
    ASSERT_FLAG_REJECTED(sc_worker_worker_direct_port, "-1", savedScPort_);
    ASSERT_FLAG_REJECTED(sc_worker_worker_direct_port, "65536", savedScPort_);
    ASSERT_FLAG_ACCEPTED(sc_worker_worker_direct_port, "0", 0);
    ASSERT_FLAG_ACCEPTED(sc_worker_worker_direct_port, "65535", 65535);
}

// max_log_size must be in [1, 4095]
TEST_F(WorkerParamFlagValidateTest, MaxLogSizeRejectsZeroAndAboveRange)
{
    ASSERT_FLAG_REJECTED(max_log_size, "0", savedMaxLogSize_);
    ASSERT_FLAG_REJECTED(max_log_size, "4096", savedMaxLogSize_);
    ASSERT_FLAG_ACCEPTED(max_log_size, "1", 1u);
    ASSERT_FLAG_ACCEPTED(max_log_size, "4095", 4095u);
}

// rocksdb_background_threads must be > 0.
TEST_F(WorkerParamFlagValidateTest, RocksdbBackgroundThreadsRejectsNonPositive)
{
    ASSERT_FLAG_REJECTED(rocksdb_background_threads, "-1", savedRocksdbThreads_);
    ASSERT_FLAG_REJECTED(rocksdb_background_threads, "0", savedRocksdbThreads_);
    ASSERT_FLAG_ACCEPTED(rocksdb_background_threads, "16", 16);
}
}  // namespace
}  // namespace ut
}  // namespace datasystem
