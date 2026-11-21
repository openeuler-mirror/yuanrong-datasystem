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
        savedLogMonitorIntervalMs_ = FLAGS_log_monitor_interval_ms;
        savedExporter_ = FLAGS_log_monitor_exporter;
        savedScPort_ = FLAGS_sc_worker_worker_direct_port;
        savedRocksdbThreads_ = FLAGS_rocksdb_background_threads;
        savedBrpcServerThreads_ = FLAGS_brpc_server_num_threads;
    }

    void TearDown() override
    {
        FLAGS_v = savedV_;
        FLAGS_max_log_size = savedMaxLogSize_;
        FLAGS_log_monitor_interval_ms = savedLogMonitorIntervalMs_;
        FLAGS_log_monitor_exporter = savedExporter_;
        FLAGS_sc_worker_worker_direct_port = savedScPort_;
        FLAGS_rocksdb_background_threads = savedRocksdbThreads_;
        FLAGS_brpc_server_num_threads = savedBrpcServerThreads_;
    }

    int32_t savedV_;
    uint32_t savedMaxLogSize_;
    int32_t savedLogMonitorIntervalMs_;
    std::string savedExporter_;
    int32_t savedScPort_;
    int32_t savedRocksdbThreads_;
    int32_t savedBrpcServerThreads_;
};

// v must be in [0, 3]
TEST_F(WorkerParamFlagValidateTest, VLogLevelRejectsNegativeAndAboveRange)
{
    ASSERT_FLAG_REJECTED(v, "-1", savedV_);
    ASSERT_FLAG_REJECTED(v, "4", savedV_);
    ASSERT_FLAG_ACCEPTED(v, "0", 0);
    ASSERT_FLAG_ACCEPTED(v, "3", 3);
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

TEST_F(WorkerParamFlagValidateTest, BrpcServerThreadsRejectsOutOfRange)
{
    ASSERT_FLAG_REJECTED(brpc_server_num_threads, "-1", savedBrpcServerThreads_);
    ASSERT_FLAG_REJECTED(brpc_server_num_threads, "4097", savedBrpcServerThreads_);
    ASSERT_FLAG_ACCEPTED(brpc_server_num_threads, "0", 0);
    ASSERT_FLAG_ACCEPTED(brpc_server_num_threads, "4096", 4096);
}

// node_timeout_s/node_dead_timeout_s must pass the strict startup validation.
TEST_F(WorkerParamFlagValidateTest, ValidateNodeTimeoutFlagsRejectsInvalidCombinations)
{
    auto savedNodeTimeoutS = FLAGS_node_timeout_s;
    auto savedNodeDeadTimeoutS = FLAGS_node_dead_timeout_s;

    FLAGS_node_timeout_s = 60;
    FLAGS_node_dead_timeout_s = 300;
    EXPECT_TRUE(ValidateNodeTimeoutFlags());

    FLAGS_node_timeout_s = 2;
    FLAGS_node_dead_timeout_s = 300;
    EXPECT_FALSE(ValidateNodeTimeoutFlags());

    FLAGS_node_timeout_s = 300;
    FLAGS_node_dead_timeout_s = 60;
    EXPECT_FALSE(ValidateNodeTimeoutFlags());

    FLAGS_node_timeout_s = 60;
    FLAGS_node_dead_timeout_s = 2;
    EXPECT_FALSE(ValidateNodeTimeoutFlags());

    FLAGS_node_timeout_s = savedNodeTimeoutS;
    FLAGS_node_dead_timeout_s = savedNodeDeadTimeoutS;
}

TEST_F(WorkerParamFlagValidateTest, ValidateCommonStartupFlagsRejectsInvalidValues)
{
    const auto savedRequestSampleRate = FLAGS_request_sample_rate;
    const auto savedAccessSampleRate = FLAGS_access_sample_rate;
    const auto savedDiagnosticSampleRate = FLAGS_diagnostic_sample_rate;
    const auto savedLogFilename = FLAGS_log_filename;
    const auto savedBrpcServerNumThreads = FLAGS_brpc_server_num_threads;
    const auto savedLogMonitorIntervalMs = FLAGS_log_monitor_interval_ms;

    FLAGS_request_sample_rate = 0.5;
    FLAGS_access_sample_rate = 0.5;
    FLAGS_diagnostic_sample_rate = 0.5;
    FLAGS_log_filename = "coord_log";
    FLAGS_brpc_server_num_threads = 64;
    FLAGS_log_monitor_interval_ms = 10000;
    EXPECT_TRUE(ValidateCommonStartupFlags());

    FLAGS_request_sample_rate = 2.0;
    EXPECT_FALSE(ValidateCommonStartupFlags());
    FLAGS_request_sample_rate = 0.5;

    FLAGS_log_filename = "bad/name";
    EXPECT_FALSE(ValidateCommonStartupFlags());
    FLAGS_log_filename = "coord_log";

    FLAGS_brpc_server_num_threads = -1;
    EXPECT_FALSE(ValidateCommonStartupFlags());
    FLAGS_brpc_server_num_threads = 64;

    FLAGS_log_monitor_interval_ms = 0;
    EXPECT_FALSE(ValidateCommonStartupFlags());
    FLAGS_log_monitor_interval_ms = -1;
    EXPECT_FALSE(ValidateCommonStartupFlags());
    FLAGS_log_monitor_interval_ms = 10000;

    FLAGS_request_sample_rate = 2.0;
    FLAGS_access_sample_rate = 2.0;
    FLAGS_diagnostic_sample_rate = 2.0;
    FLAGS_log_filename = "bad/name";
    FLAGS_brpc_server_num_threads = -1;
    FLAGS_log_monitor_interval_ms = 0;
    EXPECT_FALSE(ValidateCommonStartupFlags());

    FLAGS_request_sample_rate = savedRequestSampleRate;
    FLAGS_access_sample_rate = savedAccessSampleRate;
    FLAGS_diagnostic_sample_rate = savedDiagnosticSampleRate;
    FLAGS_log_filename = savedLogFilename;
    FLAGS_brpc_server_num_threads = savedBrpcServerNumThreads;
    FLAGS_log_monitor_interval_ms = savedLogMonitorIntervalMs;
}
}  // namespace
}  // namespace ut
}  // namespace datasystem
