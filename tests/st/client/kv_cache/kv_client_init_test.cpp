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
 * Description: KV client initialization tests.
 */
#include <functional>
#include <memory>
#include <sys/wait.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "common.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/kv_client.h"
#include "datasystem/utils/connection.h"
#include "datasystem/utils/kv_client_config.h"

DS_DECLARE_uint32(max_log_size);
DS_DECLARE_uint32(log_async_queue_size);
DS_DECLARE_int32(v);
DS_DECLARE_string(monitor_config_file);

namespace datasystem {
namespace st {
namespace {
constexpr uint32_t FIRST_MAX_LOG_SIZE_MB = 123;
constexpr uint32_t SECOND_MAX_LOG_SIZE_MB = 321;
constexpr uint32_t FIRST_LOG_ASYNC_QUEUE_SIZE = 4096;
constexpr uint32_t SECOND_LOG_ASYNC_QUEUE_SIZE = 8192;
constexpr int32_t FIRST_ZMQ_CLIENT_IO_THREAD = 2;
constexpr int32_t SECOND_ZMQ_CLIENT_IO_THREAD = 3;

KVClientConfig BuildClientConfig(uint32_t maxLogSize, uint32_t asyncQueueSize, int32_t zmqClientIoThread)
{
    KVClientConfig config;
    auto status = KVClientConfig::Builder()
                      .MaxLogSize(maxLogSize)
                      .LogAsyncQueueSize(asyncQueueSize)
                      .ZmqClientIoThread(zmqClientIoThread)
                      .Build(config);
    if (status.IsError()) {
        LOG(ERROR) << "Build KVClientConfig failed: " << status.ToString();
    }
    return config;
}

bool CheckClientConfig(uint32_t maxLogSize, uint32_t asyncQueueSize, int32_t zmqClientIoThread)
{
    if (FLAGS_max_log_size != maxLogSize) {
        LOG(ERROR) << "Unexpected max_log_size, expect: " << maxLogSize << ", actual: " << FLAGS_max_log_size;
        return false;
    }
    if (FLAGS_log_async_queue_size != asyncQueueSize) {
        LOG(ERROR) << "Unexpected log_async_queue_size, expect: " << asyncQueueSize
                   << ", actual: " << FLAGS_log_async_queue_size;
        return false;
    }
    if (FLAGS_zmq_client_io_thread != zmqClientIoThread) {
        LOG(ERROR) << "Unexpected zmq_client_io_thread, expect: " << zmqClientIoThread
                   << ", actual: " << FLAGS_zmq_client_io_thread;
        return false;
    }
    return true;
}
}  // namespace

class KVClientInitTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 1;
        opts.workerGflagParams = " -shared_memory_size_mb=100 -client_dead_timeout_s=1 ";
    }

    void TearDown() override
    {
        client_.reset();
        ExternalClusterTest::TearDown();
    }

protected:
    std::shared_ptr<KVClient> client_;

    void RunInChildProcess(const std::function<int()> &func)
    {
        auto pid = fork();
        ASSERT_GE(pid, 0);
        if (pid == 0) {
            _exit(func());
        }
        int status;
        ASSERT_EQ(waitpid(pid, &status, 0), pid);
        ASSERT_TRUE(WIFEXITED(status));
        ASSERT_EQ(WEXITSTATUS(status), 0);
    }

    ConnectOptions GetConnectOptions()
    {
        ConnectOptions connectOptions;
        InitConnectOpt(0, connectOptions, 5000);
        return connectOptions;
    }
};

TEST_F(KVClientInitTest, FastTransportFailureFallsBack)
{
    if (FLAGS_use_brpc) {
        GTEST_SKIP() << "brpc migration gap; flaky/failing under brpc. Tracked separately.";
    }
    auto connectOptions = GetConnectOptions();
    RunInChildProcess([connectOptions]() -> int {
        KVClient client(connectOptions);
        auto rc = inject::Set("FastTransportManager.Initialize", "return(3000)");
        if (rc.IsError()) {
            LOG(ERROR) << rc.ToString();
            return 1;
        }
        auto status = client.Init();
        if (status.GetCode() != StatusCode::K_URMA_ERROR) {
            LOG(ERROR) << "Unexpected status: " << status.ToString();
            return 1;
        }
        return 0;
    });
}

TEST_F(KVClientInitTest, SameKVClientConfigKeepsProcessConfig)
{
    if (FLAGS_use_brpc) {
        GTEST_SKIP() << "brpc fork-safety: brpc channel/bthread global state not fork-safe. Tracked separately.";
    }
    auto connectOptions = GetConnectOptions();
    RunInChildProcess([connectOptions]() -> int {
        auto config = BuildClientConfig(FIRST_MAX_LOG_SIZE_MB, FIRST_LOG_ASYNC_QUEUE_SIZE, FIRST_ZMQ_CLIENT_IO_THREAD);
        KVClient firstClient(connectOptions);
        if (firstClient.Init(config).IsError() || !CheckClientConfig(FIRST_MAX_LOG_SIZE_MB, FIRST_LOG_ASYNC_QUEUE_SIZE,
                                                                     FIRST_ZMQ_CLIENT_IO_THREAD)) {
            return 1;
        }
        KVClient secondClient(connectOptions);
        if (secondClient.Init(config).IsError() || !CheckClientConfig(FIRST_MAX_LOG_SIZE_MB, FIRST_LOG_ASYNC_QUEUE_SIZE,
                                                                      FIRST_ZMQ_CLIENT_IO_THREAD)) {
            return 1;
        }
        return 0;
    });
}

TEST_F(KVClientInitTest, DifferentKVClientConfigDoesNotOverrideProcessConfig)
{
    if (FLAGS_use_brpc) {
        GTEST_SKIP() << "brpc migration gap; flaky/failing under brpc. Tracked separately.";
    }
    auto connectOptions = GetConnectOptions();
    RunInChildProcess([connectOptions]() -> int {
        auto firstConfig =
            BuildClientConfig(FIRST_MAX_LOG_SIZE_MB, FIRST_LOG_ASYNC_QUEUE_SIZE, FIRST_ZMQ_CLIENT_IO_THREAD);
        KVClient firstClient(connectOptions);
        if (firstClient.Init(firstConfig).IsError()
            || !CheckClientConfig(FIRST_MAX_LOG_SIZE_MB, FIRST_LOG_ASYNC_QUEUE_SIZE, FIRST_ZMQ_CLIENT_IO_THREAD)) {
            return 1;
        }
        auto secondConfig =
            BuildClientConfig(SECOND_MAX_LOG_SIZE_MB, SECOND_LOG_ASYNC_QUEUE_SIZE, SECOND_ZMQ_CLIENT_IO_THREAD);
        KVClient secondClient(connectOptions);
        if (secondClient.Init(secondConfig).IsError()
            || !CheckClientConfig(FIRST_MAX_LOG_SIZE_MB, FIRST_LOG_ASYNC_QUEUE_SIZE, FIRST_ZMQ_CLIENT_IO_THREAD)) {
            return 1;
        }
        return 0;
    });
}

TEST_F(KVClientInitTest, ConfigAfterDefaultInitDoesNotOverrideProcessConfig)
{
    if (FLAGS_use_brpc) {
        GTEST_SKIP() << "brpc fork-safety: brpc channel/bthread global state not fork-safe. Tracked separately.";
    }
    auto connectOptions = GetConnectOptions();
    RunInChildProcess([connectOptions]() -> int {
        KVClient defaultClient(connectOptions);
        if (defaultClient.Init().IsError()) {
            return 1;
        }
        auto config = BuildClientConfig(FIRST_MAX_LOG_SIZE_MB, FIRST_LOG_ASYNC_QUEUE_SIZE, FIRST_ZMQ_CLIENT_IO_THREAD);
        KVClient configuredClient(connectOptions);
        if (configuredClient.Init(config).IsError()) {
            return 1;
        }
        if (FLAGS_max_log_size == FIRST_MAX_LOG_SIZE_MB || FLAGS_log_async_queue_size == FIRST_LOG_ASYNC_QUEUE_SIZE
            || FLAGS_zmq_client_io_thread == FIRST_ZMQ_CLIENT_IO_THREAD) {
            LOG(ERROR) << "Config after default Init unexpectedly overrides process-level config.";
            return 1;
        }
        return 0;
    });
}

TEST_F(KVClientInitTest, EmptyMonitorConfigPathAllowsUpdateConfig)
{
    if (FLAGS_use_brpc) {
        GTEST_SKIP() << "brpc migration gap; flaky/failing under brpc. Tracked separately.";
    }
    auto connectOptions = GetConnectOptions();
    RunInChildProcess([connectOptions]() -> int {
        unsetenv("DATASYSTEM_CLIENT_CONFIG_PATH");
        KVClientConfig config;
        auto buildStatus = KVClientConfig::Builder().MonitorConfigPath("").Build(config);
        if (buildStatus.IsError()) {
            LOG(ERROR) << "Build KVClientConfig failed: " << buildStatus.ToString();
            return 1;
        }
        KVClient client(connectOptions);
        if (client.Init(config).IsError()) {
            LOG(ERROR) << "Init with empty MonitorConfigPath failed.";
            return 1;
        }
        if (!FLAGS_monitor_config_file.empty()) {
            LOG(ERROR) << "Expected empty monitor_config_file, actual: " << FLAGS_monitor_config_file;
            return 1;
        }
        auto updateStatus = client.UpdateConfig(R"({"v":"2"})");
        if (updateStatus.IsError()) {
            LOG(ERROR) << "UpdateConfig failed: " << updateStatus.ToString();
            return 1;
        }
        if (FLAGS_v != 2) {
            LOG(ERROR) << "Unexpected v after UpdateConfig, expect: 2, actual: " << FLAGS_v;
            return 1;
        }
        return 0;
    });
}
}  // namespace st
}  // namespace datasystem
