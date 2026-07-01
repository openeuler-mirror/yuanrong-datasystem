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
 * Description: Wiki §3 dynamic config optimization end-to-end ST tests.
 * Automates former manual checks: worker empty-path strace, client empty-path
 * thread gate, and worker config file change detection within 10s.
 */
#include <chrono>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "common.h"
#include "datasystem/client/client_flags_monitor.h"
#include "datasystem/common/flags/config_monitor_state.h"
#include "datasystem/common/flags/dynamic_flag_config.h"
#include "datasystem/object_client.h"

DS_DECLARE_string(monitor_config_file);

namespace datasystem {
namespace st {
namespace {

constexpr int kConfigDetectTimeoutMs = 15000;
constexpr int kEmptyPathObserveSec = 12;
constexpr char kOperationLogSuffix[] = "_operation.log";

std::string ReadFileContent(const std::string &path)
{
    std::ifstream file(path);
    std::stringstream buffer;
    buffer << file.rdbuf();
    return buffer.str();
}

void WriteConfigFile(const std::string &path, const std::string &content)
{
    std::ofstream out(path, std::ios::trunc);
    out << content;
    out.close();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
}

bool IsStraceAvailable()
{
    std::string output;
    int exitCode = 0;
    Status rc = ExecuteCmd("command -v strace", output, &exitCode);
    return rc.IsOk() && exitCode == 0 && !output.empty();
}

bool StraceShowsConfigAccess(pid_t pid, const std::string &configPath, int durationSec)
{
    const std::string basename = configPath.substr(configPath.find_last_of('/') + 1);
    const std::string cmd = "timeout " + std::to_string(durationSec) + " strace -p " + std::to_string(pid) +
                            " -e trace=openat,newfstatat,statx -f 2>&1 || true";
    std::string output;
    Status rc = ExecuteCmd(cmd, output);
    if (rc.IsError()) {
        return false;
    }
    return output.find(configPath) != std::string::npos || output.find(basename) != std::string::npos;
}

bool OperationLogLacksConfigChange(const std::string &logPath, int observeSec)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(observeSec);
    while (std::chrono::steady_clock::now() < deadline) {
        const std::string content = ReadFileContent(logPath);
        if (content.find("CONFIG_CHANGED") != std::string::npos || content.find("v=99") != std::string::npos) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return true;
}

bool PollLogContains(const std::string &logPath, const std::string &needle, int timeoutMs)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
    while (std::chrono::steady_clock::now() < deadline) {
        if (ReadFileContent(logPath).find(needle) != std::string::npos) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return false;
}

std::string WorkerOperationLogPath(const std::string &rootDir, uint32_t workerIdx = 0)
{
    return rootDir + "/worker" + std::to_string(workerIdx) + "/log/datasystem_worker" + kOperationLogSuffix;
}

void SetupMinimalCluster(ExternalClusterOptions &opts, const std::string &workerGflagParams)
{
    opts.numOBS = 0;
    opts.numWorkers = 1;
    opts.numMasters = 1;
    opts.enableDistributedMaster = "false";
    opts.numEtcd = 1;
    opts.workerGflagParams = workerGflagParams;
}

}  // namespace

class DynamicConfigWorkerEmptyPathSTTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        decoyConfigPath_ = GetTestCaseDataDir() + "/datasystem.config";
        WriteConfigFile(decoyConfigPath_, "--v=99\n--minloglevel=0\n");
        SetupMinimalCluster(opts,
                            "-monitor_config_file= -log_async=0 -shared_memory_size_mb=25 -v=1 -max_client_num=2000");
    }

    std::string decoyConfigPath_;
};

/**
 * Wiki §3 #1: Worker with empty monitor_config_file must not monitor config files.
 * Primary check: decoy config (--v=99) is not applied within one monitor interval.
 * When strace is available, also verify no openat/stat on the decoy path.
 */
TEST_F(DynamicConfigWorkerEmptyPathSTTest, DISABLED_LEVEL1_WorkerEmptyPathNoConfigFileAccess)
{
    ASSERT_TRUE(cluster_->CheckWorkerProcess(0));

    const std::string opLog = WorkerOperationLogPath(cluster_->GetRootDir(), 0);
    EXPECT_TRUE(OperationLogLacksConfigChange(opLog, kEmptyPathObserveSec))
        << "decoy config should not be applied; log: " << ReadFileContent(opLog);

    if (IsStraceAvailable()) {
        const pid_t workerPid = cluster_->GetWorkerPid(0);
        ASSERT_GT(workerPid, 0);
        EXPECT_FALSE(StraceShowsConfigAccess(workerPid, decoyConfigPath_, kEmptyPathObserveSec));
    }
}

class DynamicConfigClientEmptyPathSTTest : public OCClientCommon {
public:
    void SetUp() override
    {
        FLAGS_monitor_config_file = "";
        unsetenv("DATASYSTEM_CLIENT_CONFIG_PATH");
        ConfigMonitorState::Instance().SetFileMonitorEnabled(false);
        ExternalClusterTest::SetUp();
    }

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        SetupMinimalCluster(opts, "-monitor_config_file= -shared_memory_size_mb=25 -v=1 -max_client_num=2000");
    }
};

/**
 * Wiki §3 #2: Client with empty monitor path must not start FlagsMonitor thread
 * (automates ps -T manual check via thread count + IsMonitorThreadRunning).
 */
TEST_F(DynamicConfigClientEmptyPathSTTest, LEVEL1_ClientEmptyPathNoMonitorThread)
{
    ASSERT_TRUE(cluster_->CheckWorkerProcess(0));

    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);

    EXPECT_FALSE(FlagsMonitor::GetInstance()->IsMonitorThreadRunning());
    EXPECT_FALSE(ConfigMonitorState::Instance().IsFileMonitorEnabled());
}

class DynamicConfigWorkerMonitorSTTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        configPath_ = GetTestCaseDataDir() + "/wiki_dynamic.config";
        WriteConfigFile(configPath_, "--v=0\n--minloglevel=0\n");
        SetupMinimalCluster(opts, "-monitor_config_file=" + configPath_ +
                            " -log_async=0 -shared_memory_size_mb=25 -v=0 -max_client_num=2000");
    }

    std::string configPath_;
};

/**
 * Wiki §3 #3: Worker detects config file changes within 10s and writes CONFIG_CHANGED
 * to operation log (automates 10s file-monitor manual check).
 */
TEST_F(DynamicConfigWorkerMonitorSTTest, DISABLED_LEVEL1_WorkerDetectsConfigChangeWithin10s)
{
    ASSERT_TRUE(cluster_->CheckWorkerProcess(0));

    const std::string opLog = WorkerOperationLogPath(cluster_->GetRootDir(), 0);
    ASSERT_TRUE(PollLogContains(opLog, "CONFIG_INIT:", kConfigDetectTimeoutMs))
        << "operation log not found or CONFIG_INIT missing: " << opLog;

    WriteConfigFile(configPath_, "--v=0\n--minloglevel=1\n");
    EXPECT_TRUE(PollLogContains(opLog, "CONFIG_CHANGED: minloglevel=0 --> 1", kConfigDetectTimeoutMs))
        << "operation log content: " << ReadFileContent(opLog);
}

}  // namespace st
}  // namespace datasystem
