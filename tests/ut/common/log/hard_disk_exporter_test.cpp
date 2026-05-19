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
 * Description: HardDiskExporter log rotation tests.
 */
#include "ut/common.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "datasystem/common/constants.h"
#include "datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.h"
#include "datasystem/common/metrics/res_metric_collector.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/uri.h"

DS_DECLARE_bool(log_monitor);
DS_DECLARE_string(log_dir);
DS_DECLARE_string(log_monitor_exporter);
DS_DECLARE_int32(minloglevel);
DS_DECLARE_uint32(max_log_file_num);
DS_DECLARE_uint32(max_log_size);

namespace datasystem {
namespace ut {
class EnvGuard {
public:
    explicit EnvGuard(std::vector<std::string> names) : names_(std::move(names))
    {
        for (const auto &name : names_) {
            const char *value = std::getenv(name.c_str());
            oldValues_.emplace_back(value == nullptr ? "" : value);
            hadValues_.emplace_back(value != nullptr);
        }
    }

    ~EnvGuard()
    {
        for (size_t i = 0; i < names_.size(); ++i) {
            if (hadValues_[i]) {
                (void)setenv(names_[i].c_str(), oldValues_[i].c_str(), 1);
            } else {
                (void)unsetenv(names_[i].c_str());
            }
        }
    }

private:
    std::vector<std::string> names_;
    std::vector<std::string> oldValues_;
    std::vector<bool> hadValues_;
};

class HardDiskExporterTest : public CommonTest {
public:
    template <typename F, typename Rep, typename Period>
    bool Retry(F const &func, const int times, std::chrono::duration<Rep, Period> interval)
    {
        int count = 0;
        while (count < times) {
            ++count;
            if (func()) {
                return true;
            }
            std::this_thread::sleep_for(interval);
        }
        return false;
    }

    size_t CountRotatedFiles(const std::string &filePath)
    {
        std::string dir;
        std::string basename;
        size_t pos = filePath.find_last_of('/');
        if (pos == std::string::npos) {
            dir = ".";
            basename = filePath;
        } else {
            dir = filePath.substr(0, pos);
            basename = filePath.substr(pos + 1);
        }

        std::string prefix;
        size_t idx = basename.find(".log");
        if (idx != std::string::npos) {
            prefix = basename.substr(0, idx + 1);
        } else {
            prefix = basename + ".";
        }

        std::string pattern = dir + "/" + prefix + "*[0-9]\\.log";
        std::vector<std::string> files;
        DS_EXPECT_OK(Glob(pattern, files));
        return files.size();
    }

    bool FileContains(const std::string &filePath, const std::string &content)
    {
        std::ifstream ifs(filePath);
        if (!ifs.is_open()) {
            return false;
        }
        std::stringstream buffer;
        buffer << ifs.rdbuf();
        return buffer.str().find(content) != std::string::npos;
    }

    uint64_t CountPatternInFile(const std::string &filePath, const std::string &pattern)
    {
        std::ifstream ifs(filePath);
        if (!ifs.is_open()) {
            return 0;
        }

        uint64_t count = 0;
        std::string line;
        while (std::getline(ifs, line)) {
            size_t pos = 0;
            while ((pos = line.find(pattern, pos)) != std::string::npos) {
                ++count;
                pos += pattern.size();
            }
        }
        return count;
    }
};

TEST_F(HardDiskExporterTest, TestPodIpPriority)
{
    EnvGuard guard({ "POD_IP", "POD_NAME", "HOSTNAME" });
    (void)setenv("POD_IP", "10.0.0.1", 1);
    (void)setenv("POD_NAME", "pod-a", 1);
    (void)setenv("HOSTNAME", "host-a", 1);
    std::string filePath = FLAGS_log_dir + "/harddisk_pod_ip_priority.log";

    HardDiskExporter exporter;
    DS_ASSERT_OK(exporter.Init(filePath));
    Uri uri(filePath);
    exporter.Send("pod priority test", uri, __LINE__);
    exporter.SubmitWriteMessage();

    const auto retryTimes = 30;
    ASSERT_TRUE(Retry([this, &filePath]() -> bool { return FileContains(filePath, " | 10.0.0.1 | "); }, retryTimes,
                      std::chrono::milliseconds(100)));
    ASSERT_FALSE(FileContains(filePath, " | pod-a | "));
    ASSERT_FALSE(FileContains(filePath, " | host-a | "));
}

TEST_F(HardDiskExporterTest, TestMaxLogFileNumLimit)
{
    FLAGS_max_log_size = 1;
    FLAGS_max_log_file_num = 2;  // Limit to 2 log files
    std::string filePath = FLAGS_log_dir + "/harddisk_limit.log";

    HardDiskExporter exporter;
    DS_ASSERT_OK(exporter.Init(filePath));

    Uri uri(filePath);
    constexpr size_t kChunkSize = static_cast<size_t>(600) * 1024;  // 600 KB
    std::string payload(kChunkSize, 'a');
    const int rounds = 5;
    for (int i = 0; i < rounds; ++i) {
        exporter.Send(payload, uri, __LINE__);
        exporter.Send(payload, uri, __LINE__);
        exporter.SubmitWriteMessage();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));  // 50 ms
    }

    const auto retryTimes = 30;
    ASSERT_TRUE(Retry([this, &filePath]() -> bool { return CountRotatedFiles(filePath) == FLAGS_max_log_file_num; },
                      retryTimes,
                      std::chrono::milliseconds(100)));  // 100 ms
}

TEST_F(HardDiskExporterTest, ResourceCollectorIgnoresMinLogLevelAndRespectsLogMonitor)
{
    auto oldLogMonitor = FLAGS_log_monitor;
    auto oldLogMonitorExporter = FLAGS_log_monitor_exporter;
    auto oldLogMonitorInterval = FLAGS_log_monitor_interval_ms;
    auto oldMinLogLevel = FLAGS_minloglevel;
    Raii restoreFlags([&] {
        FLAGS_log_monitor = oldLogMonitor;
        FLAGS_log_monitor_exporter = oldLogMonitorExporter;
        FLAGS_log_monitor_interval_ms = oldLogMonitorInterval;
        FLAGS_minloglevel = oldMinLogLevel;
    });

    FLAGS_log_monitor = true;
    FLAGS_log_monitor_exporter = "harddisk";
    FLAGS_log_monitor_interval_ms = 100;
    FLAGS_minloglevel = 0;

    const std::string filePath = FLAGS_log_dir + "/" + RESOURCE_LOG_NAME + ".log";
    (void)DeleteFile(filePath);

    std::atomic<uint64_t> seq{ 0 };
    ResMetricCollector collector;
    DS_ASSERT_OK(collector.Init());
    collector.RegisterCollectHandler(ResMetricName::SHARED_MEMORY, [&seq] {
        return "resource_marker_" + std::to_string(++seq);
    });
    collector.Start();

    const std::string marker = "resource_marker_";
    constexpr int retryTimes = 30;
    ASSERT_TRUE(Retry([this, &filePath, &marker]() { return CountPatternInFile(filePath, marker) > 0; }, retryTimes,
                      std::chrono::milliseconds(100)));

    FLAGS_minloglevel = 3;
    auto minLogLevelSeq = seq.load();
    auto minLogLevelCount = CountPatternInFile(filePath, marker);
    ASSERT_TRUE(Retry([&seq, minLogLevelSeq]() { return seq.load() > minLogLevelSeq; }, retryTimes,
                      std::chrono::milliseconds(100)));
    ASSERT_TRUE(Retry([this, &filePath, &marker, minLogLevelCount]() {
        return CountPatternInFile(filePath, marker) > minLogLevelCount;
    }, retryTimes, std::chrono::milliseconds(100)));

    FLAGS_log_monitor = false;
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    auto disabledSeq = seq.load();
    auto disabledCount = CountPatternInFile(filePath, marker);
    std::this_thread::sleep_for(std::chrono::milliseconds(350));
    EXPECT_EQ(seq.load(), disabledSeq);
    EXPECT_EQ(CountPatternInFile(filePath, marker), disabledCount);

    FLAGS_log_monitor = true;
    ASSERT_TRUE(Retry([&seq, disabledSeq]() { return seq.load() > disabledSeq; }, retryTimes,
                      std::chrono::milliseconds(100)));
    ASSERT_TRUE(Retry([this, &filePath, &marker, disabledCount]() {
        return CountPatternInFile(filePath, marker) > disabledCount;
    }, retryTimes, std::chrono::milliseconds(100)));
}
}  // namespace ut
}  // namespace datasystem
