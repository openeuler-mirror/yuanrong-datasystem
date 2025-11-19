/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Defines ScMetricsMonitor class to monitor and print stream metrics
 */
#ifndef DATASYSTEM_STREAM_SC_METRICS_MONITOR_H
#define DATASYSTEM_STREAM_SC_METRICS_MONITOR_H

#include <chrono>
#include <vector>
#include <memory>
#include <mutex>

#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/metrics/metrics_exporter.h"
#include "datasystem/worker/stream_cache/metrics/sc_metrics.h"

DS_DECLARE_uint32(sc_metrics_log_interval_s);
namespace datasystem {
namespace worker {
    namespace stream_cache {
        class StreamManager;
    }
}
namespace master {
    class StreamMetadata;
}
using StreamManager = worker::stream_cache::StreamManager;
using StreamMetadata = master::StreamMetadata;
using clock = std::chrono::steady_clock;
class ScMetricsMonitor {
public:
    ~ScMetricsMonitor();
    /**
     * @brief Get the Singleton Metrics manager instance.
     * @return ScMetricsMonitor instance.
     */
    static ScMetricsMonitor *Instance();

    /**
     * @brief Init and Start the ScMetricsMonitor.
     * @return Status of the call
     */
    Status StartMonitor();

    /**
     * @brief Register a stream to monitor.
     * @return Status of the call
     */
    Status AddStream(const std::string streamName, const std::weak_ptr<StreamManager> stream,
                     std::shared_ptr<SCStreamMetrics> &metrics);

    /**
     * @brief Register a master meta to monitor.
     * @return Status of the call
     */
    Status AddStreamMeta(const std::string streamName, const std::weak_ptr<StreamMetadata> streamMeta,
                         std::shared_ptr<SCStreamMetrics> &metrics);

    /**
     * @brief Trigger Monitoring logs. Should call in main thread.
     */
    void Tick();

    /**
     * @brief Shutdown the monitor
     */
    void Shutdown();

    /**
     * @brief Check whether stream metrics monitoring is enabled
     * @return True if stream metrics monitoring is enabled
     */
    bool IsEnabled();

    /**
     * @brief Remove stream from streams_ map, Print the stream metric message
     */
    void ExitStream(const std::string streamName, const std::string msg);

private:
    /**
     * @brief Updates and prints all worker and stream metrics
     */
    void UpdateAndPrintMetrics();
    struct StreamEntry {
        std::weak_ptr<StreamManager> mgr;
        std::weak_ptr<StreamMetadata> meta;
    };

    std::unique_ptr<Thread> thread_{ nullptr };
    std::unique_ptr<ThreadPool> exitPrintThread_{ nullptr };
    std::unordered_map<std::string, StreamEntry> streams_;
    std::shared_timed_mutex mutex_;  // protect streams_ map
    std::atomic<bool> interruptFlag_{ false };
    std::unique_ptr<MetricsExporter> exporter_{ nullptr };
    WaitPost cvLock_;
    bool isEnabled_{ false };
};
}  // namespace datasystem
#endif