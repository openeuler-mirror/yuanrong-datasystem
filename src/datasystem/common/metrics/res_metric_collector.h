/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Resource (system-level) metrics collector, flush current node's res information.
 */

#ifndef DATASYSTEM_COMMON_METRICS_RES_METRICS_COLLECTOR_H
#define DATASYSTEM_COMMON_METRICS_RES_METRICS_COLLECTOR_H

#include <atomic>
#include <thread>
#include <unordered_map>
#include <functional>

#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/metrics/res_metric_name.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/common/util/wait_post.h"

DS_DECLARE_int32(log_monitor_interval_ms);

namespace datasystem {

const std::string RES_ETCD_DEFAULT_USAGE = "0/0/0";
const std::string RES_THREAD_POOL_DEFAULT_USAGE = "0/0/0/0/0";

class ResMetricCollector {
public:
    /**
     * @brief Construct the ResMetricCollector
     * @param msInterval The interval of log time, default is 10s
     */
    explicit ResMetricCollector() : msInterval_(FLAGS_log_monitor_interval_ms){};
    ~ResMetricCollector();

    /**
     * @brief Init metric collector.
     * @return Status of the call.
     */
    Status Init();

    /**
     * @brief Start metric collector.
     */
    void Start();

    /**
     * @brief Get an instance of the class.
     * @return Reference of the instance class.
     */
    static ResMetricCollector &Instance();

    /**
     * @brief Register the log handler func for specific type of log.
     */
    void RegisterCollectHandler(ResMetricName metricName, std::function<std::string()> collectHandler);

    /**
     * @brief Stop metric collector.
     */
    void Stop();

private:
    /**
     * @brief Start logging Res message in the thread.
     */
    void CollectMetrics();

    int msInterval_;   //
    std::atomic<bool> exitFlag_{ false };
    std::unique_ptr<MetricsExporter> exporter_{ nullptr };
    std::unique_ptr<Thread> collectorThread_{ nullptr };
    std::unordered_map<int, std::function<std::string()>> collectHandler_;
    bool isInit_ = false;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_METRICS_RES_METRICS_COLLECTOR_H
