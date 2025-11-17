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
 * Description: Defines SCMetrics, SCWorkerMetrics, SCStreamMetrics, SCMasterStreamMetrics.
 */
#ifndef DATASYSTEM_STREAM_SC_METRICS_H
#define DATASYSTEM_STREAM_SC_METRICS_H

#include <atomic>
#include <string>
#include <unordered_map>
#include <vector>

namespace datasystem {
/**
 * @brief PerfKey enum specifies the performance point.
 *        The enum value should add to GetPerfKeyDefines function.
 */
#define SC_METRIC_KEY_DEF(keyEnum, ...) keyEnum,
enum class StreamMetric : size_t {
    NONE = 0,

#include "datasystem/worker/stream_cache/metrics/sc_metrics.def"
};
#undef SC_METRIC_KEY_DEF

class SCMetrics {
public:
    /**
     * @brief Update the specified metric.
     */
    void LogMetric(const StreamMetric metric, const uint64_t value);

    /**
     * @brief Increment the specified metric by inc.
     */
    void IncrementMetric(const StreamMetric metric, const uint64_t inc);

    /**
     * @brief Decrement the specified metric by inc.
     */
    void DecrementMetric(const StreamMetric metric, const uint64_t inc);

    /**
     * @brief Get the print string of the specified metric
     * @return Stream metric string
     */
    std::string PrintMetric(const StreamMetric metric);

    /**
     * @brief Get the print string of the specified metrics
     * @return Stream metrics string
     */
    std::string PrintMetrics(const std::vector<StreamMetric> &metrics);

    /**
     * @brief Get the specified metric
     * @return Stream metric
     */
    uint64_t GetMetric(const StreamMetric metric);

    /**
     * @brief Obtain local producer num.
     * @return Num of local producers.
     */
    int64_t GetProducersNum();

    /**
     * @brief Obtain local consumer num.
     * @return Num of local consumers.
     */
    int64_t GetConsumersNum();

    /**
     * @brief Obtain local memory usage.
     * @return Local memory usage.
     */
    uint64_t GetLocalMemUsed();

protected:
    /**
     * @brief Initialize the specified metrics
     */
    void Init(const std::vector<StreamMetric> &metrics);

private:
    std::unordered_map<StreamMetric, std::atomic<uint64_t>> metricsValuesMap_;
};

class SCStreamMetrics : public SCMetrics {
public:
    SCStreamMetrics(std::string streamName);
    ~SCStreamMetrics();

    /**
     * @brief Get the print string of all metrics in the stream
     * @return Stream metrics string
     */
    std::string PrintMetrics(bool isExit = false);

    static std::vector<StreamMetric> streamMetrics_;
    static std::vector<StreamMetric> masterStreamMetrics_;
    bool isMgrExist = false;
    bool isMetaExist = false;

private:
    std::string streamName_;
};
}  // namespace datasystem
#endif