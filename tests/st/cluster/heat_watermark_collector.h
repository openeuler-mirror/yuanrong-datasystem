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
 * Description: Read heat-related object-copy watermarks from worker resource logs in cluster tests.
 */

#ifndef DATASYSTEM_TESTS_ST_CLUSTER_HEAT_WATERMARK_COLLECTOR_H
#define DATASYSTEM_TESTS_ST_CLUSTER_HEAT_WATERMARK_COLLECTOR_H

#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "datasystem/common/metrics/res_metric_name.h"
#include "datasystem/common/util/format.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace st {

/**
 * The three watermarks use the allocator-accounted copy bytes published by OBJECT_COPY_WATERMARK:
 * - heatDataWatermark: hot primary bytes / all primary bytes;
 * - primaryWatermark: all primary bytes / worker copy capacity;
 * - hotPrimaryWatermark: hot primary bytes / worker copy capacity.
 */
struct HeatWatermarkSample {
    uint32_t workerIndex = 0;
    uint64_t hotPrimaryBytes = 0;
    uint64_t primaryBytes = 0;
    uint64_t copyCapacity = 0;
    double heatDataWatermark = 0.0;
    double primaryWatermark = 0.0;
    double hotPrimaryWatermark = 0.0;
    bool valid = false;

    std::string ToString() const
    {
        return FormatString(
            "worker=%zu, hotPrimaryBytes=%zu, primaryBytes=%zu, copyCapacity=%zu, "
            "heatDataWatermark=%.9f, primaryWatermark=%.9f, hotPrimaryWatermark=%.9f, valid=%d",
            static_cast<size_t>(workerIndex), static_cast<size_t>(hotPrimaryBytes),
            static_cast<size_t>(primaryBytes), static_cast<size_t>(copyCapacity), heatDataWatermark,
            primaryWatermark, hotPrimaryWatermark, valid ? 1 : 0);
    }
};

class HeatWatermarkCollector {
public:
    HeatWatermarkCollector(std::string clusterRootDir, uint32_t workerCount)
        : clusterRootDir_(std::move(clusterRootDir)), workerCount_(workerCount)
    {
    }

    Status Collect(std::vector<HeatWatermarkSample> &samples) const
    {
        samples.clear();
        samples.reserve(workerCount_);
        for (uint32_t workerIndex = 0; workerIndex < workerCount_; ++workerIndex) {
            HeatWatermarkSample sample;
            auto rc = ReadLatest(workerIndex, sample);
            if (rc.IsError()) {
                return rc;
            }
            samples.emplace_back(std::move(sample));
        }
        return Status::OK();
    }

    template <typename Predicate>
    Status WaitFor(Predicate &&predicate, int timeoutMs, int pollIntervalMs,
                   std::vector<HeatWatermarkSample> &samples) const
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        Status lastRc(K_NOT_READY, "No heat watermark sample has been collected");
        do {
            lastRc = Collect(samples);
            if (lastRc.IsOk() && std::invoke(predicate, samples)) {
                return Status::OK();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(pollIntervalMs));
        } while (std::chrono::steady_clock::now() < deadline);

        std::ostringstream details;
        for (const auto &sample : samples) {
            details << " [" << sample.ToString() << "]";
        }
        return Status(K_RPC_DEADLINE_EXCEEDED,
                      FormatString("Timed out waiting for heat watermarks; last status: %s; samples:%s",
                                   lastRc.ToString(), details.str()));
    }

    static void Log(const std::string &phase, const std::vector<HeatWatermarkSample> &samples)
    {
        for (const auto &sample : samples) {
            std::cout << "[heat_watermark] phase=" << phase << ", " << sample.ToString() << std::endl;
        }
    }

    static Status ParseMetricValue(const std::string &metricValue, uint32_t workerIndex,
                                   HeatWatermarkSample &sample)
    {
        const auto fields = SplitBy(StripResourceLogTerminator(metricValue), "/");
        if (fields.size() < 7) {
            return Status(K_NOT_READY, "OBJECT_COPY_WATERMARK is incomplete");
        }

        uint64_t valid = 0;
        HeatWatermarkSample parsed;
        parsed.workerIndex = workerIndex;
        if (!(ParseUint64(fields[0], parsed.hotPrimaryBytes) && ParseUint64(fields[1], parsed.primaryBytes)
              && ParseUint64(fields[2], parsed.copyCapacity)
              && ParseRatio(fields[3], parsed.hotPrimaryWatermark)
              && ParseRatio(fields[4], parsed.primaryWatermark)
              && ParseRatio(fields[5], parsed.heatDataWatermark) && ParseUint64(fields[6], valid))) {
            return Status(K_INVALID, "OBJECT_COPY_WATERMARK contains an invalid numeric field");
        }
        parsed.valid = valid != 0;
        if (!parsed.valid) {
            return Status(K_NOT_READY, "OBJECT_COPY_WATERMARK is not initialized");
        }
        if (parsed.hotPrimaryBytes > parsed.primaryBytes || parsed.primaryBytes > parsed.copyCapacity) {
            return Status(K_RUNTIME_ERROR,
                          "OBJECT_COPY_WATERMARK violates hot-primary <= primary <= capacity");
        }
        sample = std::move(parsed);
        return Status::OK();
    }

private:
    static std::string StripResourceLogTerminator(std::string value)
    {
        const auto trimTrailingWhitespace = [&value]() {
            while (!value.empty()
                   && (value.back() == ' ' || value.back() == '\t' || value.back() == '\r'
                       || value.back() == '\n')) {
                value.pop_back();
            }
        };
        trimTrailingWhitespace();
        if (!value.empty() && value.back() == '|') {
            value.pop_back();
            trimTrailingWhitespace();
        }
        return value;
    }

    static std::vector<std::string> SplitBy(const std::string &value, const std::string &delimiter)
    {
        std::vector<std::string> fields;
        size_t begin = 0;
        while (begin <= value.size()) {
            const size_t end = value.find(delimiter, begin);
            fields.emplace_back(value.substr(begin, end == std::string::npos ? std::string::npos : end - begin));
            if (end == std::string::npos) {
                break;
            }
            begin = end + delimiter.size();
        }
        return fields;
    }

    static bool ParseUint64(const std::string &value, uint64_t &result)
    {
        errno = 0;
        char *end = nullptr;
        const auto parsed = std::strtoull(value.c_str(), &end, 10);
        if (errno != 0 || end == value.c_str() || *end != '\0') {
            return false;
        }
        result = static_cast<uint64_t>(parsed);
        return true;
    }

    static bool ParseRatio(const std::string &value, double &result)
    {
        errno = 0;
        char *end = nullptr;
        const double parsed = std::strtod(value.c_str(), &end);
        if (errno != 0 || end == value.c_str() || *end != '\0' || !std::isfinite(parsed)
            || parsed < 0.0 || parsed > 1.0) {
            return false;
        }
        result = parsed;
        return true;
    }

    static Status ParseMetrics(const std::vector<std::string> &metrics, uint32_t workerIndex,
                               HeatWatermarkSample &sample)
    {
        const int metricIndex =
            static_cast<int>(ResMetricName::OBJECT_COPY_WATERMARK) - static_cast<int>(ResMetricName::SHARED_MEMORY);
        if (metricIndex < 0 || metricIndex >= static_cast<int>(metrics.size())) {
            return Status(K_NOT_READY, "OBJECT_COPY_WATERMARK is absent from the resource log");
        }
        return ParseMetricValue(metrics[metricIndex], workerIndex, sample);
    }

    Status ReadLatest(uint32_t workerIndex, HeatWatermarkSample &sample) const
    {
        const std::string path =
            FormatString("%s/worker%zu/log/resource.log", clusterRootDir_, static_cast<size_t>(workerIndex));
        std::ifstream input(path);
        if (!input.is_open()) {
            return Status(K_NOT_READY, FormatString("Worker resource log is not ready: %s", path));
        }
        std::string line;
        std::string lastLine;
        while (std::getline(input, line)) {
            if (!line.empty()) {
                lastLine = std::move(line);
            }
        }
        if (lastLine.empty()) {
            return Status(K_NOT_READY, FormatString("Worker resource log is empty: %s", path));
        }

        auto metrics = SplitBy(lastLine, " | ");
        constexpr size_t LOG_PREFIX_FIELD_COUNT = 7;
        if (metrics.size() <= LOG_PREFIX_FIELD_COUNT) {
            return Status(K_NOT_READY, "Worker resource log has no metric fields");
        }
        metrics.erase(metrics.begin(), metrics.begin() + LOG_PREFIX_FIELD_COUNT);
        return ParseMetrics(metrics, workerIndex, sample);
    }

    const std::string clusterRootDir_;
    const uint32_t workerCount_;
};

}  // namespace st
}  // namespace datasystem

#endif  // DATASYSTEM_TESTS_ST_CLUSTER_HEAT_WATERMARK_COLLECTOR_H
