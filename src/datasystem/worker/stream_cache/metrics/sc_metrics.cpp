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

#include "datasystem/worker/stream_cache/metrics/sc_metrics.h"
#include "datasystem/worker/stream_cache/metrics/sc_metrics_monitor.h"

namespace datasystem {
// We will print in this order
std::vector<StreamMetric> SCStreamMetrics::streamMetrics_ = {
    StreamMetric::NumLocalProducers,
    StreamMetric::NumRemoteProducers,
    StreamMetric::NumLocalConsumers,
    StreamMetric::NumRemoteConsumers,

    StreamMetric::SharedMemoryUsed,
    StreamMetric::LocalMemoryUsed,

    StreamMetric::NumTotalElementsSent,
    StreamMetric::NumTotalElementsReceived,
    StreamMetric::NumTotalElementsAcked,
    StreamMetric::NumSendRequests,
    StreamMetric::NumReceiveRequests,

    StreamMetric::NumPagesCreated,
    StreamMetric::NumPagesReleased,
    StreamMetric::NumPagesInUse,
    StreamMetric::NumPagesCached,
    StreamMetric::NumBigPagesCreated,
    StreamMetric::NumBigPagesReleased,

    StreamMetric::NumLocalProducersBlocked,
    StreamMetric::NumRemoteProducersBlocked,
    StreamMetric::NumRemoteConsumersBlocking,

    StreamMetric::RetainDataState,
    StreamMetric::StreamState
};
// We will print in this order
std::vector<StreamMetric> SCStreamMetrics::masterStreamMetrics_ = {
    StreamMetric::NumProducersMaster,
    StreamMetric::NumConsumersMaster
};
void SCMetrics::LogMetric(const StreamMetric metric, const uint64_t value)
{
    metricsValuesMap_[metric].store(value, std::memory_order_relaxed);
}

void SCMetrics::IncrementMetric(const StreamMetric metric, const uint64_t inc)
{
    metricsValuesMap_[metric].fetch_add(inc, std::memory_order_relaxed);
}

void SCMetrics::DecrementMetric(const StreamMetric metric, const uint64_t inc)
{
    metricsValuesMap_[metric].fetch_sub(inc, std::memory_order_relaxed);
}

void SCMetrics::Init(const std::vector<StreamMetric> &metrics)
{
    // Initialize all metrics
    for (auto metric : metrics) {
        LogMetric(metric, 0);
    }
}

std::string SCMetrics::PrintMetric(const StreamMetric metric)
{
    return std::to_string(GetMetric(metric));
}

std::string SCMetrics::PrintMetrics(const std::vector<StreamMetric> &metrics)
{
    std::string out;
    for (auto metric : metrics) {
        out += PrintMetric(metric) + "/";
    }
    // remove last /
    out.pop_back();
    return out;
}

uint64_t SCMetrics::GetMetric(const StreamMetric metric)
{
    return metricsValuesMap_[metric].load(std::memory_order_relaxed);
}

int64_t SCMetrics::GetProducersNum()
{
    return metricsValuesMap_[StreamMetric::NumLocalProducers].load(std::memory_order_relaxed);
}

int64_t SCMetrics::GetConsumersNum()
{
    return metricsValuesMap_[StreamMetric::NumLocalConsumers].load(std::memory_order_relaxed);
}

uint64_t SCMetrics::GetLocalMemUsed()
{
    return metricsValuesMap_[StreamMetric::LocalMemoryUsed].load(std::memory_order_relaxed);
}

SCStreamMetrics::SCStreamMetrics(std::string streamName) : streamName_(streamName)
{
    Init(streamMetrics_);
    Init(masterStreamMetrics_);
}

SCStreamMetrics::~SCStreamMetrics()
{
    ScMetricsMonitor::Instance()->ExitStream(this->streamName_, this->PrintMetrics(true));
}

std::string SCStreamMetrics::PrintMetrics(const bool isExit)
{
    std::string exit = isExit ? " exit" : "";
    std::string result = streamName_ + exit + "/";
    if (isMgrExist) {
        result += SCMetrics::PrintMetrics(streamMetrics_) + "/";
    } else {
        result += std::string(SCStreamMetrics::streamMetrics_.size() - 1, '/');
    }
    if (isMetaExist) {
        result += SCMetrics::PrintMetrics(masterStreamMetrics_);
    } else {
        result += std::string(SCStreamMetrics::masterStreamMetrics_.size() - 1, '/');
    }
    return result;
}
}  // namespace datasystem