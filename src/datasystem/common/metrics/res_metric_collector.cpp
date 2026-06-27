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
 * Description: Res logger, flush current node's res information.
 */

#include "datasystem/common/metrics/res_metric_collector.h"
#include <chrono>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <utility>
#include "datasystem/common/constants.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.h"
#include "datasystem/common/metrics/resource_json_schema.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/validator.h"

DS_DEFINE_int32(sc_regular_socket_num, 16,
             "The number of regular backend socket for stream cache. Must be great equal than 0.");
DS_DEFINE_int32(sc_stream_socket_num, 16,
             "The number of stream backend socket for stream cache. Must be great equal than 0.");
DS_DEFINE_int32(log_monitor_interval_ms, 10000, "The sleep time between iterations of observability collector scan");
DS_DEFINE_validator(sc_regular_socket_num, &Validator::ValidateRpcThreadNum);
DS_DEFINE_validator(sc_stream_socket_num, &Validator::ValidateRpcThreadNum);
DS_DEFINE_validator(log_monitor_interval_ms, &Validator::ValidateInt32);
DS_DECLARE_bool(log_monitor);
DS_DECLARE_bool(json_log_monitor);
DS_DECLARE_string(log_monitor_exporter);
DS_DECLARE_string(log_dir);
DS_DECLARE_string(cluster_name);

namespace datasystem {
namespace {
// Append one metric group to the JSON metrics object. Single-field groups flatten to a scalar;
// multi-field groups emit an object. Empty/malformed handler output emits an all-zero structure
// (design 2.2.5 option 3) so the group key is stable across lines.
// Returns true when the group was emitted (so the caller can track the comma separator).
bool AppendGroupJson(std::ostringstream &os, const ResourceFieldDesc &desc, const std::string &raw,
                     bool firstGroup)
{
    if (!desc.recordGroup) {
        return false;
    }
    if (!firstGroup) {
        os << ',';
    }
    os << '"' << desc.groupName << "\":";
    std::vector<std::string> tokens;
    bool valid = !raw.empty();
    if (valid) {
        SplitResourceFields(raw, desc.sep, tokens);
        valid = (tokens.size() == desc.fieldNames.size());
    }
    if (!valid) {
        LOG_FIRST_N(WARNING, 1) << "Resource JSON group " << desc.groupName
                                << " emitted all-zero: raw='" << raw << "'";
    }
    const bool singleField = (desc.fieldNames.size() == 1 && desc.recordMask[0]);
    if (singleField) {
        os << (valid ? tokens[0] : "0");
        return true;
    }
    os << '{';
    bool firstField = true;
    for (size_t k = 0; k < desc.fieldNames.size(); ++k) {
        if (!desc.recordMask[k]) {
            continue;
        }
        if (!firstField) {
            os << ',';
        }
        firstField = false;
        os << '"' << desc.fieldNames[k] << "\":";
        os << (valid ? tokens[k] : "0");
    }
    os << '}';
    return true;
}
}  // namespace

void ResMetricCollector::Stop()
{
    exitFlag_ = true;
    if (collectorThread_ != nullptr) {
        collectorThread_->join();
        collectorThread_.reset();
    }
}

Status ResMetricCollector::Init()
{
    if (FLAGS_log_monitor || FLAGS_json_log_monitor) {
        if (FLAGS_log_monitor_exporter != "harddisk") {
            RETURN_STATUS(K_INVALID, "Invalid gflag log_monitor_exporter. Must be \"harddisk\".");
        }
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Logging::CreateLogDir(), K_NOT_READY, "Log file creation failed");
        if (FLAGS_log_monitor) {
            std::string filePath = FLAGS_log_dir + "/" + RESOURCE_LOG_NAME + ".log";
            auto hardDiskExporter = std::make_unique<HardDiskExporter>();
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(hardDiskExporter->Init(filePath), "hardDiskExporter Init failed.");
            exporter_ = std::move(hardDiskExporter);
        }
        if (FLAGS_json_log_monitor) {
            std::string jsonPath = FLAGS_log_dir + "/" + KV_RESOURCE_LOG_NAME + ".log";
            auto jsonExporter = std::make_unique<JsonLinesExporter>();
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(jsonExporter->Init(jsonPath), "jsonExporter Init failed.");
            jsonExporter_ = std::move(jsonExporter);
        }
        isInit_ = true;
    }
    return Status::OK();
}

void ResMetricCollector::Start()
{
    if (isInit_) {
        CollectMetrics();
    }
}

ResMetricCollector &ResMetricCollector::Instance()
{
    static ResMetricCollector instance;
    return instance;
}

void ResMetricCollector::CollectMetrics()
{
    collectorThread_ = std::make_unique<Thread>([this]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
        int iLimit = int(ResMetricName::RES_METRICS_END) - 1;
        while (!exitFlag_) {
            if (FLAGS_log_monitor || FLAGS_json_log_monitor) {
                std::string metricString;
                std::stringstream unRegisteredFunctions;
                // Cache each handler result once: some handlers reset interval counters
                // (e.g. GetAndResetIntervalStats), so text and JSON paths must share one call.
                std::vector<std::string> handlerResults(iLimit + 1);
                for (int i = int(ResMetricName::SHARED_MEMORY); i <= iLimit; i++) {
                    auto iter = collectHandler_.find(i);
                    if (iter != collectHandler_.end()) {
                        std::string collectMetrics = iter->second();
                        handlerResults[i] = collectMetrics;
                        metricString += collectMetrics + " | ";
                    } else {
                        metricString += " | ";
                        unRegisteredFunctions << std::to_string(i) << (i == iLimit ? "" : ",");
                    }
                }
                if (!unRegisteredFunctions.str().empty()) {
                    LOG_FIRST_N(WARNING, 1)
                        << "No handler found for resource log type :" << unRegisteredFunctions.str();
                }
                if (!metricString.empty()) {
                    metricString.pop_back();
                }
                Uri uri(__FILE__);
                if (FLAGS_log_monitor && exporter_ != nullptr) {
                    exporter_->Send(metricString, uri, __LINE__);
                    exporter_->SubmitWriteMessage();
                }
                if (FLAGS_json_log_monitor && jsonExporter_ != nullptr) {
                    std::string jsonLine = BuildResourceJson(handlerResults);
                    jsonExporter_->WriteJsonLine(jsonLine);
                }
            }
            INJECT_POINT("worker.CollectMetrics", [this](int interval) { msInterval_ = interval; });
            auto waitTimeMs = 100;
            int count = 0;
            auto maxCount = msInterval_ / waitTimeMs;
            while (!exitFlag_ && count < maxCount) {
                std::this_thread::sleep_for(std::chrono::milliseconds(waitTimeMs));
                count++;
            }
        }
    });
    collectorThread_->set_name("CollectMetrics");
}

std::string ResMetricCollector::BuildResourceJson(const std::vector<std::string> &handlerResults)
{
    std::ostringstream os;
    os << "{\"event\":\"resource_snapshot\",\"version\":\"v0\",\"metrics\":{";
    int iLimit = int(ResMetricName::RES_METRICS_END) - 1;
    bool firstGroup = true;
    for (int i = int(ResMetricName::SHARED_MEMORY); i <= iLimit; ++i) {
        const auto &desc = GetResourceFieldDesc(static_cast<ResMetricName>(i));
        if (i < static_cast<int>(handlerResults.size())) {
            if (AppendGroupJson(os, desc, handlerResults[i], firstGroup)) {
                firstGroup = false;
            }
        }
    }
    os << "}}";
    // Wrap with the single shared pod/cluster-label path (escaped), same as kv_metrics.log.
    return WrapJsonWithPodCluster(os.str(), jsonExporter_->PodName(), jsonExporter_->ClusterName());
}

void ResMetricCollector::RegisterCollectHandler(ResMetricName metricName, std::function<std::string()> collectHandler)
{
    if (exporter_ == nullptr && jsonExporter_ == nullptr) {
        return;
    }
    collectHandler_.emplace(int(metricName), collectHandler);
}

ResMetricCollector::~ResMetricCollector()
{
    Stop();
}
}  // namespace datasystem
