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
#include <thread>
#include <utility>

#include "datasystem/common/constants.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/validator.h"

DS_DEFINE_int32(log_monitor_interval_ms, 10000, "The sleep time between iterations of observability collector scan");
DS_DEFINE_validator(log_monitor_interval_ms, &Validator::ValidateInt32);
DS_DECLARE_bool(log_monitor);
DS_DECLARE_string(log_monitor_exporter);
DS_DECLARE_string(log_dir);

namespace datasystem {
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
    if (FLAGS_log_monitor) {
        if (FLAGS_log_monitor_exporter == "harddisk") {
            std::string filePath = FLAGS_log_dir + "/" + RESOURCE_LOG_NAME + ".log";
            auto hardDiskExporter = std::make_unique<HardDiskExporter>();
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Logging::CreateLogDir(), K_NOT_READY, "Log file creation failed");
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(hardDiskExporter->Init(filePath), "hardDiskExporter Init failed.");
            exporter_ = std::move(hardDiskExporter);
        } else {
            RETURN_STATUS(K_INVALID,
                          "Invalid gflag log_monitor_exporter. Must be \"harddisk\".");
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
            std::string metricString;
            std::stringstream unRegisteredFunctions;
            for (int i = int(ResMetricName::SHARED_MEMORY); i <= iLimit; i++) {
                auto iter = collectHandler_.find(i);
                if (iter != collectHandler_.end()) {
                    std::string collectMetrics = iter->second();
                    metricString += collectMetrics + " | ";
                } else {
                    metricString += " | ";
                    unRegisteredFunctions << std::to_string(i) << (i == iLimit ? "" : ",");
                }
            }
            if (!unRegisteredFunctions.str().empty()) {
                LOG_FIRST_N(WARNING, 1) << "No handler found for resource log type :" << unRegisteredFunctions.str();
            }
            if (!metricString.empty()) {
                metricString.pop_back();
            }
            Uri uri(__FILE__);
            exporter_->Send(metricString, uri, __LINE__);
            exporter_->SubmitWriteMessage();
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

void ResMetricCollector::RegisterCollectHandler(ResMetricName metricName, std::function<std::string()> collectHandler)
{
    if (exporter_ == nullptr) {
        return;
    }
    collectHandler_.emplace(int(metricName), collectHandler);
}

ResMetricCollector::~ResMetricCollector()
{
    Stop();
}
}  // namespace datasystem
