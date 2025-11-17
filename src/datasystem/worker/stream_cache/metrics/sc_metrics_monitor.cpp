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

#include "datasystem/worker/stream_cache/metrics/sc_metrics_monitor.h"

#include <iomanip>

#include "datasystem/common/constants.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.h"
#include "datasystem/common/util/lock_helper.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/worker/stream_cache/stream_manager.h"

DS_DECLARE_bool(log_monitor);
DS_DECLARE_uint64(sc_local_cache_memory_size_mb);
DS_DEFINE_uint32(sc_metrics_log_interval_s, 60, "Interval between logging stream metrics. Default to 60s");
DS_DEFINE_validator(sc_metrics_log_interval_s, &Validator::ValidateUint32);
DS_DECLARE_string(log_dir);

const uint64_t MAX_LOCAL_CACHE_MEMORY_BYTES = FLAGS_sc_local_cache_memory_size_mb * 1024 * 1024;
const int THREE_DECIMAL_PLACES = 1000;

#define MONITOR_LOCK_ARGS(lockname) \
    (lockname), [funName = __FUNCTION__] { return FormatString("%s, %s:%s", #lockname, funName, __LINE__); }

namespace datasystem {
Status ScMetricsMonitor::StartMonitor()
{
    if (FLAGS_log_monitor) {
        std::string filePath = FLAGS_log_dir + "/" + SC_METRICS_LOG_NAME + ".log";
        auto hardDiskExporter = std::make_unique<HardDiskExporter>();
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Logging::CreateLogDir(), K_NOT_READY, "Log file creation failed");
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(hardDiskExporter->Init(filePath), "hardDiskExporter Init failed.");
        exporter_ = std::move(hardDiskExporter);
        thread_ = std::make_unique<Thread>(&ScMetricsMonitor::Tick, this);
        exitPrintThread_ = std::make_unique<ThreadPool>(1);
    }
    isEnabled_ = FLAGS_log_monitor;
    return Status::OK();
}

ScMetricsMonitor::~ScMetricsMonitor()
{
    if (isEnabled_) {
        Shutdown();
    }
}

ScMetricsMonitor *ScMetricsMonitor::Instance()
{
    static ScMetricsMonitor inst;
    return &inst;
}

bool ScMetricsMonitor::IsEnabled()
{
    return isEnabled_;
}

void ScMetricsMonitor::UpdateAndPrintMetrics()
{
    Uri uri(__FILE__);
    std::unordered_map<std::string, StreamEntry> tempStreams;
    {
        // Make a temporary copy of streams_ without holding the lock for too long
        ReadLockHelper rlock(MONITOR_LOCK_ARGS(mutex_));
        tempStreams = streams_;
    }
    // Print stream metrics
    for (auto streamIt = tempStreams.begin(); streamIt != tempStreams.end(); ++streamIt) {
        StreamEntry entry = streamIt->second;
        auto streamManager = entry.mgr.lock();
        auto streamMeta = entry.meta.lock();
        if (streamManager || streamMeta) {
            std::shared_ptr<SCStreamMetrics> scMetric;
            if (streamManager) {
                streamManager->UpdateStreamMetrics();
                scMetric = streamManager->GetSCStreamMetrics();
            }
            if (streamMeta) {
                streamMeta->UpdateStreamMetrics();
                scMetric = streamMeta->GetSCStreamMetrics();
            }
            exporter_->Send(scMetric->PrintMetrics(), uri, __LINE__);
        }
    }
    // Flush log message
    exporter_->SubmitWriteMessage();
}

Status ScMetricsMonitor::AddStream(const std::string streamName, const std::weak_ptr<StreamManager> stream,
                                   std::shared_ptr<SCStreamMetrics> &metrics)
{
    WriteLockHelper wlock(MONITOR_LOCK_ARGS(mutex_));
    StreamEntry &entry = streams_[streamName];
    entry.mgr = stream;
    // Get the stream metrics object from StreamMetaData if it already exists
    if (auto ptr = entry.meta.lock()) {
        metrics = ptr->GetSCStreamMetrics();
    } else {
        metrics = std::make_shared<SCStreamMetrics>(streamName);
    }
    metrics->isMgrExist = true;
    return Status::OK();
}

Status ScMetricsMonitor::AddStreamMeta(const std::string streamName, const std::weak_ptr<StreamMetadata> streamMeta,
                                       std::shared_ptr<SCStreamMetrics> &metrics)
{
    WriteLockHelper wlock(MONITOR_LOCK_ARGS(mutex_));
    StreamEntry &entry = streams_[streamName];
    entry.meta = streamMeta;
    // Get the stream metrics object from StreamManager if it already exists
    if (auto ptr = entry.mgr.lock()) {
        metrics = ptr->GetSCStreamMetrics();
    } else {
        metrics = std::make_shared<SCStreamMetrics>(streamName);
    }
    metrics->isMetaExist = true;
    return Status::OK();
}

void ScMetricsMonitor::ExitStream(const std::string streamName, const std::string msg)
{
    if (isEnabled_) {
        // Execute print in another thread to avoid destructor taking time
        exitPrintThread_->Execute([this, msg]() {
            Uri uri(__FILE__);
            exporter_->Send(msg, uri, __LINE__);
        });
        // Erase from streams_
        WriteLockHelper wlock(MONITOR_LOCK_ARGS(mutex_));
        streams_.erase(streamName);
    }
}

void ScMetricsMonitor::Tick()
{
    const int CONVERT_TO_MS = 1000;
    std::chrono::time_point<clock> prevLogTime;
    while (!interruptFlag_) {
        std::chrono::time_point<clock> nowTime = clock::now();
        int64_t elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(nowTime - prevLogTime).count();
        // Check if its been FLAGS_sc_metrics_log_interval_s number of secs
        if (elapsed >= FLAGS_sc_metrics_log_interval_s * CONVERT_TO_MS) {
            prevLogTime = nowTime;
            UpdateAndPrintMetrics();
        }
        // Wait for FLAGS_sc_metrics_log_interval_s number of secs between another log
        cvLock_.WaitFor(FLAGS_sc_metrics_log_interval_s * CONVERT_TO_MS);
    }
}

void ScMetricsMonitor::Shutdown()
{
    if (!thread_) {
        return;
    }
    interruptFlag_ = true;
    cvLock_.Set();
    if (thread_->joinable()) {
        thread_->join();
    }
}
}  // namespace datasystem
