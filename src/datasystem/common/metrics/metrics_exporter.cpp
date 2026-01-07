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
 * Description: exporter of metrics.
 */
#include "datasystem/common/metrics/metrics_exporter.h"

#include <string>

#include "datasystem/common/flags/flags.h"

DS_DECLARE_string(log_monitor_exporter);

namespace datasystem {
const int32_t MAX_BUFFER_BYTE = 200 * 1024;  // 200k

Status MetricsExporter::Init()
{
    StartFlushThread();
    return Status::OK();
}

void MetricsExporter::StartFlushThread()
{
    auto traceId = Trace::Instance().GetTraceID();
    flushThread_ = std::make_unique<Thread>([this, traceId]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        while (!exitFlag_) {
            FlushThread();
        }
        // Process the remaining buffers in memory.
        {
            std::unique_lock<std::mutex> l(mtx_);
            SetActiveBufferToQueue(activeBuffer_);
        }
        while (!bufferPool_.empty()) {
            FlushThread();
        }
    });
    flushThread_->set_name("MetricsFlush");
}

void MetricsExporter::Stop()
{
    {
        std::lock_guard<std::mutex> l(mtx_);
        exitFlag_ = true;
    }
    notEmpty_.notify_all();
    if (flushThread_ != nullptr) {
        flushThread_->join();
    }
}

void MetricsExporter::WriteMessage(const std::string &message)
{
    std::unique_lock<std::mutex> l(mtx_);
    if (activeBuffer_ == nullptr) {
        activeBuffer_ = std::make_unique<std::vector<std::string>>();
    }

    activeBuffer_->emplace_back(message);
    if (bufferSize_ > UINT64_MAX - message.size()) {
        bufferSize_ = UINT64_MAX;
    } else {
        bufferSize_ += message.size();
    }
    const int TWO = 2;
    if (bufferSize_ < MAX_BUFFER_BYTE / TWO) {
        return;
    }

    // wait flush buffer flush data.
    notFull_.wait(l, [&] { return bufferPool_.size() < static_cast<size_t>(poolSize_); });

    SetActiveBufferToQueue(activeBuffer_);
}

void MetricsExporter::SubmitWriteMessage()
{
    std::unique_lock<std::mutex> l(mtx_);
    if (bufferSize_ <= 0) {
        return;
    }
    notFull_.wait(l, [&] { return bufferPool_.size() < static_cast<size_t>(poolSize_); });
    SetActiveBufferToQueue(activeBuffer_);
}

void MetricsExporter::SetActiveBufferToQueue(std::unique_ptr<std::vector<std::string>> &activeBuffer)
{
    if (activeBuffer == nullptr) {
        return;
    }
    bufferPool_.emplace(std::move(activeBuffer));
    notEmpty_.notify_all();
    activeBuffer_ = nullptr;
    bufferSize_ = 0;
}

void MetricsExporter::GetFlushBufferFromQueue(std::unique_ptr<std::vector<std::string>> &flushBuffer)
{
    if (bufferPool_.empty()) {
        return;
    }
    flushBuffer = std::move(bufferPool_.front());
    bufferPool_.pop();
    notFull_.notify_all();
}
}  // namespace datasystem