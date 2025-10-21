/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Perf manager.
 */
#include "datasystem/common/perf/perf_manager.h"

#include <sstream>

#include <nlohmann/json.hpp>

#include "datasystem/common/log/log.h"

namespace datasystem {
#ifdef ENABLE_PERF
const uint64_t SECONDS_TO_NANO_UNIT = 1000ul * 1000ul * 1000ul;
const uint64_t TRIGGER_PERF_LOG_NANO_INTERVAL = 60ul * SECONDS_TO_NANO_UNIT;

/**
 * PerfInfo to json value
 * @param[out] json value
 * @param[in] info PerfInfo
 */
static void to_json(nlohmann::json &json, const PerfInfo &info)
{
    uint64_t count = info.count.load();
    uint64_t totalTime = info.totalTime.load();
    uint64_t avgTime = 0;

    if (count > 0) {
        avgTime = totalTime / count;
    }

    json = {
        { "count", count },
        { "minTime", info.minTime.load() },
        { "maxTime", info.maxTime.load() },
        { "totalTime", totalTime },
        { "avgTime", avgTime },
        { "maxFrequency", info.maxFrequency.load() },
    };
}

PerfManager *PerfManager::Instance()
{
    static PerfManager inst;
    return &inst;
}

PerfManager::PerfManager()
{
#define PERF_KEY_DEF(keyEnum) #keyEnum,
    static const char *keyNames[] = {
        "NONE",
#include "datasystem/common/perf/perf_point.def"
    };
#undef PERF_KEY_DEF
    constexpr size_t enumCount = sizeof(keyNames) / sizeof(char *);
    static PerfInfo perfInfoList[enumCount];
    prevTickTime_ = clock::now();
    prevLogTime_ = clock::now();
    perfKeyCount_ = enumCount;
    perfInfoList_ = perfInfoList;
    keyNameList_ = keyNames;
}

void PerfManager::Add(PerfKey key, uint64_t elapsed)
{
    auto index = static_cast<size_t>(key);
    if (index >= perfKeyCount_) {
        return;
    }

    auto &info = perfInfoList_[index];
    info.count.fetch_add(1, std::memory_order_relaxed);
    info.totalTime.fetch_add(elapsed, std::memory_order_relaxed);
    info.tickCount.fetch_add(1, std::memory_order_relaxed);

    uint64_t preValue = info.maxTime.load();
    while (elapsed > preValue && !info.maxTime.compare_exchange_weak(preValue, elapsed)) {
        // empty
    }

    preValue = info.minTime.load();
    while (elapsed < preValue && !info.minTime.compare_exchange_weak(preValue, elapsed)) {
        // empty
    }
}

void PerfManager::ResetPerfLog()
{
    for (size_t i = 0; i < perfKeyCount_; i++) {
        perfInfoList_[i].Reset();
    }
    LOG(INFO) << "Reset PerfLog in perf manager......";
}

std::string PerfManager::GetPerfLog() const
{
    std::stringstream ss;
    std::string prefix;

    for (size_t i = 0; i < perfKeyCount_; i++) {
        const PerfInfo &info = perfInfoList_[i];
        auto keyName = keyNameList_[i];
        uint64_t count = info.count.load();
        if (count > 0) {
            ss << prefix << keyName << ": " << nlohmann::json(info);
            if (prefix.empty()) {
                prefix = '\n';
            }
        }
    }
    return ss.str();
}

void PerfManager::GetPerfInfoList(std::vector<std::pair<std::string, PerfInfo>> &perfInfoList) const
{
    for (size_t i = 0; i < perfKeyCount_; i++) {
        const PerfInfo &info = perfInfoList_[i];
        auto keyName = keyNameList_[i];
        uint64_t count = info.count.load();
        if (count > 0) {
            perfInfoList.emplace_back(keyName, info);
        }
    }
}

void PerfManager::PrintPerfLog() const
{
    std::string perfLog = PerfManager::Instance()->GetPerfLog();
    LOG(INFO) << "[Perf Log]:\n" << perfLog;
}

void PerfManager::Tick()
{
    std::chrono::time_point<clock> nowTime = clock::now();
    uint64_t elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(nowTime - prevLogTime_).count();
    uint64_t tickElapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(nowTime - prevTickTime_).count();
    if (tickElapsed > 0) {
        // Get the max frequency.
        prevTickTime_ = nowTime;
        for (size_t i = 0; i < perfKeyCount_; i++) {
            PerfInfo &info = perfInfoList_[i];
            uint64_t tickCount = info.tickCount.load();
            uint64_t maxFrequency = info.maxFrequency.load();
            uint64_t frequency = tickCount * SECONDS_TO_NANO_UNIT / tickElapsed;
            if (frequency > maxFrequency) {
                info.maxFrequency.store(frequency);
            }
            info.tickCount.store(0);
        }
    }

    if (elapsed >= TRIGGER_PERF_LOG_NANO_INTERVAL) {
        prevLogTime_ = nowTime;
        std::string perfLog = GetPerfLog();
        LOG(INFO) << "[Perf Log]:\n" << perfLog;
    }
}

PerfPoint::~PerfPoint() noexcept
{
    if (!isRecord_) {
        Record();
    }
}

void PerfPoint::Record()
{
    uint64_t elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(clock::now() - beg_).count();
    PerfManager *perfManager = PerfManager::Instance();
    if (perfManager != nullptr) {
        perfManager->Add(key_, elapsed);
    }
    isRecord_ = true;
}

void PerfPoint::Reset(PerfKey key)
{
    beg_ = clock::now();
    isRecord_ = false;
    if (key != PerfKey::NONE) {
        key_ = key;
    }
}

void PerfPoint::RecordAndReset(PerfKey key)
{
    Record();
    Reset(key);
}

void PerfPoint::RecordElapsed(PerfKey key, uint64_t elapsed)
{
    PerfManager *perfManager = PerfManager::Instance();
    if (perfManager != nullptr) {
        perfManager->Add(key, elapsed);
    }
}
#else
PerfManager *PerfManager::Instance()
{
    return nullptr;
}

PerfManager::PerfManager()
{
}

void PerfManager::Add(PerfKey key, uint64_t elapsed)
{
    (void)key;
    (void)elapsed;
}

void PerfManager::ResetPerfLog()
{
}

std::string PerfManager::GetPerfLog() const
{
    return "";
}

void PerfManager::GetPerfInfoList(std::vector<std::pair<std::string, PerfInfo>> &perfInfoList) const
{
    (void)perfInfoList;
}

void PerfManager::PrintPerfLog() const
{
}

void PerfManager::Tick()
{
}

PerfPoint::~PerfPoint() noexcept
{
}

void PerfPoint::Record()
{
}

void PerfPoint::Reset(PerfKey key)
{
    (void)key;
}

void PerfPoint::RecordAndReset(PerfKey key)
{
    (void)key;
}

void PerfPoint::RecordElapsed(PerfKey key, uint64_t elapsed)
{
    (void)key;
    (void)elapsed;
}
#endif
}  // namespace datasystem
