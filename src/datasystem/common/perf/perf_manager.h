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
 * Description: perf manager.
 */
#ifndef DATASYSTEM_PERFMANAGER_H
#define DATASYSTEM_PERFMANAGER_H

#include <atomic>
#include <chrono>
#include <climits>
#include <string>
#include <vector>

namespace datasystem {
/**
 * @brief PerfKey enum specifies the performance point.
 *        The enum value should add to GetPerfKeyDefines function.
 */
#define PERF_KEY_DEF(keyEnum) keyEnum,
enum class PerfKey : size_t {
    NONE = 0,

#include "datasystem/common/perf/perf_point.def"
};
#undef PERF_KEY_DEF

/**
 * @brief PerfInfo used for store the performance information. Time unit is nanoseconds!
 */
struct PerfInfo {
    std::atomic<uint64_t> count;
    std::atomic<uint64_t> totalTime;
    std::atomic<uint64_t> tickCount;
    std::atomic<uint64_t> maxFrequency;
    std::atomic<uint64_t> maxTime;
    std::atomic<uint64_t> minTime = { ULONG_MAX };

    PerfInfo() = default;

    explicit PerfInfo(const PerfInfo &info)
    {
        count.store(info.count.load());
        maxTime.store(info.maxTime.load());
        minTime.store(info.minTime.load());
        totalTime.store(info.totalTime.load());
        tickCount.store(info.tickCount.load());
        maxFrequency.store(info.maxFrequency.load());
    };

    void Reset()
    {
        count = 0;
        maxTime = 0;
        minTime = ULONG_MAX;
        totalTime = 0;
        tickCount = 0;
        maxFrequency = 0;
    }

    PerfInfo &operator=(const PerfInfo &info)
    {
        count.store(info.count.load());
        maxTime.store(info.maxTime.load());
        minTime.store(info.minTime.load());
        totalTime.store(info.totalTime.load());
        tickCount.store(info.tickCount.load());
        maxFrequency.store(info.maxFrequency.load());
        return *this;
    }
};

/**
 * @brief PerfManager class used for managing the performance information.
 */
class PerfManager {
public:
    /**
     * @brief Get the Singleton performance manager instance.
     * @return PerfManager instance.
     */
    static PerfManager *Instance();

    virtual ~PerfManager() noexcept = default;

    /**
     * @brief Record one performance information according to the key.
     * @param[in] key The key.
     * @param[in] elapsed Time (unit nanoseconds) the info to add.
     */
    void Add(PerfKey key, uint64_t elapsed);

    /**
     * @brief Delete one performance information according to the key.
     */
    void ResetPerfLog();

    /**
     * @brief Get performance logs.
     * @return The perf log.
     */
    std::string GetPerfLog() const;

    /**
     * @brief Print performance logs.
     */
    void PrintPerfLog() const;

    /**
     * @brief Trigger performance logs. Should call in main thread.
     */
    void Tick();

    /**
     * @brief Get the performance info list.
     * @param[out] perfInfoList The performance info list.
     */
    void GetPerfInfoList(std::vector<std::pair<std::string, PerfInfo>> &perfInfoList) const;

protected:
    /**
     * @brief We need to declare the constructor as protected
     *        function because PerfManager is a singleton.
     */
    PerfManager();

private:
    using clock = std::chrono::steady_clock;

    // All keys will add in constructor, so no need lock.
    PerfInfo *perfInfoList_{ nullptr };
    const char **keyNameList_{ nullptr };
    size_t perfKeyCount_{ 0 };

    std::chrono::time_point<clock> prevTickTime_;
    std::chrono::time_point<clock> prevLogTime_;
};

/**
 * @brief Performance check point, the time unit is nanoseconds.
 */
class PerfPoint {
public:
    /**
     * @brief Construct the PerfPoint.
     * @param[in] key PerfKey.
     */
    explicit PerfPoint(PerfKey key) : beg_(clock::now()), key_(key), isRecord_(false)
    {
    }

    /**
     * @brief Call Record if isRecord_ is false.
     */
    virtual ~PerfPoint() noexcept;

    /**
     * @brief Add performance information to PerfManager.
     */
    void Record();

    /**
     * @brief Reset begin time.
     * @param[in] key PerfKey.
     */
    void Reset(PerfKey key = PerfKey::NONE);

    /**
     * @brief Add performance information to PerfManager and reset the begin time and key.
     * @param[in] key PerfKey.
     */
    void RecordAndReset(PerfKey key);

    /**
     * @brief Add elapsed time to PerfManager.
     * @param[in] key PerfKey.
     * @param[in] elapsed Time to add.
     */
    static void RecordElapsed(PerfKey key, uint64_t elapsed);

private:
    using clock = std::chrono::steady_clock;
    std::chrono::time_point<clock> beg_;
    PerfKey key_;
    bool isRecord_;
};
}  // namespace datasystem

#endif  // DATASYSTEM_PERFMANAGER_H
