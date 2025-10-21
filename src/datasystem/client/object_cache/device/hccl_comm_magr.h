/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
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

#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_DEVICE_HCCL_COMM_THREAD_CONTROL_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_DEVICE_HCCL_COMM_THREAD_CONTROL_H

#include <random>
#include <string>

#include <tbb/concurrent_hash_map.h>
#include <tuple>
#include <unordered_set>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/utils/status.h"

// Number of transfer threads excluding the two threads occupied by the designed inference
constexpr int THREADPOOL_SIZE = 6;

namespace datasystem {
class ThreadCommRecord {
public:
    ThreadCommRecord();

    ~ThreadCommRecord() = default;

    /**
     * @brief Returns the busyness of the current thread.
     * @return Busy score. The higher the score, the busier it is.
     */
    size_t GetThreadLoadScore();

    /**
     * @brief Check whether the comm in the current thread conflicts with the commid in the input parameter.
     * @param[in] waitCheckConflictCommId Commid to be checked for new stock-in
     * @return If a conflict occurs, true is returned. If no conflict occurs, false is returned.
     */
    bool CheckCommConflict(const std::string &waitCheckConflictCommId);

    /**
     * @brief Add commid to the thread information running record table to check whether comm conflicts occur.
     * @param[in] commId Indicates the comm ID to be inserted.
     */
    void AddNewCommIdRecord(const std::string &commId);

    /**
     * @brief Obtains the abstract thread pool corresponding to a thread.
     * @return Thread Pool Shared Smart Pointer
     */
    std::shared_ptr<ThreadPool> GetThreadPool();

    /**
     * @brief When comm exits, the corresponding comm is deleted from the thread running record.
     * @param[in] commId Indicates the comm ID to be erased.
     */
    void RemoveCommIdRecord(const std::string &commId);

private:
    std::shared_ptr<ThreadPool> pool_;
    std::unordered_set<std::string> threadCommIds_;
    std::shared_timed_mutex setMutex_;
};

using TbbThreadDetailsTable = tbb::concurrent_hash_map<int, std::shared_ptr<ThreadCommRecord>>;

class HcclCommMagr {
public:
    HcclCommMagr();

    ~HcclCommMagr() = default;

    /**
     *
     * @brief Finds the most suitable thread in a large thread pool for inserting a commId.
     *        Scores all threads in the thread pool, including the length of the waiting queue in the thread pool and
     *        the number of threads bound to the thread pool.
     *        Sort the score and select the thread with the lowest score.
     *
     *        Deserializes the commId to extract the source (src) and destination (dst).
     *        It then checks if the communication from dst to src does not already exist in the thread.
     *        If the condition is met, the thread is considered legal and suitable for insertion.
     *        If the value is invalid, the second thread is selected.
     * @param[in] commId ID of the new comm.
     * @return threadId and Pointer to the most idle thread pool
     */
    std::tuple<int, std::shared_ptr<ThreadPool>> AssignThreadToComm(const std::string &commId);

    /**
     * @brief Delete commid from the thread running record corresponding to tid.
     * @param[in] tid thread ID
     * @param[in] commId Indicates the commid to be deleted.
     * @return Whether the deletion is successful
     */
    Status RemoveThreadPoolCommRecord(int tid, const std::string &commId);

protected:
    std::string FindNegDirectionCommKey(const std::string &commId);

    std::tuple<int, std::shared_ptr<ThreadPool>> GetThreadPool(int threadId);

    void GetThreadPoolBusyScore(std::vector<std::tuple<int, uint64_t>> &scoreList);

    int GetOptimalThreadId(const std::vector<std::tuple<int, uint64_t>> &scoreList,
                           const std::string &negDirectionCommId);

    void UpdateThreadDetailRecord(int threadId, const std::string &commId);

private:
    TbbThreadDetailsTable threadDetails_;
};

}  // namespace datasystem

#endif