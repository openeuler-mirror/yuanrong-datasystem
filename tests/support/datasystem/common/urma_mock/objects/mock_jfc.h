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

#ifndef DATASYSTEM_COMMON_URMA_MOCK_OBJECTS_MOCK_JFC_H
#define DATASYSTEM_COMMON_URMA_MOCK_OBJECTS_MOCK_JFC_H

#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>

#include "datasystem/common/urma_mock/abi/urma_abi_compat.h"

namespace datasystem {
namespace urma_mock {
/**
 * @brief Completion record stored by MockJfc before conversion to urma_cr_t.
 */
struct MockCr {
    urma_status_t status = URMA_SUCCESS;
    uint64_t userCtx = 0;
    uint32_t byteCnt = 0;
    uint32_t wcIpLen = 0;
    uint32_t localId = 0;
    bool opcodeIsRead = false;
    uint64_t immData = 0;
};

class MockContext;

/**
 * @brief Mock completion queue consumed by ds_urma_poll_jfc and ds_urma_wait_jfc.
 */
class MockJfc {
public:
    /**
     * @brief Create a mock completion queue.
     * @param[in] id JFC id.
     * @param[in] ctx Owning context.
     */
    MockJfc(uint64_t id, const std::shared_ptr<MockContext> &ctx);
    ~MockJfc();

    /**
     * @brief Get the completion queue id.
     * @return JFC id.
     */
    uint64_t GetId() const;

    /**
     * @brief Lock the owning context.
     * @return Owning context, or nullptr if it has already been released.
     */
    std::shared_ptr<MockContext> LockContext() const;

    /**
     * @brief Push a completion record and wake waiters.
     * @param[in] cr Completion record to enqueue.
     */
    void PushCr(const MockCr &cr);

    /**
     * @brief Poll completion records without blocking.
     * @param[in] maxCr Maximum records to return.
     * @param[out] outRecords Output URMA completion records.
     * @return Number of records returned.
     */
    int Poll(int maxCr, urma_cr_t *outRecords);

    /**
     * @brief Wait for completion events.
     * @param[in] maxEvents Maximum events to return.
     * @param[in] timeoutMs Wait timeout in milliseconds.
     * @param[out] outJfc Completion queue that produced events.
     * @return Number of events returned.
     */
    int Wait(int maxEvents, int timeoutMs, MockJfc **outJfc);

    /**
     * @brief Acknowledge consumed events.
     * @param[in] ackCnt Number of events consumed by caller.
     */
    void Ack(uint32_t ackCnt);

    /**
     * @brief Reset event notification state.
     * @param[in] enableEvents Whether event notification is enabled.
     */
    void Rearm(bool enableEvents);

private:
    uint64_t id_;
    std::weak_ptr<MockContext> ctx_;
    std::mutex mu_;
    std::condition_variable cv_;
    std::deque<MockCr> crs_;
    int eventFd_ = -1;  // eventfd for poll/wait wake
    bool rearmed_ = true;
};

}  // namespace urma_mock
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_URMA_MOCK_OBJECTS_MOCK_JFC_H
