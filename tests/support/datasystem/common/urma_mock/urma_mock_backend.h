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

#ifndef DATASYSTEM_COMMON_URMA_MOCK_URMA_MOCK_BACKEND_H
#define DATASYSTEM_COMMON_URMA_MOCK_URMA_MOCK_BACKEND_H

#include "datasystem/common/urma_mock/abi/urma_abi_compat.h"

#ifdef USE_URMA_MOCK

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <sys/types.h>
#include <unordered_map>
#include <utility>
#include <vector>

#include "datasystem/common/urma_mock/objects/mock_context.h"
#include "datasystem/common/urma_mock/objects/mock_device.h"
#include "datasystem/common/urma_mock/objects/mock_jfc.h"
#include "datasystem/common/urma_mock/objects/mock_jetty.h"
#include "datasystem/common/urma_mock/objects/mock_seg.h"
#include "datasystem/common/urma_mock/objects/mock_tjetty.h"
#include "datasystem/common/urma_mock/post_send/mock_thread_pool.h"

namespace datasystem {
namespace urma_mock {

/**
 * @brief In-process URMA transport used when Datasystem is built with URMA mock support.
 * The backend serves the ds_urma_* adapter calls through mock C ABI entry points. It exercises the same manager paths
 * as real URMA for segment registration, jetty import, post-send, completion polling, async events, and payload writes
 * while replacing hardware access with shared-memory and UDS-backed transfers.
 */
class MockUrmaBackend {
public:
    /**
     * @brief Get the process-local backend singleton.
     * @return Backend instance.
     */
    static MockUrmaBackend &Instance();

    /**
     * @brief Initialize mock device and backend state.
     * @return true if initialization succeeds.
     */
    bool Init();

    /**
     * @brief Release mock backend state and stop background workers.
     */
    void Cleanup();

    /**
     * @brief Check whether backend initialization has completed.
     * @return true if the mock backend is initialized.
     */
    bool IsInitialized() const;

    /**
     * @brief Return available mock devices.
     * @return Mock device list.
     */
    std::vector<MockDevice *> GetDevices();

    /**
     * @brief Find a mock device by name.
     * @param[in] name Device name.
     * @return Matched device, or nullptr if not found.
     */
    MockDevice *GetDeviceByName(const std::string &name);

    /**
     * @brief Create a URMA context handle and its mock object.
     * @param[in] dev Mock device that owns the context.
     * @return Pair of ABI handle and mock context.
     */
    std::pair<urma_context_t *, MockContext *> CreateContextHandle(MockDevice *dev);

    /**
     * @brief Delete a mock context.
     * @param[in] ctx Context to delete.
     */
    void DeleteContext(MockContext *ctx);

    /**
     * @brief Create a JFC ABI handle and its mock completion queue.
     * @param[in] ctx Context that owns the JFC.
     * @return Pair of ABI handle and mock JFC.
     */
    std::pair<urma_jfc_t *, MockJfc *> CreateJfcHandle(const std::shared_ptr<MockContext> &ctx);

    /**
     * @brief Create a mock completion queue.
     * @param[in] ctx Context that owns the JFC.
     * @return Created JFC, or nullptr on failure.
     */
    MockJfc *CreateJfc(const std::shared_ptr<MockContext> &ctx);

    /**
     * @brief Delete a mock completion queue.
     * @param[in] jfc JFC to delete.
     */
    void DeleteJfc(MockJfc *jfc);

    /**
     * @brief Register a segment backed by real shared memory.
     * @param[in] businessVa Virtual address already mmaped by the business side. When non-zero, the mock tries to adopt
     * the matching memfd; otherwise it falls back to a shm_open-backed segment.
     * @param[in] ctx Context that owns the segment.
     * @param[in] size Segment size in bytes.
     * @param[in] key Segment key.
     * @param[out] rawOut Optional ABI target segment handle.
     * @return Created segment, or nullptr on failure.
     */
    MockSeg *RegisterSeg(MockContext *ctx, uint64_t size, const std::string &key, uint64_t businessVa = 0,
                         urma_target_seg_t **rawOut = nullptr);
    /**
     * @brief Unregister a mock segment.
     * @param[in] seg Segment to unregister.
     */
    void UnregisterSeg(MockSeg *seg);

    /**
     * @brief Create a jetty ABI handle and its mock object.
     * @param[in] ctx Context that owns the jetty.
     * @param[in] sendJfc Send completion queue.
     * @return Pair of ABI handle and mock jetty.
     */
    std::pair<urma_jetty_t *, MockJetty *> CreateJettyHandle(MockContext *ctx, MockJfc *sendJfc);

    /**
     * @brief Delete a mock jetty.
     * @param[in] jetty Jetty to delete.
     */
    void DeleteJetty(MockJetty *jetty);

    /**
     * @brief Import a target jetty and create its ABI handle.
     * @param[in] ctx Context that owns the imported target jetty.
     * @param[in] remoteSeg Remote segment associated with the target jetty.
     * @param[in] remoteRecvJfc Remote receive completion queue.
     * @param[in] token Import token string.
     * @return Pair of ABI handle and mock target jetty.
     */
    std::pair<urma_target_jetty_t *, MockTjetty *> ImportJettyHandle(MockContext *ctx, MockSeg *remoteSeg,
                                                                     MockJfc *remoteRecvJfc, const std::string &token);

    /**
     * @brief Unimport a target jetty.
     * @param[in] tjetty Target jetty to unimport.
     */
    void UnimportJetty(MockTjetty *tjetty);

    /**
     * @brief Import a target segment.
     * @param[in] ctx Context that owns the imported segment.
     * @param[in] size Segment size in bytes.
     * @param[in] token Import token string.
     * @param[in] key Segment key.
     * @param[in] remoteVa Optional expected remote segment address.
     * @param[out] rawOut Optional ABI target segment handle.
     * @return Imported segment, or nullptr on failure.
     */
    MockSeg *ImportSeg(MockContext *ctx, uint64_t size, const std::string &token, const std::string &key,
                       uint64_t remoteVa = 0, urma_target_seg_t **rawOut = nullptr);

    /**
     * @brief Unimport a target segment.
     * @param[in] seg Segment to unimport.
     */
    void UnimportSeg(MockSeg *seg);

    /**
     * @brief Submit a mock post-send work request.
     * @param[in] jetty Source jetty.
     * @param[in] wr Work request.
     * @param[out] badWr First invalid work request on failure.
     * @return URMA_SUCCESS on success; otherwise a mock URMA error code.
     */
    urma_status_t PostSendWr(MockJetty *jetty, const urma_jfs_wr_t *wr, urma_jfs_wr_t **badWr);

    /**
     * @brief Poll completion records from a mock JFC.
     * @param[in] jfc Completion queue to poll.
     * @param[in] maxCr Maximum records to return.
     * @param[out] outRecords Output completion records.
     * @return Number of records returned.
     */
    int PollJfc(MockJfc *jfc, int maxCr, urma_cr_t *outRecords);

    /**
     * @brief Acknowledge consumed completion events.
     * @param[in] jfc Completion queue.
     * @param[in] ackCnt Number of consumed events.
     */
    void AckJfc(MockJfc *jfc, uint32_t ackCnt);

    /**
     * @brief Rearm completion event notification.
     * @param[in] jfc Completion queue.
     * @param[in] enableEvents Whether event notification is enabled.
     */
    void RearmJfc(MockJfc *jfc, bool enableEvents);

    /**
     * @brief Get one injected async event.
     * @param[out] event Output async event.
     * @return URMA_SUCCESS when an event is returned.
     */
    urma_status_t GetAsyncEvent(urma_async_event_t *event);

    /**
     * @brief Allocate a process-local monotonic mock object id.
     */
    uint64_t NextId();

    /**
     * @brief Process-global thread pool for mock data-plane workers.
     * The pool is lazily initialized from the environment. It is intentionally process-global because static
     * destruction order is not guaranteed.
     */
    static MockThreadPool &Pool();

    /**
     * @brief Internal lock accessor for module free functions.
     * Callers that also need side tables must take this mutex before SideTables::mu. This accessor must not be exposed
     * through the public C ABI.
     */
    static std::mutex &Mu();

private:
    /**
     * @brief Construct the singleton backend.
     */
    MockUrmaBackend();

    /**
     * @brief Reset backend-owned state while the backend mutex is held.
     */
    void ResetStateLocked();

    std::atomic<bool> initialized_{ false };
    std::mutex mu_;
    std::unordered_map<uint64_t, std::shared_ptr<MockDevice>> devices_;
    std::atomic<uint64_t> nextId_{ 1 };
    pid_t ownerPid_ = 0;
};

}  // namespace urma_mock
}  // namespace datasystem

#endif  // USE_URMA_MOCK

#endif  // DATASYSTEM_COMMON_URMA_MOCK_URMA_MOCK_BACKEND_H
