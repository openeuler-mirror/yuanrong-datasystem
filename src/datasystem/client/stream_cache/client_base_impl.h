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

/**
 * Description: Implement stream cache base class for producer and consumer.
 */
#ifndef DATASYSTEM_CLIENT_STREAM_CACHE_CLIENT_BASE_IMPL_H
#define DATASYSTEM_CLIENT_STREAM_CACHE_CLIENT_BASE_IMPL_H

#include <string>
#include "datasystem/client/listen_worker.h"
#include "datasystem/client/mmap_manager.h"
#include "datasystem/client/stream_cache/client_worker_api.h"
#include "datasystem/client/stream_cache/stream_client_impl.h"
#include "datasystem/client/stream_cache/producer_consumer_worker_api.h"
#include "datasystem/common/util/thread_local.h"

namespace datasystem {
namespace client {
namespace stream_cache {
class FirstCallTracer {
public:
    bool NeedWriteLog(bool ready)
    {
        bool needWriteLog = false;
        switch (state_) {
            case State::INIT:
                if (ready) {
                    state_ = State::CONTINUE;
                } else {
                    state_ = State::WAIT_FIRST;
                }
                needWriteLog = true;
                break;
            case State::WAIT_FIRST:
                if (ready) {
                    state_ = State::CONTINUE;
                    needWriteLog = true;
                }
                break;
            case State::CONTINUE:
                break;
        }
        return needWriteLog;
    }

private:
    enum class State { INIT, WAIT_FIRST, CONTINUE };
    State state_{ State::INIT };
};
class ClientBaseImpl {
public:
    enum class State { CLOSE, NORMAL, RESET };
    ClientBaseImpl(std::string streamName, std::string tenantId, std::shared_ptr<ProducerConsumerWorkerApi> workerApi,
                   std::shared_ptr<StreamClientImpl> client, MmapManager *mmapManager,
                   std::shared_ptr<client::ListenWorker> listenWorker);
    virtual ~ClientBaseImpl();

    /**
     * @brief Get the stream name for the consumer.
     * @return stream name for the consumer.
     */
    const std::string &GetStreamName();

    /**
     * @brief Set the worker disconnect.
     */
    virtual void SetInactive();

    /**
     * @brief Getter of state_.
     * @return Return state_.
     */
    bool IsActive() const;

    /**
     * @brief Log helper. Creates the prefix for log messages.
     * @return The generated log prefix for this Producer.
     */
    virtual std::string LogPrefix() const = 0;

    /**
     * @brief Get the Tenant Id object.
     * @return std::string Tenant id.
     */
    std::string GetTenantId()
    {
        return tenantId_;
    }

    /**
     * @brief CheckStreamNameAndTenantId
     * @param[in] streamName streamName
     * @param[in] tenantId tenantId
     * @return true if check success
     */
    bool CheckStreamNameAndTenantId(const std::string &streamName, const std::string &tenantId);

    /**
     * @brief Get the stream name
     * @return Stream name.
     */
    const std::string &GetStreamName() const
    {
        return streamName_;
    }

    /**
     * @brief check if another thread is using the client (producer or consumer), if not, the calling thread will set
     *        inUse_ to true and use the client.
     * @return Status of the call.
     */
    Status CheckAndSetInUse();

    /**
     * @brief Set inUse_ to false. The calling thread needs to call CheckAndSetInUse() before using the client
     *        (producer or consumer) again.
     */
    void UnsetInUse();

    /**
     * @brief Check if the work area is downlevel
     */
    bool WorkAreaIsV2() const;
protected:
    /**
     * Base class initialization
     * @return
     */
    virtual Status Init();

    /**
     * State changing function
     */
    Status ChangeState(State newState);

    /**
     * @brief Check the state_ is NORMAL
     * @return Status of the call.
     */
    virtual Status CheckNormalState() const;

    /**
     * @brief Check some of the states are normal, used for certain situations.
     * @return Status of the call.
     */
    virtual Status CheckState() const;

    Status GetShmInfo(const ShmView &shmView, std::shared_ptr<ShmUnitInfo> &out,
                      std::shared_ptr<client::MmapTableEntry> &mmapEntry);

    const std::string streamName_;
    std::shared_ptr<StreamClientImpl> client_;
    std::shared_ptr<client::stream_cache::ProducerConsumerWorkerApi> workerApi_;
    client::MmapManager *mmapManager_;
    std::shared_ptr<client::ListenWorker> listenWorker_{ nullptr };
    uint32_t lockId_;
    mutable std::mutex recvFdsMutex_;
    std::unordered_map<int, std::shared_ptr<ShmUnitInfo>> recvFds_;
    std::atomic<State> state_;
    // A work area that is shared between the corresponding worker::stream_cache::Consumer
    // sz is the size of this work area. It is set up by the worker.
    ShmView workArea_;
    std::unique_ptr<Cursor> cursor_;
    std::string tenantId_;
    uint32_t workerVersion_ = 0;
private:
    // True if there is at least 1 thread actively using the client (producer or consumer).
    // There should be at most 1 thread actively using the client (producer or consumer) since it is not thread-safe.
    std::atomic<bool> inUse_{ false };
};
}  // namespace stream_cache
}  // namespace client
}  // namespace datasystem
#endif  // DATASYSTEM_CLIENT_STREAM_CACHE_CLIENT_BASE_IMPL_H
