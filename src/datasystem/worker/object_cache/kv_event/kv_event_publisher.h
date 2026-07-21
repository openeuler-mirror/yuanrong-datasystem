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

/**
 * Description: KV event publisher for RFC #1527 compatible indexers.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_KV_EVENT_PUBLISHER_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_KV_EVENT_PUBLISHER_H

#include <bthread/bthread.h>

#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "datasystem/common/util/thread.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/object_cache/kv_event/kv_event_config.h"
#include "datasystem/worker/object_cache/kv_event/zmq_lite.h"

namespace datasystem {
namespace object_cache {

class KvEventPublisher {
public:
    explicit KvEventPublisher(KvEventConfig config);
    ~KvEventPublisher();

    KvEventPublisher(const KvEventPublisher &) = delete;
    KvEventPublisher &operator=(const KvEventPublisher &) = delete;

    bool Enabled() const;

    void PublishStored(std::string objectKey, std::string_view medium);
    void PublishRemoved(std::string objectKey, std::string_view medium);

    struct Stats {
        uint64_t publishedBatches{ 0 };
        uint64_t publishedEvents{ 0 };
        uint64_t droppedEvents{ 0 };
        uint64_t skippedUnparsedKeys{ 0 };
    };

    Stats GetStats() const;

    struct ParsedKey {
        std::string tenantId;
        std::string realObjectKey;
        uint64_t seqHash{ 0 };
    };

    static std::optional<ParsedKey> ParseKey(const std::string &objectKey, const std::string &tenantId);

private:
    enum class EventKind { STORED, REMOVED };

    struct EventKindInfo {
        const char *eventType;
        const char *legacyType;
        bool hasStoredFields;
    };

    struct PendingEvent {
        EventKind kind;
        std::string objectKey;
        std::string medium;
    };

    static EventKindInfo ResolveEventKind(EventKind kind);

    void Initialize();
    void DisableAndClose();
    void Stop();
    void Enqueue(PendingEvent event);
    void WorkerLoop();
    std::vector<PendingEvent> TakeBatch(bool &shouldExit);
    void PublishBatch(const std::vector<PendingEvent> &batch);
    Status SendPayload(uint64_t sequence, const std::vector<uint8_t> &packed);

    KvEventConfig config_;
    std::unique_ptr<ZmqLiteContext> zmqContext_;
    std::unique_ptr<ZmqLiteSocket> zmqSocket_;
    Thread worker_;

    bthread::Mutex queueMutex_;
    std::deque<PendingEvent> queue_;
    bthread_cond_t queueCv_;

    std::atomic<bool> stop_{ false };
    std::atomic<uint64_t> nextEventId_{ 1 };
    std::atomic<uint64_t> nextZmqSequence_{ 1 };

    std::atomic<uint64_t> publishedBatches_{ 0 };
    std::atomic<uint64_t> publishedEvents_{ 0 };
    std::atomic<uint64_t> droppedEvents_{ 0 };
    std::atomic<uint64_t> skippedUnparsedKeys_{ 0 };
};

void PublishKvStoredEvent(const std::shared_ptr<KvEventPublisher> &publisher, const std::string &objectKey,
                          std::string_view medium);
void PublishKvRemovedEvent(const std::shared_ptr<KvEventPublisher> &publisher, const std::string &objectKey,
                           std::string_view medium);

}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_KV_EVENT_PUBLISHER_H
