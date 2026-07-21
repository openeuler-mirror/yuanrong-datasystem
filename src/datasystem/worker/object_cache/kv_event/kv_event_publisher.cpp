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
 * Description: KV event publisher implementation.
 */
#include "datasystem/worker/object_cache/kv_event/kv_event_publisher.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cctype>
#include <new>
#include <stdexcept>
#include <system_error>
#include <utility>

#include <nlohmann/json.hpp>

#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/object_cache/kv_event/zmq_lite.h"

namespace datasystem {
namespace object_cache {
namespace {
constexpr size_t K_MAX_BATCH_SIZE = 128;
constexpr uint32_t K_QUEUE_FULL_WARNING_LOG_EVERY_N = 100;

std::string StripNormalizeSuffix(const std::string &value)
{
    constexpr char normalizeSuffixSeparator[] = "__";
    constexpr size_t normalizeSuffixSeparatorSize = sizeof(normalizeSuffixSeparator) - 1;
    constexpr size_t normalizeSuffixHexSize = 16;
    constexpr size_t normalizeSuffixSize = normalizeSuffixSeparatorSize + normalizeSuffixHexSize;

    if (value.size() < normalizeSuffixSize) {
        return value;
    }
    auto suffixPos = value.size() - normalizeSuffixSize;
    if (value.compare(suffixPos, normalizeSuffixSeparatorSize, normalizeSuffixSeparator) != 0 ||
        !std::all_of(value.begin() + suffixPos + normalizeSuffixSeparatorSize, value.end(), [](char c) {
            return std::isxdigit(static_cast<unsigned char>(c)) != 0;
        })) {
        return value;
    }
    return value.substr(0, suffixPos);
}

std::optional<uint64_t> ParseSeqHashFromPoolKey(const std::string &objectKey)
{
    constexpr char cacheFamilyField[] = "@cache_family:";
    auto cacheFamilyPos = objectKey.find(cacheFamilyField);
    if (cacheFamilyPos == std::string::npos) {
        return std::nullopt;
    }

    auto chunkHashBegin = objectKey.find('@', cacheFamilyPos + sizeof(cacheFamilyField) - 1);
    if (chunkHashBegin == std::string::npos) {
        return std::nullopt;
    }
    auto chunkHashEnd = objectKey.find('@', chunkHashBegin + 1);
    if (chunkHashEnd == std::string::npos) {
        chunkHashEnd = objectKey.size();
    } else if (chunkHashEnd + 1 == objectKey.size() ||
               !std::all_of(objectKey.begin() + chunkHashEnd + 1, objectKey.end(),
                            [](char c) { return std::isdigit(static_cast<unsigned char>(c)) != 0; })) {
        return std::nullopt;
    }

    auto chunkHashSize = chunkHashEnd - chunkHashBegin - 1;
    auto chunkHashStart = objectKey.begin() + chunkHashBegin + 1;
    if (chunkHashSize == 0 ||
        !std::all_of(chunkHashStart, objectKey.begin() + chunkHashEnd,
                     [](char c) { return std::isxdigit(static_cast<unsigned char>(c)) != 0; })) {
        return std::nullopt;
    }

    constexpr size_t uint64HexSize = sizeof(uint64_t) * 2;
    auto low64HexSize = std::min(chunkHashSize, uint64HexSize);
    auto low64Hex = objectKey.substr(chunkHashEnd - low64HexSize, low64HexSize);
    try {
        constexpr int hexadecimalBase = 16;
        return static_cast<uint64_t>(std::stoull(low64Hex, nullptr, hexadecimalBase));
    } catch (const std::exception &) {
        return std::nullopt;
    }
}

nlohmann::json NullableString(const std::string &value)
{
    if (value.empty()) {
        return nullptr;
    }
    return value;
}

nlohmann::json NullableUint32(uint32_t value)
{
    if (value == 0) {
        return nullptr;
    }
    return value;
}

std::array<uint8_t, sizeof(uint64_t)> ToBigEndian(uint64_t value)
{
    constexpr unsigned int bitsPerByte = 8;
    constexpr uint64_t byteMask = 0xff;

    std::array<uint8_t, sizeof(uint64_t)> bytes{};
    for (size_t i = 0; i < bytes.size(); ++i) {
        auto shift = static_cast<unsigned int>((bytes.size() - i - 1) * bitsPerByte);
        bytes[i] = static_cast<uint8_t>((value >> shift) & byteMask);
    }
    return bytes;
}

struct EventJsonInput {
    const KvEventPublisher::ParsedKey &parsed;
    const std::string &medium;
    const char *eventType;
    const char *legacyType;
    bool hasStoredFields;
    uint64_t eventId;
    uint64_t timestamp;
};

nlohmann::json BuildEventJson(const KvEventConfig &config, const EventJsonInput &input)
{
    nlohmann::json event = {
        { "event_id", input.eventId },
        { "timestamp", input.timestamp },
        { "event_type", input.eventType },
        { "model_name", NullableString(config.modelName) },
        { "block_size", NullableUint32(config.blockSize) },
        { "additional_salt", NullableString(config.additionalSalt) },
        { "lora_name", NullableString(config.loraName) },
        { "tenant_id", input.parsed.tenantId },
        { "backend_id", config.backendId },
        { "medium", input.medium },
        { "dp_rank", config.dpRank },
        { "seq_hashes", nlohmann::json::array({ input.parsed.seqHash }) },
        { "base_block_idx", nullptr },
    };

    if (input.hasStoredFields) {
        event["parent_hash"] = nullptr;
        event["token_ids"] = nullptr;
    }
    if (config.emitLegacyCompatFields) {
        event["type"] = input.legacyType;
        event["block_hashes"] = nlohmann::json::array({ input.parsed.seqHash });
        if (input.hasStoredFields) {
            event["parent_block_hash"] = nullptr;
        }
    }
    return event;
}

template <typename T>
bool ReadKvEventConfigField(const nlohmann::json &value, const char *key, T &field)
{
    try {
        field = value.value(key, field);
        return true;
    } catch (const nlohmann::json::exception &e) {
        LOG(ERROR) << "Invalid kv_events_config field type for key '" << key << "': " << e.what();
        return false;
    }
}
}  // namespace

KvEventConfig BuildKvEventConfigFromJsonString(const std::string &jsonConfig)
{
    KvEventConfig config;
    if (jsonConfig.empty()) {
        return config;
    }

    nlohmann::json value;
    try {
        value = nlohmann::json::parse(jsonConfig);
    } catch (const nlohmann::json::exception &e) {
        LOG(ERROR) << "Failed to parse kv_events_config: " << e.what();
        return config;
    }

    if (!value.is_object()) {
        LOG(ERROR) << "kv_events_config must be a JSON object.";
        return config;
    }

    if (!ReadKvEventConfigField(value, "bind_endpoint", config.bindEndpoint)
        || !ReadKvEventConfigField(value, "model_name", config.modelName)
        || !ReadKvEventConfigField(value, "backend_id", config.backendId)
        || !ReadKvEventConfigField(value, "tenant_id", config.tenantId)
        || !ReadKvEventConfigField(value, "additional_salt", config.additionalSalt)
        || !ReadKvEventConfigField(value, "lora_name", config.loraName)
        || !ReadKvEventConfigField(value, "block_size", config.blockSize)
        || !ReadKvEventConfigField(value, "dp_rank", config.dpRank)
        || !ReadKvEventConfigField(value, "emit_legacy_compat_fields", config.emitLegacyCompatFields)
        || !ReadKvEventConfigField(value, "queue_capacity", config.queueCapacity)) {
        return config;
    }

    bool isValid = true;
    if (config.bindEndpoint.empty()) {
        LOG(ERROR) << "kv_events_config.bind_endpoint is required.";
        isValid = false;
    }
    if (config.backendId.empty()) {
        LOG(ERROR) << "kv_events_config.backend_id is required.";
        isValid = false;
    }
    if (config.queueCapacity == 0) {
        LOG(ERROR) << "kv_events_config.queue_capacity must be greater than 0.";
        isValid = false;
    }
    if (!isValid) {
        return config;
    }
    config.enabled = true;
    return config;
}

KvEventPublisher::KvEventPublisher(KvEventConfig config) : config_(std::move(config))
{
    int rc = bthread_cond_init(&queueCv_, nullptr);
    if (rc != 0) {
        throw std::system_error(rc, std::system_category(), "Failed to initialize KV event queue condition variable");
    }
    try {
        Initialize();
    } catch (...) {
        (void)bthread_cond_destroy(&queueCv_);
        throw;
    }
}

KvEventPublisher::~KvEventPublisher()
{
    Stop();
    (void)bthread_cond_destroy(&queueCv_);
}

bool KvEventPublisher::Enabled() const
{
    return config_.enabled;
}

void PublishKvStoredEvent(const std::shared_ptr<KvEventPublisher> &publisher, const std::string &objectKey,
                          std::string_view medium)
{
    if (publisher != nullptr) {
        publisher->PublishStored(objectKey, medium);
    }
}

void PublishKvRemovedEvent(const std::shared_ptr<KvEventPublisher> &publisher, const std::string &objectKey,
                           std::string_view medium)
{
    if (publisher != nullptr) {
        publisher->PublishRemoved(objectKey, medium);
    }
}

void KvEventPublisher::PublishStored(std::string objectKey, std::string_view medium)
{
    if (!config_.enabled || stop_.load(std::memory_order_acquire)) {
        return;
    }
    Enqueue(PendingEvent{ EventKind::STORED, std::move(objectKey), std::string(medium) });
}

void KvEventPublisher::PublishRemoved(std::string objectKey, std::string_view medium)
{
    if (!config_.enabled || stop_.load(std::memory_order_acquire)) {
        return;
    }
    Enqueue(PendingEvent{ EventKind::REMOVED, std::move(objectKey), std::string(medium) });
}

KvEventPublisher::Stats KvEventPublisher::GetStats() const
{
    return Stats{ publishedBatches_.load(std::memory_order_relaxed),
                  publishedEvents_.load(std::memory_order_relaxed),
                  droppedEvents_.load(std::memory_order_relaxed),
                  skippedUnparsedKeys_.load(std::memory_order_relaxed) };
}

std::optional<KvEventPublisher::ParsedKey> KvEventPublisher::ParseKey(const std::string &objectKey,
                                                                      const std::string &tenantId)
{
    std::string parsedTenantId = TenantAuthManager::ExtractTenantId(objectKey);
    if (parsedTenantId.empty()) {
        parsedTenantId = tenantId;
    }
    std::string realObjectKey = StripNormalizeSuffix(TenantAuthManager::ExtractRealObjectKey(objectKey));
    auto seqHash = ParseSeqHashFromPoolKey(realObjectKey);
    VLOG(1) << "Parsed KV event key, objectKey: " << objectKey << ", tenantId: " << parsedTenantId
            << ", realObjectKey: " << realObjectKey << ", hasSeq: " << (seqHash.has_value() ? "true" : "false")
            << (seqHash.has_value() ? ", seqHash: " + std::to_string(*seqHash) : "");
    if (!seqHash.has_value()) {
        return std::nullopt;
    }
    return ParsedKey{ parsedTenantId, realObjectKey, *seqHash };
}

KvEventPublisher::EventKindInfo KvEventPublisher::ResolveEventKind(EventKind kind)
{
    static constexpr char storedEventType[] = "stored";
    static constexpr char removedEventType[] = "removed";
    static constexpr char storedLegacyType[] = "BlockStored";
    static constexpr char removedLegacyType[] = "BlockRemoved";

    switch (kind) {
        case EventKind::STORED:
            return EventKindInfo{ storedEventType, storedLegacyType, true };
        case EventKind::REMOVED:
            return EventKindInfo{ removedEventType, removedLegacyType, false };
    }
    return EventKindInfo{ removedEventType, removedLegacyType, false };
}

void KvEventPublisher::Initialize()
{
    if (!config_.enabled) {
        return;
    }
    if (config_.bindEndpoint.empty() || config_.backendId.empty()) {
        LOG(ERROR) << "KV event publisher config is invalid.";
        DisableAndClose();
        return;
    }

    zmqContext_ = std::make_unique<ZmqLiteContext>();
    auto rc = zmqContext_->Init();
    if (rc.IsError()) {
        LOG(ERROR) << "Failed to initialize KV event publisher: " << rc.ToString();
        DisableAndClose();
        return;
    }

    auto socket = zmqContext_->CreateZmqSocket(ZmqLiteSocketType::PUB);
    if (!socket.IsValid()) {
        LOG(ERROR) << "Failed to create KV event PUB socket.";
        DisableAndClose();
        return;
    }
    zmqSocket_ = std::make_unique<ZmqLiteSocket>(std::move(socket));

    constexpr int noLinger = 0;
    rc = zmqSocket_->SetLinger(noLinger);
    if (rc.IsError()) {
        LOG(ERROR) << "Failed to set KV event PUB linger: " << rc.ToString();
        DisableAndClose();
        return;
    }
    rc = zmqSocket_->Bind(config_.bindEndpoint);
    if (rc.IsError()) {
        LOG(ERROR) << "Failed to bind KV event PUB endpoint " << config_.bindEndpoint << ": " << rc.ToString();
        DisableAndClose();
        return;
    }

    worker_ = Thread(&KvEventPublisher::WorkerLoop, this);
    worker_.set_name("KvEventPub");
    LOG(INFO) << "KV event publisher started at " << config_.bindEndpoint;
}

void KvEventPublisher::DisableAndClose()
{
    config_.enabled = false;
    zmqSocket_.reset();
    if (zmqContext_ != nullptr) {
        zmqContext_->Close(false);
    }
}

void KvEventPublisher::Stop()
{
    {
        std::lock_guard<bthread::Mutex> lock(queueMutex_);
        stop_.store(true, std::memory_order_release);
    }
    (void)bthread_cond_broadcast(&queueCv_);
    if (worker_.joinable()) {
        worker_.join();
    }
    zmqSocket_.reset();
    if (zmqContext_ != nullptr) {
        zmqContext_->Close(false);
    }
}

void KvEventPublisher::Enqueue(PendingEvent event)
{
    uint64_t droppedCount = 0;
    {
        std::lock_guard<bthread::Mutex> lock(queueMutex_);
        if (queue_.size() >= config_.queueCapacity) {
            droppedCount = droppedEvents_.fetch_add(1, std::memory_order_relaxed) + 1;
        } else {
            queue_.push_back(std::move(event));
        }
    }
    if (droppedCount != 0) {
        METRIC_INC(metrics::KvMetricId::WORKER_KV_EVENT_DROPPED_TOTAL);
        LOG_FIRST_AND_EVERY_N(WARNING, K_QUEUE_FULL_WARNING_LOG_EVERY_N)
            << "KV event publisher queue is full; event dropped, eventType="
            << ResolveEventKind(event.kind).eventType << ", objectKey=" << event.objectKey
            << ", droppedEvents=" << droppedCount
            << ", queueCapacity=" << config_.queueCapacity;
        return;
    }
    (void)bthread_cond_signal(&queueCv_);
}

void KvEventPublisher::WorkerLoop()
{
    bool shouldExit = false;
    while (!shouldExit) {
        try {
            auto batch = TakeBatch(shouldExit);
            if (!batch.empty()) {
                PublishBatch(batch);
            }
        } catch (const std::exception &e) {
            LOG(ERROR) << "Failed to process KV event batch: " << e.what();
        } catch (...) {
            LOG(ERROR) << "Failed to process KV event batch due to an unknown exception";
        }
    }
}

std::vector<KvEventPublisher::PendingEvent> KvEventPublisher::TakeBatch(bool &shouldExit)
{
    std::vector<PendingEvent> batch;
    std::unique_lock<bthread::Mutex> lock(queueMutex_);
    while (!stop_.load(std::memory_order_acquire) && queue_.empty()) {
        (void)bthread_cond_wait(&queueCv_, queueMutex_.native_handler());
    }
    if (queue_.empty() && stop_.load(std::memory_order_acquire)) {
        shouldExit = true;
        return batch;
    }
    INJECT_POINT_NO_RETURN("KvEventPublisher.TakeBatch.beforeMove", [] { throw std::bad_alloc(); });
    while (!queue_.empty() && batch.size() < K_MAX_BATCH_SIZE) {
        batch.push_back(std::move(queue_.front()));
        queue_.pop_front();
    }
    return batch;
}

void KvEventPublisher::PublishBatch(const std::vector<PendingEvent> &batch)
{
    if (!config_.enabled || zmqSocket_ == nullptr || !zmqSocket_->IsValid()) {
        return;
    }

    auto now = std::chrono::system_clock::now().time_since_epoch();
    auto timestamp = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
    nlohmann::json events = nlohmann::json::array();
    for (const auto &pending : batch) {
        auto parsed = ParseKey(pending.objectKey, config_.tenantId);
        if (!parsed.has_value()) {
            skippedUnparsedKeys_.fetch_add(1, std::memory_order_relaxed);
            METRIC_INC(metrics::KvMetricId::WORKER_KV_EVENT_SKIPPED_UNPARSED_KEYS_TOTAL);
            continue;
        }

        auto eventId = nextEventId_.fetch_add(1, std::memory_order_relaxed);
        auto kindInfo = ResolveEventKind(pending.kind);
        EventJsonInput input{ *parsed, pending.medium, kindInfo.eventType, kindInfo.legacyType,
                              kindInfo.hasStoredFields, eventId, timestamp };
        events.push_back(BuildEventJson(config_, input));
    }

    if (events.empty()) {
        return;
    }

    nlohmann::json payload = nlohmann::json::array({ timestamp, events, config_.dpRank });
    std::vector<uint8_t> packed;
    try {
        packed = nlohmann::json::to_msgpack(payload);
    } catch (const std::exception &e) {
        LOG(ERROR) << "Failed to encode KV event payload: " << e.what();
        return;
    }

    auto sequence = nextZmqSequence_.fetch_add(1, std::memory_order_relaxed);
    Status rc = SendPayload(sequence, packed);
    if (rc.IsError()) {
        LOG(ERROR) << "Failed to send KV event batch: " << rc.ToString();
        return;
    }

    publishedBatches_.fetch_add(1, std::memory_order_relaxed);
    publishedEvents_.fetch_add(events.size(), std::memory_order_relaxed);
    METRIC_INC(metrics::KvMetricId::WORKER_KV_EVENT_PUBLISHED_BATCHES_TOTAL);
    METRIC_ADD(metrics::KvMetricId::WORKER_KV_EVENT_PUBLISHED_EVENTS_TOTAL, events.size());
    VLOG(1) << "Published KV event batch, sequence: " << sequence << ", event count: " << events.size()
            << ", payload bytes: " << packed.size() << ", payload: " << payload.dump();
}

Status KvEventPublisher::SendPayload(uint64_t sequence, const std::vector<uint8_t> &packed)
{
    auto seqFrame = ToBigEndian(sequence);
    ZmqLiteMessage topicMsg;
    ZmqLiteMessage sequenceMsg;
    ZmqLiteMessage payloadMsg;

    // Prepare every frame before starting the multipart send. This prevents an allocation failure while copying a
    // later frame from leaving the socket with an unfinished multipart message.
    RETURN_IF_NOT_OK(topicMsg.CopyBuffer(nullptr, 0));
    RETURN_IF_NOT_OK(sequenceMsg.CopyBuffer(seqFrame.data(), seqFrame.size()));
    RETURN_IF_NOT_OK(payloadMsg.CopyBuffer(packed.data(), packed.size()));

    RETURN_IF_NOT_OK(zmqSocket_->SendMsg(topicMsg, ZmqLiteSendFlags::SNDMORE));
    RETURN_IF_NOT_OK(zmqSocket_->SendMsg(sequenceMsg, ZmqLiteSendFlags::SNDMORE));
    return zmqSocket_->SendMsg(payloadMsg, ZmqLiteSendFlags::NONE);
}
}  // namespace object_cache
}  // namespace datasystem
