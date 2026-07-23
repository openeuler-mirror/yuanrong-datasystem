/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Object-cache recovery state aggregation.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_OBJECT_CACHE_RECOVERY_STATE_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_OBJECT_CACHE_RECOVERY_STATE_H

#include <cstdint>
#include <functional>
#include <mutex>
#include <string>

#include "datasystem/common/shared_memory/arena_group_key.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/worker/object_cache/metadata_recovery_manager.h"
#include "datasystem/worker/runtime/worker_recovery_evidence.h"

namespace datasystem {
namespace object_cache {

class ObjectCacheRecoveryState {
public:
    using SlotRecoveryEvidenceProvider = std::function<worker::WorkerRecoveryEvidenceReport()>;
    using OwnershipRecoveryEvidenceProvider = std::function<worker::WorkerRecoveryEvidenceReport()>;
    using ResourceRecoveredProvider = std::function<bool(CacheType)>;

    struct ResourceRecoverySnapshot {
        bool memoryRequired{ false };
        bool diskRequired{ false };
        uint64_t generation{ 0 };
    };

    ObjectCacheRecoveryState();
    ~ObjectCacheRecoveryState();

    ObjectCacheRecoveryState(const ObjectCacheRecoveryState &) = delete;
    ObjectCacheRecoveryState &operator=(const ObjectCacheRecoveryState &) = delete;

    worker::WorkerRecoveryEvidenceReport GetLastMetadataRecoveryEvidenceReport() const;
    void SetMetadataRecoverySummary(const MetaDataRecoveryManager::RecoverySummary &summary);
    void SetMetadataRecoveryEvidenceReport(worker::WorkerRecoveryEvidenceReport report);
    worker::WorkerRecoveryEvidenceReport GetLastOwnershipRecoveryEvidenceReport() const;
    void SetOwnershipRecoveryEvidenceReport(worker::WorkerRecoveryEvidenceReport report);
    void MarkOwnershipReconciliationReady(const std::string &detail);
    void RegisterRecoveryEvidenceReadyHandler(std::function<void()> handler);

    uint64_t MarkResourceRecoveryRequired(memory::CacheType cacheType);
    ResourceRecoverySnapshot GetResourceRecoverySnapshot() const;
    bool PublishResourceRecoveryIfCurrent(uint64_t generation, const std::function<bool()> &publish);
    worker::WorkerRecoveryEvidenceReport BuildObjectCacheRecoveryEvidenceReport(
        const SlotRecoveryEvidenceProvider &slotEvidenceProvider,
        const OwnershipRecoveryEvidenceProvider &ownershipEvidenceProvider,
        const ResourceRecoveredProvider &resourceRecovered, uint64_t *resourceRecoveryGeneration = nullptr) const;

    worker::WorkerRecoveryGeneration BeginRecoveryEvidenceGeneration(const std::string &detail);
    worker::WorkerRecoveryEvidenceReport TrackEvidenceForGeneration(worker::WorkerRecoveryGeneration generation,
                                                                    worker::WorkerRecoveryEvidenceReport report);

private:
    mutable std::mutex metadataRecoveryEvidenceMutex_;
    worker::WorkerRecoveryEvidenceReport lastMetadataRecoveryEvidence_;
    mutable std::mutex ownershipRecoveryEvidenceMutex_;
    worker::WorkerRecoveryEvidenceReport lastOwnershipRecoveryEvidence_;

    mutable std::mutex resourceRecoveryMutex_;
    bool memoryRecoveryRequired_{ false };
    bool diskRecoveryRequired_{ false };
    uint64_t resourceRecoveryGeneration_{ 0 };

    mutable std::mutex recoveryEvidenceGenerationMutex_;
    worker::WorkerRecoveryGeneration recoveryEvidenceGeneration_{ 0 };
    worker::WorkerRecoveryEvidenceReport recoveryEvidenceReport_;

    mutable std::mutex recoveryEvidenceReadyHandlerMutex_;
    std::function<void()> recoveryEvidenceReadyHandler_;
};

}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_OBJECT_CACHE_RECOVERY_STATE_H
