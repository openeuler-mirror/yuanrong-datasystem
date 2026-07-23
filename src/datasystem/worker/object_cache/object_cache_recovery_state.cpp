/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Object-cache recovery state aggregation.
 */
#include "datasystem/worker/object_cache/object_cache_recovery_state.h"

#include <utility>

#include "datasystem/worker/object_cache/object_cache_recovery_evidence.h"
#include "datasystem/worker/runtime/worker_recovery_evidence_tracker.h"

namespace datasystem {
namespace object_cache {

ObjectCacheRecoveryState::ObjectCacheRecoveryState()
    : lastMetadataRecoveryEvidence_(BuildMetadataRecoveryEvidenceReport(MetaDataRecoveryManager::RecoverySummary{})),
      lastOwnershipRecoveryEvidence_(BuildOwnershipRecoveryEvidenceReport(true, "no ownership reconciliation pending")),
      recoveryEvidenceTracker_(std::make_unique<worker::WorkerRecoveryEvidenceTracker>())
{
}

ObjectCacheRecoveryState::~ObjectCacheRecoveryState() = default;

worker::WorkerRecoveryEvidenceReport ObjectCacheRecoveryState::GetLastMetadataRecoveryEvidenceReport() const
{
    std::lock_guard<std::mutex> lock(metadataRecoveryEvidenceMutex_);
    return lastMetadataRecoveryEvidence_;
}

void ObjectCacheRecoveryState::SetMetadataRecoverySummary(const MetaDataRecoveryManager::RecoverySummary &summary)
{
    auto metadataReport = BuildMetadataRecoveryEvidenceReport(summary);
    SetMetadataRecoveryEvidenceReport(metadataReport);
    SetOwnershipRecoveryEvidenceReport(BuildOwnershipRecoveryEvidenceReport(
        metadataReport.evidence.metadataReady, metadataReport.evidence.metadataReady
                                                   ? "metadata owner reconciliation confirmed"
                                                   : "metadata owner reconciliation incomplete"));
}

void ObjectCacheRecoveryState::SetMetadataRecoveryEvidenceReport(worker::WorkerRecoveryEvidenceReport report)
{
    std::lock_guard<std::mutex> lock(metadataRecoveryEvidenceMutex_);
    lastMetadataRecoveryEvidence_ = std::move(report);
}

worker::WorkerRecoveryEvidenceReport ObjectCacheRecoveryState::GetLastOwnershipRecoveryEvidenceReport() const
{
    std::lock_guard<std::mutex> lock(ownershipRecoveryEvidenceMutex_);
    return lastOwnershipRecoveryEvidence_;
}

void ObjectCacheRecoveryState::SetOwnershipRecoveryEvidenceReport(worker::WorkerRecoveryEvidenceReport report)
{
    std::lock_guard<std::mutex> lock(ownershipRecoveryEvidenceMutex_);
    lastOwnershipRecoveryEvidence_ = std::move(report);
}

void ObjectCacheRecoveryState::MarkOwnershipReconciliationReady(const std::string &detail)
{
    worker::WorkerRecoveryEvidenceBuilder builder;
    builder.MarkMetadataReady(detail);
    SetMetadataRecoveryEvidenceReport(builder.BuildReport(detail));
    SetOwnershipRecoveryEvidenceReport(BuildOwnershipRecoveryEvidenceReport(true, detail));

    std::function<void()> handler;
    {
        std::lock_guard<std::mutex> lock(recoveryEvidenceReadyHandlerMutex_);
        handler = recoveryEvidenceReadyHandler_;
    }
    if (handler != nullptr) {
        handler();
    }
}

void ObjectCacheRecoveryState::RegisterRecoveryEvidenceReadyHandler(std::function<void()> handler)
{
    std::lock_guard<std::mutex> lock(recoveryEvidenceReadyHandlerMutex_);
    recoveryEvidenceReadyHandler_ = std::move(handler);
}

uint64_t ObjectCacheRecoveryState::MarkResourceRecoveryRequired(memory::CacheType cacheType)
{
    std::lock_guard<std::mutex> lock(resourceRecoveryMutex_);
    if (cacheType == memory::CacheType::DISK) {
        diskRecoveryRequired_ = true;
    } else {
        memoryRecoveryRequired_ = true;
    }
    return ++resourceRecoveryGeneration_;
}

ObjectCacheRecoveryState::ResourceRecoverySnapshot ObjectCacheRecoveryState::GetResourceRecoverySnapshot() const
{
    std::lock_guard<std::mutex> lock(resourceRecoveryMutex_);
    return { memoryRecoveryRequired_, diskRecoveryRequired_, resourceRecoveryGeneration_ };
}

bool ObjectCacheRecoveryState::PublishResourceRecoveryIfCurrent(uint64_t generation,
                                                                const std::function<bool()> &publish)
{
    std::lock_guard<std::mutex> lock(resourceRecoveryMutex_);
    if (generation != resourceRecoveryGeneration_) {
        return false;
    }
    const bool open = publish();
    if (open) {
        memoryRecoveryRequired_ = false;
        diskRecoveryRequired_ = false;
    }
    return open;
}

worker::WorkerRecoveryEvidenceReport ObjectCacheRecoveryState::BuildObjectCacheRecoveryEvidenceReport(
    const SlotRecoveryEvidenceProvider &slotEvidenceProvider,
    const OwnershipRecoveryEvidenceProvider &ownershipEvidenceProvider,
    const ResourceRecoveredProvider &resourceRecovered, uint64_t *resourceRecoveryGeneration) const
{
    const auto metadataReport = GetLastMetadataRecoveryEvidenceReport();
    worker::WorkerRecoveryEvidenceBuilder builder;
    const auto slotReport =
        slotEvidenceProvider == nullptr ? builder.BuildReport("slot_manager_unavailable") : slotEvidenceProvider();
    const auto ownershipReport = ownershipEvidenceProvider == nullptr
                                     ? builder.BuildReport("ownership_evidence_unavailable")
                                     : ownershipEvidenceProvider();
    const auto resourceSnapshot = GetResourceRecoverySnapshot();
    if (resourceRecoveryGeneration != nullptr) {
        *resourceRecoveryGeneration = resourceSnapshot.generation;
    }
    const bool memoryReady =
        !resourceSnapshot.memoryRequired || (resourceRecovered != nullptr && resourceRecovered(CacheType::MEMORY));
    const bool diskReady =
        !resourceSnapshot.diskRequired || (resourceRecovered != nullptr && resourceRecovered(CacheType::DISK));
    return object_cache::BuildObjectCacheRecoveryEvidenceReport(metadataReport, slotReport, ownershipReport,
                                                                memoryReady && diskReady);
}

worker::WorkerRecoveryGeneration ObjectCacheRecoveryState::BeginRecoveryEvidenceGeneration(const std::string &detail)
{
    auto generation = recoveryEvidenceTracker_->BeginRecovery(detail);
    worker::WorkerRecoveryEvidenceBuilder builder;
    SetMetadataRecoveryEvidenceReport(builder.BuildReport(detail));
    SetOwnershipRecoveryEvidenceReport(builder.BuildReport("ownership reconciliation pending"));
    return generation;
}

worker::WorkerRecoveryEvidenceReport ObjectCacheRecoveryState::TrackEvidenceForGeneration(
    worker::WorkerRecoveryGeneration generation, worker::WorkerRecoveryEvidenceReport report)
{
    if (!recoveryEvidenceTracker_->UpdateEvidence(generation, std::move(report))) {
        return worker::WorkerRecoveryEvidenceReport{};
    }
    auto evidence = recoveryEvidenceTracker_->GetEvidence(generation);
    return evidence.has_value() ? evidence->report : worker::WorkerRecoveryEvidenceReport{};
}

}  // namespace object_cache
}  // namespace datasystem
