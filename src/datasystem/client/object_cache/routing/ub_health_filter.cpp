/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include "datasystem/client/object_cache/routing/ub_health_filter.h"

#include <exception>
#include <stdexcept>
#include <unordered_set>
#include <utility>

#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/object_cache/provider_ub_failure_detail.h"
#include "datasystem/common/util/timer.h"

namespace datasystem::client {
namespace {
constexpr int UB_RECOVERY_REJECT_LOG_EVERY_N = 100;
}  // namespace

UbHealthFilter::UbHealthFilter()
    : UbHealthFilter(std::make_shared<WorkerUbHealthRegistry>())
{
}

UbHealthFilter::UbHealthFilter(std::shared_ptr<WorkerUbHealthRegistry> ubHealthRegistry)
    : ubHealthRegistry_(std::move(ubHealthRegistry)),
      writeTargetAdmission_(std::make_shared<PeerUbAdmission>()),
      writeTargetCompletionGenerations_(std::make_shared<const WriteTargetCompletionGenerations>())
{
    if (ubHealthRegistry_ == nullptr) {
        throw std::invalid_argument("Worker UB health registry must not be null");
    }
}

bool UbHealthFilter::ObserveSummary(const UbHealthSummary &summary,
                                    const std::string &expectedIncarnation)
{
    if (!IsClientUbFaultIsolationEnabled()) {
        return false;
    }
    std::string expected = expectedIncarnation;
    {
        std::lock_guard<bthread::Mutex> lock(incarnationMutex_);
        if (topologyInitialized_) {
            auto trusted = trustedIncarnations_.find(summary.worker);
            if (trusted == trustedIncarnations_.end()) {
                return false;
            }
            expected = trusted->second;
        }
    }
    const bool updated = ubHealthRegistry_->ApplySummary(summary, expected);
    auto accepted = ubHealthRegistry_->GetSummary(summary.worker);
    if (!accepted.has_value() || accepted->incarnation != expected) {
        return false;
    }
    std::lock_guard<bthread::Mutex> lock(incarnationMutex_);
    auto trusted = trustedIncarnations_.find(summary.worker);
    if (topologyInitialized_
        && (trusted == trustedIncarnations_.end() || trusted->second != summary.incarnation)) {
        return false;
    }
    if (accepted->portHealth.has_value()) {
        localAdmission_.SetRemotePortHealthCapability(summary.worker, true, accepted->incarnation);
        writeTargetAdmission_->SetRemotePortHealthCapability(summary.worker, true, accepted->incarnation);
    }
    return updated;
}

bool UbHealthFilter::ApplySummary(const UbHealthSummary &summary, const std::string &expectedIncarnation)
{
    std::string expected = expectedIncarnation;
    {
        std::lock_guard<bthread::Mutex> lock(incarnationMutex_);
        if (topologyInitialized_) {
            auto trusted = trustedIncarnations_.find(summary.worker);
            if (trusted == trustedIncarnations_.end()) {
                return false;
            }
            expected = trusted->second;
        }
    }

    const bool wasGloballyUnavailable = ubHealthRegistry_->IsVerifiedUnavailable(summary.worker);
    const bool verifiedRecovery = summary.portHealth.has_value()
                                      ? ShouldRecoverFromUbIsolation(*summary.portHealth)
                                      : summary.writable;
    if (!ubHealthRegistry_->ApplyVerifiedSummary(summary, expected)) {
        return false;
    }
    const bool legacyReadRecovery = wasGloballyUnavailable
                                    && !ubHealthRegistry_->IsVerifiedUnavailable(summary.worker);
    std::lock_guard<bthread::Mutex> lock(incarnationMutex_);
    auto trusted = trustedIncarnations_.find(summary.worker);
    if (topologyInitialized_
        && (trusted == trustedIncarnations_.end() || trusted->second != summary.incarnation)) {
        return false;
    }
    if (summary.portHealth.has_value()) {
        localAdmission_.SetRemotePortHealthCapability(summary.worker, true, summary.incarnation);
        writeTargetAdmission_->SetRemotePortHealthCapability(summary.worker, true, summary.incarnation);
        // The verified port fact is authoritative for the write direction. Applying it on recovery too (instead of
        // clearing the write admission) keeps the health epoch watermark, so a stale all-BAD fact cannot re-quarantine
        // a recovered Worker, and quarantine and release follow one code path.
        ApplyVerifiedWriteTargetPortHealth(summary.worker, summary.incarnation, *summary.portHealth);
    }

    if (verifiedRecovery && (summary.portHealth.has_value() || legacyReadRecovery)) {
        localAdmission_.ClearLocalState(summary.worker);
        localObservationIncarnations_.erase(summary.worker);
    } else {
        ReconcileLocalObservationWithTrustedIncarnationLocked(summary.worker, summary.incarnation);
    }
    trustedIncarnations_[summary.worker] = summary.incarnation;
    if (ubHealthRegistry_->IsVerifiedUnavailable(summary.worker)) {
        INJECT_POINT_NO_RETURN("client.ub_health_filter.global_unavailable_applied");
    }
    return true;
}

void UbHealthFilter::SetRemotePortHealthVerificationTrigger(
    PeerUbAdmission::RemotePortHealthVerificationTrigger trigger)
{
    localAdmission_.SetRemotePortHealthVerificationTrigger(trigger);
    writeTargetAdmission_->SetRemotePortHealthVerificationTrigger(std::move(trigger));
}

void UbHealthFilter::ApplyTopologyIncarnations(const ::datasystem::ClusterTopologyPb &ring)
{
    std::unordered_map<HostPort, std::string> replacement;
    std::unordered_set<HostPort> workers;
    replacement.reserve(ring.members_size());
    workers.reserve(ring.members_size());
    for (const auto &[endpoint, member] : ring.members()) {
        HostPort worker;
        if (worker.ParseString(endpoint).IsError() || member.id().empty()) {
            continue;
        }
        workers.emplace(worker);
        replacement.emplace(std::move(worker), member.id());
    }

    std::lock_guard<bthread::Mutex> lock(incarnationMutex_);
    topologyInitialized_ = true;
    for (const auto &[worker, incarnation] : replacement) {
        ReconcileLocalObservationWithTrustedIncarnationLocked(worker, incarnation);
        ReconcileWriteTargetObservationLocked(worker, incarnation);
    }
    DropRemovedObservationsLocked(workers);
    const auto nowMs = GetSteadyClockTimeStampMs();
    localAdmission_.ReconcileRemotePortHealthCapabilities(replacement);
    writeTargetAdmission_->ReconcileRemotePortHealthCapabilities(replacement);
    localAdmission_.ReconcileTopologyWorkers(workers, nowMs, 0);
    writeTargetAdmission_->ReconcileTopologyWorkers(workers, nowMs, 0);
    trustedIncarnations_ = std::move(replacement);
    writeTargetObservationCount_.store(writeTargetObservationIncarnations_.size(), std::memory_order_release);
    PublishWriteTargetCompletionGenerationsLocked(workers);
}

void UbHealthFilter::ReconcileWriteTargetObservationLocked(const HostPort &worker,
                                                           const std::string &incarnation)
{
    const auto trusted = trustedIncarnations_.find(worker);
    auto observation = writeTargetObservationIncarnations_.find(worker);
    const bool incarnationChanged = trusted != trustedIncarnations_.end() && trusted->second != incarnation;
    const bool observationStale = observation != writeTargetObservationIncarnations_.end()
                                  && (observation->second.empty() || observation->second != incarnation);
    if (!incarnationChanged && !observationStale) {
        return;
    }
    localAdmission_.SetRemotePortHealthCapability(worker, false);
    writeTargetAdmission_->SetRemotePortHealthCapability(worker, false);
    writeTargetAdmission_->ClearLocalState(worker);
    if (observation != writeTargetObservationIncarnations_.end()) {
        writeTargetObservationIncarnations_.erase(observation);
    }
}

void UbHealthFilter::DropRemovedObservationsLocked(const std::unordered_set<HostPort> &workers)
{
    for (auto iter = writeTargetObservationIncarnations_.begin();
         iter != writeTargetObservationIncarnations_.end();) {
        if (workers.count(iter->first) == 0) {
            writeTargetAdmission_->ClearLocalState(iter->first);
            iter = writeTargetObservationIncarnations_.erase(iter);
        } else {
            ++iter;
        }
    }
    for (auto iter = localObservationIncarnations_.begin(); iter != localObservationIncarnations_.end();) {
        if (workers.count(iter->first) == 0) {
            localAdmission_.ClearLocalState(iter->first);
            iter = localObservationIncarnations_.erase(iter);
        } else {
            ++iter;
        }
    }
}

bool UbHealthFilter::ReportProviderFailure(const HostPort &provider, const ProviderUbFailureDetailPb &detail)
{
    auto outcome = DecodeProviderUbFailureDetail(detail, provider, UbOperationKind::CLIENT_GET_WRITEBACK,
                                                 "client_direct_get_provider_detail");
    if (!outcome.has_value()) {
        return false;
    }
    {
        std::lock_guard<bthread::Mutex> lock(incarnationMutex_);
        EnablePortHealthVerificationIfSupportedLocked(outcome->peer);
    }
    localAdmission_.ReportOutcome(*outcome);
    const auto state = localAdmission_.GetState(provider);
    const bool unavailable = state.has_value() && state->state == UbAdmissionState::UNAVAILABLE;
    if (unavailable) {
        std::lock_guard<bthread::Mutex> lock(incarnationMutex_);
        auto incarnation = trustedIncarnations_.find(provider);
        localObservationIncarnations_[provider] =
            incarnation == trustedIncarnations_.end() ? std::string{} : incarnation->second;
        INJECT_POINT_NO_RETURN("client.ub_health_filter.local_observation");
    }
    return unavailable;
}

bool UbHealthFilter::ReportWriteTargetFailure(const HostPort &worker, const Status &status,
                                              std::optional<int> providerStatus,
                                              std::optional<int> cqeStatus)
{
    UbOpOutcome outcome(worker, UbOperationKind::CLIENT_PUT, status);
    outcome.providerStatus = providerStatus;
    outcome.cqeStatus = cqeStatus;
    outcome.learnedFrom = "client_write_target";
    {
        std::lock_guard<bthread::Mutex> lock(incarnationMutex_);
        EnablePortHealthVerificationIfSupportedLocked(worker);
    }
    writeTargetAdmission_->ReportOutcome(outcome);
    const auto state = writeTargetAdmission_->GetState(worker);
    const bool unavailable = state.has_value() && state->state == UbAdmissionState::UNAVAILABLE;
    if (unavailable) {
        std::lock_guard<bthread::Mutex> lock(incarnationMutex_);
        auto incarnation = trustedIncarnations_.find(worker);
        writeTargetObservationIncarnations_[worker] =
            incarnation == trustedIncarnations_.end() ? std::string{} : incarnation->second;
        writeTargetObservationCount_.store(writeTargetObservationIncarnations_.size(), std::memory_order_release);
        INJECT_POINT_NO_RETURN("client.ub_health_filter.write_target_unavailable");
    }
    return unavailable;
}

uint64_t UbHealthFilter::CaptureWriteTargetCompletionGeneration(const HostPort &worker)
{
    auto generations = std::atomic_load(&writeTargetCompletionGenerations_);
    auto generation = generations->find(worker);
    if (generation != generations->end()) {
        return generation->second;
    }
    std::lock_guard<bthread::Mutex> lock(incarnationMutex_);
    generations = std::atomic_load(&writeTargetCompletionGenerations_);
    generation = generations->find(worker);
    if (generation != generations->end()) {
        return generation->second;
    }
    RefreshWriteTargetCompletionGenerationLocked(worker);
    generations = std::atomic_load(&writeTargetCompletionGenerations_);
    generation = generations->find(worker);
    return generation == generations->end() ? 0 : generation->second;
}

void UbHealthFilter::PublishWriteTargetCompletionGenerationsLocked(const std::unordered_set<HostPort> &workers)
{
    auto generations = std::make_shared<WriteTargetCompletionGenerations>();
    generations->reserve(workers.size());
    for (const auto &worker : workers) {
        auto context = writeTargetAdmission_->BuildLateCompletionContext(UbOperationKind::CLIENT_PUT, worker);
        if (context.has_value()) {
            generations->emplace(worker, context->peerToken);
        }
    }
    std::atomic_store(&writeTargetCompletionGenerations_,
                      std::shared_ptr<const WriteTargetCompletionGenerations>(std::move(generations)));
}

void UbHealthFilter::RefreshWriteTargetCompletionGenerationLocked(const HostPort &worker)
{
    auto current = std::atomic_load(&writeTargetCompletionGenerations_);
    auto generations = std::make_shared<WriteTargetCompletionGenerations>(*current);
    auto context = writeTargetAdmission_->BuildLateCompletionContext(UbOperationKind::CLIENT_PUT, worker);
    if (context.has_value()) {
        (*generations)[worker] = context->peerToken;
    } else {
        generations->erase(worker);
    }
    std::atomic_store(&writeTargetCompletionGenerations_,
                      std::shared_ptr<const WriteTargetCompletionGenerations>(std::move(generations)));
}

void UbHealthFilter::ApplyVerifiedWriteTargetPortHealth(const HostPort &worker, const std::string &incarnation,
                                                        const UbPortHealthSummary &portHealth)
{
    (void)writeTargetAdmission_->ApplyPortHealth(worker, portHealth, UbPortHealthEvidenceSource::QUERY_RESPONSE);
    // The exclusion entry follows the final admission verdict, not the ApplyPortHealth return value: a repeated
    // fact with an unchanged epoch reports "not applied" while the Worker is still quarantined.
    const bool unavailable = writeTargetAdmission_->CheckWriteTarget(worker, UbOperationKind::CLIENT_PUT).IsError();
    if (unavailable) {
        writeTargetObservationIncarnations_[worker] = incarnation;
    } else {
        writeTargetObservationIncarnations_.erase(worker);
    }
    writeTargetObservationCount_.store(writeTargetObservationIncarnations_.size(), std::memory_order_release);
    RefreshWriteTargetCompletionGenerationLocked(worker);
}

void UbHealthFilter::ReportLateWriteTargetFailure(const UrmaLateCompletion &completion, uint64_t peerToken) noexcept
{
    try {
        auto context = writeTargetAdmission_->BuildLateCompletionContext(UbOperationKind::CLIENT_PUT);
        if (!context.has_value()) {
            return;
        }
        HostPort worker;
        const bool validWorker = worker.ParseString(completion.remoteAddress).IsOk();
        if (validWorker) {
            std::lock_guard<bthread::Mutex> lock(incarnationMutex_);
            EnablePortHealthVerificationIfSupportedLocked(worker);
        }
        writeTargetAdmission_->OnLateUrmaCompletion(completion, context->ownerToken, peerToken);
        if (validWorker) {
            const auto state = writeTargetAdmission_->GetState(worker);
            if (state.has_value() && state->state == UbAdmissionState::UNAVAILABLE) {
                std::lock_guard<bthread::Mutex> lock(incarnationMutex_);
                auto incarnation = trustedIncarnations_.find(worker);
                writeTargetObservationIncarnations_[worker] =
                    incarnation == trustedIncarnations_.end() ? std::string{} : incarnation->second;
                writeTargetObservationCount_.store(writeTargetObservationIncarnations_.size(),
                                                   std::memory_order_release);
            }
        }
    } catch (const std::exception &error) {
        LOG(ERROR) << "Failed to process late Client write-target completion: " << error.what();
    } catch (...) {
        LOG(ERROR) << "Failed to process late Client write-target completion: unknown exception";
    }
}

void UbHealthFilter::ReconcileLocalObservationWithTrustedIncarnationLocked(const HostPort &worker,
                                                                           const std::string &incarnation)
{
    auto observation = localObservationIncarnations_.find(worker);
    if (observation == localObservationIncarnations_.end()
        || (!observation->second.empty() && observation->second == incarnation)) {
        return;
    }
    localAdmission_.ClearLocalState(worker);
    localObservationIncarnations_.erase(observation);
}

void UbHealthFilter::EnablePortHealthVerificationIfSupportedLocked(const HostPort &worker)
{
    auto summary = ubHealthRegistry_->GetSummary(worker);
    auto trusted = trustedIncarnations_.find(worker);
    if (summary.has_value() && summary->portHealth.has_value()
        && (trusted == trustedIncarnations_.end() || summary->incarnation == trusted->second)) {
        localAdmission_.SetRemotePortHealthCapability(worker, true, summary->incarnation);
        writeTargetAdmission_->SetRemotePortHealthCapability(worker, true, summary->incarnation);
    }
}

bool UbHealthFilter::IsAvailable(const HostPort &addr, WorkerAccessAction action) const
{
    if (!IsClientUbFaultIsolationEnabled()) {
        return true;
    }
    if (action == WorkerAccessAction::GET) {
        // A remote Worker's UB health must not deny reads: the Worker owns the UB writeback verdict and the read
        // path reacts to the response (TCP fallback or another replica). Only the client-local port admission
        // closes reads, and that gate runs before routing.
        return true;
    }
    if (action == WorkerAccessAction::SET) {
        return IsWriteTargetAvailable(addr);
    }
    if (localAdmission_.CheckReadSource(addr).IsError()) {
        INJECT_POINT_NO_RETURN("client.ub_health_filter.local_read_denied");
        return false;
    }
    if (ubHealthRegistry_->IsVerifiedUnavailable(addr)) {
        INJECT_POINT_NO_RETURN("client.ub_health_filter.global_read_denied");
        return false;
    }
    return true;
}

bool UbHealthFilter::IsWriteTargetAvailable(const HostPort &addr) const
{
    if (!IsClientUbFaultIsolationEnabled()) {
        return true;
    }
    return !ubHealthRegistry_->IsVerifiedUnavailable(addr)
           && writeTargetAdmission_->CheckWriteTarget(addr, UbOperationKind::CLIENT_PUT).IsOk();
}

bool UbHealthFilter::SupportsPortHealthVerification(const HostPort &addr) const
{
    std::lock_guard<bthread::Mutex> lock(incarnationMutex_);
    auto summary = ubHealthRegistry_->GetSummary(addr);
    if (!summary.has_value() || !summary->portHealth.has_value()) {
        return false;
    }
    auto trusted = trustedIncarnations_.find(addr);
    return !topologyInitialized_
           || (trusted != trustedIncarnations_.end() && trusted->second == summary->incarnation);
}

std::vector<HostPort> UbHealthFilter::GetUnavailableWriteTargets() const
{
    std::vector<HostPort> unavailable;
    if (!IsClientUbFaultIsolationEnabled()
        || writeTargetObservationCount_.load(std::memory_order_acquire) == 0) {
        return unavailable;
    }
    std::lock_guard<bthread::Mutex> lock(incarnationMutex_);
    unavailable.reserve(writeTargetObservationIncarnations_.size());
    for (const auto &[worker, incarnation] : writeTargetObservationIncarnations_) {
        (void)incarnation;
        if (writeTargetAdmission_->CheckWriteTarget(worker, UbOperationKind::CLIENT_PUT).IsError()) {
            unavailable.emplace_back(worker);
        }
    }
    return unavailable;
}

std::optional<UbPathState> UbHealthFilter::GetWriteTargetObservation(const HostPort &addr) const
{
    return writeTargetAdmission_->GetState(addr);
}

std::optional<UbPathState> UbHealthFilter::GetLocalObservation(const HostPort &addr) const
{
    return localAdmission_.GetState(addr);
}

bool UbHealthFilter::SeedProviderRecoveryFromGlobalSummary(const HostPort &addr)
{
    std::lock_guard<bthread::Mutex> lock(incarnationMutex_);
    auto summary = ubHealthRegistry_->GetSummary(addr);
    if (!summary.has_value() || summary->writable) {
        return false;
    }
    auto state = localAdmission_.GetState(addr);
    if (state.has_value() && state->state != UbAdmissionState::AVAILABLE) {
        return false;
    }
    localAdmission_.InitializeVerification(addr, GetSteadyClockTimeStampMs());
    localObservationIncarnations_[addr] = summary->incarnation;
    return true;
}

std::optional<ProviderUbRecoveryCandidate> UbHealthFilter::TryBeginProviderRecovery(uint64_t nowMs)
{
    std::lock_guard<bthread::Mutex> lock(incarnationMutex_);
    auto worker = localAdmission_.NextProbeCandidate(nowMs);
    if (!worker.has_value()) {
        return std::nullopt;
    }
    auto token = localAdmission_.TryBeginProbe(*worker, nowMs);
    if (!token.has_value()) {
        return std::nullopt;
    }
    auto trusted = trustedIncarnations_.find(*worker);
    return ProviderUbRecoveryCandidate{ *token,
                                        trusted == trustedIncarnations_.end() ? std::string{} : trusted->second };
}

bool UbHealthFilter::CompleteProviderRecovery(const ProviderUbRecoveryCandidate &candidate,
                                              const std::optional<UbHealthSummary> &summary,
                                              const Status &probeStatus, uint64_t nowMs)
{
    std::optional<std::string> registryExpectedIncarnation;
    Status completion = probeStatus;
    {
        std::lock_guard<bthread::Mutex> lock(incarnationMutex_);
        if (!summary.has_value() || summary->worker != candidate.token.peer || summary->incarnation.empty()
            || (!candidate.expectedIncarnation.empty() && summary->incarnation != candidate.expectedIncarnation)) {
            completion = Status(K_INVALID, "Provider UB recovery response identity does not match probe candidate");
        } else {
            auto trusted = trustedIncarnations_.find(candidate.token.peer);
            if (topologyInitialized_
                && (trusted == trustedIncarnations_.end() || trusted->second != summary->incarnation)) {
                completion = Status(K_INVALID,
                                    "Discard Provider UB recovery response from a retired Worker incarnation");
            } else if (!summary->writable) {
                completion = Status(K_URMA_WORKER_UNAVAILABLE,
                                    "Provider UB recovery response still reports the Worker unavailable");
            }
            if (!topologyInitialized_ || trusted != trustedIncarnations_.end()) {
                registryExpectedIncarnation = topologyInitialized_ ? trusted->second : summary->incarnation;
            }
        }
    }
    if (completion.IsOk() && summary.has_value() && registryExpectedIncarnation.has_value()
        && !ubHealthRegistry_->ApplyVerifiedSummary(*summary, *registryExpectedIncarnation)) {
        completion = Status(K_INVALID, "Provider UB recovery response was rejected by the health registry");
    }
    bool recovered = localAdmission_.CompleteProbe(candidate.token, completion, nowMs, false);
    if (recovered) {
        std::lock_guard<bthread::Mutex> lock(incarnationMutex_);
        recovered = localAdmission_.CheckReadSource(candidate.token.peer).IsOk();
        if (recovered) {
            localObservationIncarnations_.erase(candidate.token.peer);
        }
    }
    if (recovered) {
        INJECT_POINT_NO_RETURN("client.ub_health_filter.provider_probe_recovered");
    } else if (completion.IsError()) {
        LOG_FIRST_EVERY_N(WARNING, UB_RECOVERY_REJECT_LOG_EVERY_N)
            << "Provider UB recovery rejected for " << candidate.token.peer.ToString()
            << ": " << completion.ToString();
    }
    return recovered;
}

std::optional<uint64_t> UbHealthFilter::NextProviderRecoveryDeadlineMs() const
{
    std::lock_guard<bthread::Mutex> lock(incarnationMutex_);
    return localAdmission_.NextProbeDeadlineMs();
}

std::optional<WriteTargetUbRecoveryCandidate> UbHealthFilter::TryBeginWriteTargetRecovery(uint64_t nowMs)
{
    std::lock_guard<bthread::Mutex> lock(incarnationMutex_);
    auto worker = writeTargetAdmission_->NextProbeCandidate(nowMs);
    if (!worker.has_value()) {
        return std::nullopt;
    }
    auto token = writeTargetAdmission_->TryBeginProbe(*worker, nowMs);
    if (!token.has_value()) {
        return std::nullopt;
    }
    auto trusted = trustedIncarnations_.find(*worker);
    return WriteTargetUbRecoveryCandidate{ *token,
                                           trusted == trustedIncarnations_.end() ? std::string{} : trusted->second };
}

bool UbHealthFilter::CompleteWriteTargetRecovery(const WriteTargetUbRecoveryCandidate &candidate,
                                                 const Status &probeStatus, uint64_t nowMs)
{
    Status completion = probeStatus;
    {
        std::lock_guard<bthread::Mutex> lock(incarnationMutex_);
        auto trusted = trustedIncarnations_.find(candidate.token.peer);
        if (topologyInitialized_
            && (trusted == trustedIncarnations_.end()
                || (!candidate.expectedIncarnation.empty() && trusted->second != candidate.expectedIncarnation))) {
            completion = Status(K_INVALID,
                                "Discard write-target UB recovery for a retired Worker incarnation");
        }
    }
    bool recovered = writeTargetAdmission_->CompleteProbe(candidate.token, completion, nowMs, false);
    if (recovered) {
        std::lock_guard<bthread::Mutex> lock(incarnationMutex_);
        recovered = writeTargetAdmission_->CheckWriteTarget(candidate.token.peer, UbOperationKind::CLIENT_PUT).IsOk();
        if (recovered) {
            writeTargetObservationIncarnations_.erase(candidate.token.peer);
            writeTargetObservationCount_.store(writeTargetObservationIncarnations_.size(), std::memory_order_release);
            RefreshWriteTargetCompletionGenerationLocked(candidate.token.peer);
            INJECT_POINT_NO_RETURN("client.ub_health_filter.write_target_recovered");
        }
    }
    if (!recovered && completion.IsError()) {
        LOG_FIRST_EVERY_N(WARNING, UB_RECOVERY_REJECT_LOG_EVERY_N)
            << "Write-target UB recovery rejected for " << candidate.token.peer.ToString()
            << ": " << completion.ToString();
    }
    return recovered;
}

std::optional<uint64_t> UbHealthFilter::NextWriteTargetRecoveryDeadlineMs() const
{
    std::lock_guard<bthread::Mutex> lock(incarnationMutex_);
    return writeTargetAdmission_->NextProbeDeadlineMs();
}
}  // namespace datasystem::client
