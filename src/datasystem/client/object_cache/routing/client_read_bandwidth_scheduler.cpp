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

#include "datasystem/client/object_cache/routing/client_read_bandwidth_scheduler.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"

namespace datasystem {
namespace client {
namespace {

constexpr uint64_t NANOSECONDS_PER_MICROSECOND = 1000ULL;
constexpr uint64_t NANOSECONDS_PER_MILLISECOND = 1000000ULL;
constexpr uint64_t HASH_GOLDEN_RATIO = 0x9e3779b97f4a7c15ULL;
constexpr uint64_t CANDIDATE_STREAM = 0x243f6a8885a308d3ULL;
constexpr uint64_t ACCEPTANCE_STREAM = 0x13198a2e03707344ULL;
constexpr uint32_t ACCEPTANCE_RANDOM_BITS = 32U;
constexpr uint64_t HASH_WARNING_TOMBSTONE_SCALE = 4ULL;

uint64_t SteadyNowNs() noexcept
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

uint64_t Fnv1a64(const std::string &value) noexcept
{
    uint64_t hash = 1469598103934665603ULL;
    for (unsigned char character : value) {
        hash ^= static_cast<uint64_t>(character);
        hash *= 1099511628211ULL;
    }
    return hash;
}

uint64_t Mix64(uint64_t value) noexcept
{
    value ^= value >> 30U;
    value *= 0xbf58476d1ce4e5b9ULL;
    value ^= value >> 27U;
    value *= 0x94d049bb133111ebULL;
    value ^= value >> 31U;
    return value;
}

uint64_t SaturatingAdd(uint64_t lhs, uint64_t rhs) noexcept
{
    const uint64_t maximum = std::numeric_limits<uint64_t>::max();
    return lhs > maximum - rhs ? maximum : lhs + rhs;
}

uint64_t SaturatingMultiply(uint64_t lhs, uint64_t rhs) noexcept
{
    if (rhs != 0 && lhs > std::numeric_limits<uint64_t>::max() / rhs) {
        return std::numeric_limits<uint64_t>::max();
    }
    return lhs * rhs;
}

class AtomicByteReleaseGuard {
public:
    explicit AtomicByteReleaseGuard(std::atomic<uint8_t> &flag) noexcept : flag_(flag)
    {
    }

    ~AtomicByteReleaseGuard()
    {
        Release();
    }

    void Release() noexcept
    {
        if (active_) {
            flag_.store(0, std::memory_order_release);
            active_ = false;
        }
    }

private:
    std::atomic<uint8_t> &flag_;
    bool active_{ true };
};

}  // namespace

ClientReadBandwidthScheduler::Config ClientReadBandwidthScheduler::Config::FromFlags()
{
    Config config;
    config.enabled = FLAGS_enable_load_aware_scheduler;
    return config;
}

class ClientReadBandwidthScheduler::Impl {
public:
    explicit Impl(Config config);
    ~Impl() = default;

    bool Enabled() const noexcept;
    void Observe(const HostPort &worker, uint32_t p50Ns, uint32_t p99Ns, uint64_t latencyVersion,
                 const std::string &callFrom);
    void RefreshCandidates(const std::vector<HostPort> &workers);
    bool GetWorkerStatus(const HostPort &worker, const std::shared_ptr<const UbRoutingHealthSnapshot> &snapshot,
                         WorkerStatus &out) const;
    bool ShouldKeepAffinity(const HostPort &worker, const std::string &requestKey,
                            const std::shared_ptr<const UbRoutingHealthSnapshot> &snapshot, uint64_t &taskId);
    bool SelectWorkerFast(const std::string &requestKey, const std::vector<HostPort> &exclude,
                          const HostPort &preferredWorker, const std::vector<std::shared_ptr<IWorkerFilter>> &filters,
                          const std::shared_ptr<const UbRoutingHealthSnapshot> &snapshot, uint64_t taskId,
                          HostPort &selected, WorkerAccessAction action);
    void SetCutInGuardNs(uint64_t latencyHardLimitMs)
    {
        if (latencyHardLimitMs > 0) {
            uint64_t hardLimitNs = 0;
            if (latencyHardLimitMs > std::numeric_limits<uint64_t>::max() / NANOSECONDS_PER_MILLISECOND) {
                hardLimitNs = std::numeric_limits<uint64_t>::max();
            } else {
                hardLimitNs = latencyHardLimitMs * NANOSECONDS_PER_MILLISECOND;
            }
            cutInGuardNs_ = hardLimitNs / 1000ULL * config_.latencyCutInGuardPermille;
        }
    }

private:
    static constexpr uint64_t kEmptyKey = 0;
    static constexpr uint64_t kPublishingKey = 1;
    static constexpr uint64_t kTombstoneKey = 2;
    static constexpr uint64_t kFirstValidWorkerHash = 3;
    static constexpr uint32_t kNoActiveTable = std::numeric_limits<uint32_t>::max();
    static constexpr uint32_t kMaxStarvationProbes = 2;
    static constexpr uint32_t kMaxSlotLookupAttempts = 3;
    static constexpr uint32_t kMaxWeightedDrawAttempts = 100;
    static constexpr size_t kRejectedSlotCapacity = 16;
    static constexpr size_t kLongProbeThreshold = 64;
    static constexpr uint64_t kWarningIntervalNs = 5ULL * 1000ULL * NANOSECONDS_PER_MILLISECOND;

    struct WorkerSlot {
        HostPort worker{};
        std::atomic<uint64_t> keyHash{ 0 };
        mutable std::atomic<uint32_t> identityReaders{ 0 };
        std::atomic<uint64_t> generation{ 0 };
        std::atomic<uint64_t> packedLatency{ 0 };
        std::atomic<uint64_t> latencyVersion{ 0 };
        std::atomic<uint64_t> affinityCostNs{ 0 };
        std::atomic<uint64_t> nonAffinityCostNs{ 0 };
        std::atomic<uint8_t> updateBusy{ 0 };
        std::atomic<uint64_t> lastStateRefreshNs{ 0 };
        std::atomic<uint64_t> lastSelectedNs{ 0 };
        std::atomic<uint8_t> starvationClaimed{ 0 };
        std::atomic<uint64_t> lastStarvationForceNs{ 0 };
        std::atomic<uint64_t> candidateEpoch{ 0 };
        std::atomic<uint64_t> lastCandidateSeenNs{ 0 };
    };

    class SlotPin {
    public:
        SlotPin() noexcept = default;
        SlotPin(WorkerSlot *slot, uint64_t expectedKey, const HostPort &worker);
        SlotPin(SlotPin &&other) noexcept;
        SlotPin &operator=(SlotPin &&other) noexcept;
        ~SlotPin();

        SlotPin(const SlotPin &) = delete;
        SlotPin &operator=(const SlotPin &) = delete;

        explicit operator bool() const noexcept;
        WorkerSlot *Get() const noexcept;

    private:
        void Release() noexcept;
        WorkerSlot *slot_{ nullptr };
    };

    struct WorkerState {
        uint64_t p50Ns{ 0 };
        uint64_t p99Ns{ 0 };
        uint64_t latencyVersion{ 0 };
        uint64_t lastStateRefreshNs{ 0 };
        bool exists{ false };
        bool fresh{ false };
    };

    struct CandidateEntry {
        HostPort worker{};
        WorkerSlot *slot{ nullptr };
        uint64_t slotGeneration{ 0 };
    };

    struct CandidateTable {
        explicit CandidateTable(size_t capacity)
        {
            if (capacity == 0) {
                capacity = 1;
            }
            entries.reset(new CandidateEntry[capacity]);
        }

        // Sequential consistency closes the active-index pinning race.
        std::atomic<uint32_t> readers{ 0 };
        size_t count{ 0 };
        std::unique_ptr<CandidateEntry[]> entries;
    };

    class TablePin {
    public:
        TablePin() noexcept = default;
        explicit TablePin(CandidateTable *table) noexcept;
        TablePin(TablePin &&other) noexcept;
        TablePin &operator=(TablePin &&other) noexcept;
        ~TablePin();

        TablePin(const TablePin &) = delete;
        TablePin &operator=(const TablePin &) = delete;

        explicit operator bool() const noexcept;
        CandidateTable *Get() const noexcept;

    private:
        void Release() noexcept;
        CandidateTable *table_{ nullptr };
    };

    struct SlotScanResult {
        SlotPin found{};
        WorkerSlot *firstTombstone{ nullptr };
        WorkerSlot *firstEmpty{ nullptr };
        size_t probes{ 0 };
        bool sawPublishing{ false };
    };

    struct SlotLookupResult {
        SlotPin pin{};
        bool retry{ false };
    };

    struct PortCounts {
        uint32_t failed{ 0 };
        uint32_t total{ 0 };
        bool known{ false };

        bool AllFailed() const noexcept
        {
            return known && failed >= total;
        }
    };

    enum class SelectionMode : uint8_t { kWeighted = 0, kStarvation };

    enum class CandidateCheckResult : uint8_t {
        kUsable = 0,
        kInvalidSlot,
        kInvalidGeneration,
        kRejected,
        kExcluded,
        kNotStarved
    };

    struct WeightedProbe {
        CandidateEntry *entry{ nullptr };
        uint64_t latencyCostNs{ 0 };
        PortCounts ports{};
    };

    static uint64_t NormalizeWorkerHash(const HostPort &worker) noexcept;
    static bool IsExcluded(const HostPort &worker, const std::vector<HostPort> &exclude);
    static void AtomicMax(std::atomic<uint64_t> &value, uint64_t newValue) noexcept;
    static void MarkSelected(WorkerSlot &slot, uint64_t nowNs) noexcept;
    static bool IsRejected(const WorkerSlot *slot,
                           const std::array<const WorkerSlot *, kRejectedSlotCapacity> &rejected,
                           size_t rejectedCount) noexcept;
    static void AddRejectedSlot(const WorkerSlot *slot, std::array<const WorkerSlot *, kRejectedSlotCapacity> &rejected,
                                size_t &rejectedCount) noexcept;

    SlotScanResult ScanSlots(const HostPort &worker, uint64_t hash) const;
    SlotLookupResult FindOrCreateSlotOnce(const HostPort &worker, uint64_t nowNs);
    SlotPin FindOrCreateSlot(const HostPort &worker, uint64_t nowNs);
    SlotPin FindSlot(const HostPort &worker) const;
    void InitializeSlot(WorkerSlot &slot, const HostPort &worker, uint64_t nowNs);
    bool CanRecycleSlot(const WorkerSlot &slot, uint64_t key, uint64_t nowNs, uint64_t currentEpoch,
                        uint64_t inactiveNs) const noexcept;
    bool TryRecycleSlot(WorkerSlot &slot, uint64_t nowNs, uint64_t currentEpoch, uint64_t inactiveNs);
    void ResetRecycledSlot(WorkerSlot &slot) noexcept;
    void MaybeRecycleInactiveSlots(uint64_t nowNs, uint64_t currentEpoch);
    void ConsumeTombstone() noexcept;
    void RecordProbeLength(size_t probes) const noexcept;

    bool CandidateRefreshDue(uint64_t nowNs) const noexcept;
    void BuildCandidateTable(CandidateTable &table, const std::vector<HostPort> &workers, uint64_t nowNs,
                             uint64_t epoch);
    TablePin AcquireCandidateTable() noexcept;

    WorkerState ReadWorkerState(const WorkerSlot &slot, uint64_t nowNs) const noexcept;
    bool IsCurrentCandidate(const WorkerSlot &slot) const noexcept;
    bool NeedsStarvationForce(const WorkerSlot &slot, uint64_t nowNs) const noexcept;
    PortCounts ReadPortCounts(const UbRoutingHealthSnapshot *snapshot, const HostPort &worker) const;

    uint64_t CalculateLatencyCost(uint64_t p50Ns, uint64_t p99Ns, bool affinity) const noexcept;
    uint64_t CalculateLiveWeight(uint64_t latencyCostNs, const PortCounts &ports) const noexcept;
    void RefreshPrecomputedCosts(WorkerSlot &slot, uint32_t p50Ns, uint32_t p99Ns);
    uint64_t LoadPrecomputedCost(const WorkerSlot &slot, bool affinity) const noexcept;
    bool AcceptWeightedProbe(const WeightedProbe &probe, uint64_t randomValue) const noexcept;
    static size_t MapRandomToIndex(uint64_t randomValue, size_t count) noexcept;

    CandidateCheckResult CheckCandidate(const CandidateEntry &entry, const std::vector<HostPort> &exclude,
                                        const std::array<const WorkerSlot *, kRejectedSlotCapacity> &rejected,
                                        size_t rejectedCount, uint64_t nowNs, SelectionMode mode) const;
    WeightedProbe PrepareWeightedProbe(CandidateEntry &entry, const std::vector<HostPort> &exclude,
                                       const HostPort &preferredWorker, const UbRoutingHealthSnapshot *snapshot,
                                       const std::array<const WorkerSlot *, kRejectedSlotCapacity> &rejected,
                                       size_t rejectedCount, uint64_t nowNs, SelectionMode mode) const;
    CandidateEntry *TryUseWeightedProbe(const WeightedProbe &probe, const std::vector<HostPort> &exclude,
                                        const std::vector<std::shared_ptr<IWorkerFilter>> &filters,
                                        WorkerAccessAction action,
                                        std::array<const WorkerSlot *, kRejectedSlotCapacity> &rejected,
                                        size_t &rejectedCount, uint64_t nowNs, size_t rejectLimit, bool &stop) const;
    CandidateEntry *TryPickStarvedCandidate(CandidateTable &table, const std::vector<HostPort> &exclude,
                                            const HostPort &preferredWorker,
                                            const std::vector<std::shared_ptr<IWorkerFilter>> &filters,
                                            WorkerAccessAction action,
                                            const UbRoutingHealthSnapshot *snapshot,
                                            std::array<const WorkerSlot *, kRejectedSlotCapacity> &rejected,
                                            size_t &rejectedCount, uint64_t nowNs, size_t rejectLimit);
    CandidateEntry *PickWeightedCandidate(const std::string &requestKey, CandidateTable &table,
                                          const std::vector<HostPort> &exclude, const HostPort &preferredWorker,
                                          const std::vector<std::shared_ptr<IWorkerFilter>> &filters,
                                          WorkerAccessAction action,
                                          const UbRoutingHealthSnapshot *snapshot,
                                          std::array<const WorkerSlot *, kRejectedSlotCapacity> &rejected,
                                          size_t &rejectedCount, uint64_t nowNs, size_t rejectLimit);
    CandidateEntry *PickOneCandidate(const std::string &requestKey, CandidateTable &table,
                                     const std::vector<HostPort> &exclude, const HostPort &preferredWorker,
                                     const std::vector<std::shared_ptr<IWorkerFilter>> &filters,
                                     WorkerAccessAction action,
                                     const UbRoutingHealthSnapshot *snapshot,
                                     std::array<const WorkerSlot *, kRejectedSlotCapacity> &rejected,
                                     size_t &rejectedCount, uint64_t nowNs, size_t rejectLimit, SelectionMode &mode);

    bool IsWorkerAvailable(const HostPort &worker, const std::vector<std::shared_ptr<IWorkerFilter>> &filters,
                           WorkerAccessAction action) const;
    uint64_t GenerateSelectionSeed(const std::string &requestKey) noexcept;
    static uint64_t GenerateAttemptValue(uint64_t seed, uint32_t attempt, uint64_t streamSalt) noexcept;

    void MaybeWarnHashTable(uint64_t nowNs) const;
    void LogObserve(const HostPort &worker, const std::string &callFrom, uint64_t nowNs, uint32_t p50Ns, uint32_t p99Ns,
                    uint64_t latencyVersion, bool refreshed, uint64_t affinityCost, uint64_t nonAffinityCost) const;
    void LogDecisionEntry(const HostPort &worker, const std::string &requestKey, const PortCounts &ports,
                          uint64_t taskId, uint64_t nowNs, bool keepAffinity) const;
    void LogDecisionFinal(const HostPort &preferredWorker, const HostPort *selected, uint64_t taskId, uint64_t nowNs,
                          const char *reason, SelectionMode mode) const;

    Config config_;
    uint64_t initialP50Ns_;
    uint64_t initialP99Ns_;
    uint64_t nonAffinityPenaltyNs_;
    uint64_t referenceLatencyNs_;
    uint64_t maxSelectionWeight_;
    uint64_t staleNs_;
    uint64_t starvationNs_;
    uint64_t cutInGuardNs_{ 12000000 };  // Default worker-side load bottleneck
    uint64_t clientSaltHash_;
    size_t workerSlotCapacity_;
    std::string clientIp_;
    std::unique_ptr<WorkerSlot[]> workerSlots_;
    std::unique_ptr<CandidateTable> candidateTables_[2];  // Fixed two candidate worker lists.
    std::atomic<uint32_t> activeCandidateTableIndex_{ kNoActiveTable };
    std::atomic<uint64_t> lastCandidateRefreshNs_{ 0 };
    std::atomic<uint8_t> candidateRefreshBusy_{ 0 };
    std::atomic<uint64_t> candidateEpochCounter_{ 0 };
    std::atomic<uint64_t> publishedCandidateEpoch_{ 0 };
    std::atomic<uint64_t> lastWorkerRecycleCheckNs_{ 0 };
    std::atomic<uint64_t> tombstoneCount_{ 0 };
    mutable std::atomic<uint64_t> longProbeCount_{ 0 };
    mutable std::atomic<uint64_t> lastHashWarningNs_{ 0 };
    std::atomic<uint64_t> selectionSequence_{ 0 };
    std::atomic<uint64_t> starvationProbeCursor_{ 0 };
    mutable std::atomic<uint64_t> taskIdCounter_{ 0 };
};

ClientReadBandwidthScheduler::Impl::Impl(Config config)
    : config_(std::move(config)),
      initialP50Ns_(SaturatingMultiply(config_.initialP50Us, NANOSECONDS_PER_MICROSECOND)),
      initialP99Ns_(SaturatingMultiply(config_.initialP99Us, NANOSECONDS_PER_MICROSECOND)),
      nonAffinityPenaltyNs_(SaturatingMultiply(config_.latencyNonAffinityPenaltyUs, NANOSECONDS_PER_MICROSECOND)),
      referenceLatencyNs_(
          SaturatingMultiply(std::max<uint64_t>(1, config_.latencyWeightReferenceUs), NANOSECONDS_PER_MICROSECOND)),
      maxSelectionWeight_(std::max<uint32_t>(1, config_.latencyWeightScale)),
      staleNs_(SaturatingMultiply(config_.latencyClientStaleMs, NANOSECONDS_PER_MILLISECOND)),
      starvationNs_(SaturatingMultiply(config_.latencyStarvationProtectMs, NANOSECONDS_PER_MILLISECOND)),
      clientSaltHash_(Fnv1a64(config_.clientSalt)),
      workerSlotCapacity_(std::max<size_t>(1, config_.latencyClientTableSize)),
      clientIp_("client ip"),
      workerSlots_(new WorkerSlot[workerSlotCapacity_])
{
    candidateTables_[0].reset(new CandidateTable(workerSlotCapacity_));
    candidateTables_[1].reset(new CandidateTable(workerSlotCapacity_));
}

bool ClientReadBandwidthScheduler::Impl::Enabled() const noexcept
{
    return config_.enabled;
}

ClientReadBandwidthScheduler::Impl::SlotPin::SlotPin(WorkerSlot *slot, uint64_t expectedKey, const HostPort &worker)
    : slot_(slot)
{
    if (slot_ == nullptr) {
        return;
    }
    slot_->identityReaders.fetch_add(1, std::memory_order_seq_cst);
    if (slot_->keyHash.load(std::memory_order_seq_cst) != expectedKey || !(slot_->worker == worker)) {
        Release();
    }
}

ClientReadBandwidthScheduler::Impl::SlotPin::SlotPin(SlotPin &&other) noexcept : slot_(other.slot_)
{
    other.slot_ = nullptr;
}

ClientReadBandwidthScheduler::Impl::SlotPin &ClientReadBandwidthScheduler::Impl::SlotPin::operator=(
    SlotPin &&other) noexcept
{
    if (this != &other) {
        Release();
        slot_ = other.slot_;
        other.slot_ = nullptr;
    }
    return *this;
}

ClientReadBandwidthScheduler::Impl::SlotPin::~SlotPin()
{
    Release();
}

ClientReadBandwidthScheduler::Impl::SlotPin::operator bool() const noexcept
{
    return slot_ != nullptr;
}

ClientReadBandwidthScheduler::Impl::WorkerSlot *ClientReadBandwidthScheduler::Impl::SlotPin::Get() const noexcept
{
    return slot_;
}

void ClientReadBandwidthScheduler::Impl::SlotPin::Release() noexcept
{
    if (slot_ != nullptr) {
        slot_->identityReaders.fetch_sub(1, std::memory_order_seq_cst);
        slot_ = nullptr;
    }
}

ClientReadBandwidthScheduler::Impl::TablePin::TablePin(CandidateTable *table) noexcept : table_(table)
{
}

ClientReadBandwidthScheduler::Impl::TablePin::TablePin(TablePin &&other) noexcept : table_(other.table_)
{
    other.table_ = nullptr;
}

ClientReadBandwidthScheduler::Impl::TablePin &ClientReadBandwidthScheduler::Impl::TablePin::operator=(
    TablePin &&other) noexcept
{
    if (this != &other) {
        Release();
        table_ = other.table_;
        other.table_ = nullptr;
    }
    return *this;
}

ClientReadBandwidthScheduler::Impl::TablePin::~TablePin()
{
    Release();
}

ClientReadBandwidthScheduler::Impl::TablePin::operator bool() const noexcept
{
    return table_ != nullptr;
}

ClientReadBandwidthScheduler::Impl::CandidateTable *ClientReadBandwidthScheduler::Impl::TablePin::Get() const noexcept
{
    return table_;
}

void ClientReadBandwidthScheduler::Impl::TablePin::Release() noexcept
{
    if (table_ != nullptr) {
        table_->readers.fetch_sub(1, std::memory_order_seq_cst);
        table_ = nullptr;
    }
}

uint64_t ClientReadBandwidthScheduler::Impl::NormalizeWorkerHash(const HostPort &worker) noexcept
{
    uint64_t hash = Fnv1a64(worker.Host());
    const uint64_t port = static_cast<uint64_t>(static_cast<uint32_t>(worker.Port()));
    hash ^= Mix64(port + HASH_GOLDEN_RATIO);
    hash = Mix64(hash);
    return hash < kFirstValidWorkerHash ? hash + kFirstValidWorkerHash : hash;
}

bool ClientReadBandwidthScheduler::Impl::IsExcluded(const HostPort &worker, const std::vector<HostPort> &exclude)
{
    return std::any_of(exclude.begin(), exclude.end(), [&worker](const HostPort &item) { return item == worker; });
}

void ClientReadBandwidthScheduler::Impl::AtomicMax(std::atomic<uint64_t> &value, uint64_t newValue) noexcept
{
    uint64_t oldValue = value.load(std::memory_order_relaxed);
    bool keepTrying = oldValue < newValue;
    while (keepTrying) {
        const bool stored =
            value.compare_exchange_weak(oldValue, newValue, std::memory_order_relaxed, std::memory_order_relaxed);
        keepTrying = !stored && oldValue < newValue;
    }
}

void ClientReadBandwidthScheduler::Impl::MarkSelected(WorkerSlot &slot, uint64_t nowNs) noexcept
{
    AtomicMax(slot.lastSelectedNs, nowNs);
}

bool ClientReadBandwidthScheduler::Impl::IsRejected(
    const WorkerSlot *slot, const std::array<const WorkerSlot *, kRejectedSlotCapacity> &rejected,
    size_t rejectedCount) noexcept
{
    for (size_t index = 0; index < rejectedCount; ++index) {
        if (rejected[index] == slot) {
            return true;
        }
    }
    return false;
}

void ClientReadBandwidthScheduler::Impl::AddRejectedSlot(
    const WorkerSlot *slot, std::array<const WorkerSlot *, kRejectedSlotCapacity> &rejected,
    size_t &rejectedCount) noexcept
{
    if (slot == nullptr || rejectedCount >= rejected.size() || IsRejected(slot, rejected, rejectedCount)) {
        return;
    }
    rejected[rejectedCount++] = slot;
}

ClientReadBandwidthScheduler::Impl::SlotScanResult ClientReadBandwidthScheduler::Impl::ScanSlots(const HostPort &worker,
                                                                                                 uint64_t hash) const
{
    SlotScanResult result;
    const size_t start = static_cast<size_t>(hash % workerSlotCapacity_);
    for (; result.probes < workerSlotCapacity_; ++result.probes) {
        WorkerSlot &slot = workerSlots_[(start + result.probes) % workerSlotCapacity_];
        const uint64_t key = slot.keyHash.load(std::memory_order_acquire);
        if (key == hash) {
            SlotPin pin(&slot, key, worker);
            if (pin) {
                result.found = std::move(pin);
                ++result.probes;
                break;
            }
            if (slot.keyHash.load(std::memory_order_seq_cst) == kPublishingKey) {
                result.sawPublishing = true;
            }
        } else if (key == kPublishingKey) {
            result.sawPublishing = true;
        } else if (key == kTombstoneKey) {
            if (result.firstTombstone == nullptr) {
                result.firstTombstone = &slot;
            }
        } else if (key == kEmptyKey) {
            result.firstEmpty = &slot;
            ++result.probes;
            break;
        }
    }
    RecordProbeLength(result.probes);
    return result;
}

ClientReadBandwidthScheduler::Impl::SlotLookupResult ClientReadBandwidthScheduler::Impl::FindOrCreateSlotOnce(
    const HostPort &worker, uint64_t nowNs)
{
    const uint64_t hash = NormalizeWorkerHash(worker);
    SlotScanResult scan = ScanSlots(worker, hash);
    if (scan.found) {
        return { std::move(scan.found), false };
    }
    if (scan.sawPublishing) {
        return { {}, true };
    }
    WorkerSlot *target = scan.firstTombstone != nullptr ? scan.firstTombstone : scan.firstEmpty;
    if (target == nullptr) {
        return {};
    }
    const bool reusedTombstone = target == scan.firstTombstone;
    uint64_t expected = reusedTombstone ? kTombstoneKey : kEmptyKey;
    if (!target->keyHash.compare_exchange_strong(expected, kPublishingKey, std::memory_order_seq_cst,
                                                 std::memory_order_seq_cst)) {
        return { {}, true };
    }
    if (target->identityReaders.load(std::memory_order_seq_cst) != 0) {
        target->keyHash.store(expected, std::memory_order_seq_cst);
        return { {}, true };
    }
    try {
        InitializeSlot(*target, worker, nowNs);
    } catch (...) {
        target->keyHash.store(expected, std::memory_order_seq_cst);
        throw;
    }
    target->keyHash.store(hash, std::memory_order_seq_cst);
    if (reusedTombstone) {
        ConsumeTombstone();
    }
    return { SlotPin(target, hash, worker), false };
}

ClientReadBandwidthScheduler::Impl::SlotPin ClientReadBandwidthScheduler::Impl::FindOrCreateSlot(const HostPort &worker,
                                                                                                 uint64_t nowNs)
{
    if (worker.Empty()) {
        return {};
    }
    for (uint32_t attempt = 0; attempt < kMaxSlotLookupAttempts; ++attempt) {
        SlotLookupResult result = FindOrCreateSlotOnce(worker, nowNs);
        if (result.pin || !result.retry) {
            return std::move(result.pin);
        }
        std::this_thread::yield();
    }
    return {};
}

ClientReadBandwidthScheduler::Impl::SlotPin ClientReadBandwidthScheduler::Impl::FindSlot(const HostPort &worker) const
{
    if (worker.Empty()) {
        return {};
    }
    const uint64_t hash = NormalizeWorkerHash(worker);
    for (uint32_t attempt = 0; attempt < kMaxSlotLookupAttempts; ++attempt) {
        SlotScanResult scan = ScanSlots(worker, hash);
        if (scan.found) {
            return std::move(scan.found);
        }
        if (!scan.sawPublishing) {
            return {};
        }
        std::this_thread::yield();
    }
    return {};
}

void ClientReadBandwidthScheduler::Impl::InitializeSlot(WorkerSlot &slot, const HostPort &worker, uint64_t nowNs)
{
    slot.worker = worker;
    slot.generation.fetch_add(1, std::memory_order_relaxed);
    const uint32_t p50 = static_cast<uint32_t>(std::min<uint64_t>(initialP50Ns_, std::numeric_limits<uint32_t>::max()));
    const uint32_t p99 = static_cast<uint32_t>(std::min<uint64_t>(initialP99Ns_, std::numeric_limits<uint32_t>::max()));
    const uint64_t packed = (static_cast<uint64_t>(p50) << 32U) | p99;
    slot.packedLatency.store(packed, std::memory_order_relaxed);
    RefreshPrecomputedCosts(slot, p50, p99);
    slot.latencyVersion.store(0, std::memory_order_release);
    slot.updateBusy.store(0, std::memory_order_relaxed);
    slot.lastStateRefreshNs.store(nowNs, std::memory_order_release);
    slot.lastSelectedNs.store(nowNs, std::memory_order_relaxed);
    slot.starvationClaimed.store(0, std::memory_order_relaxed);
    slot.lastStarvationForceNs.store(0, std::memory_order_relaxed);
    slot.candidateEpoch.store(0, std::memory_order_relaxed);
    slot.lastCandidateSeenNs.store(nowNs, std::memory_order_relaxed);
}

bool ClientReadBandwidthScheduler::Impl::CanRecycleSlot(const WorkerSlot &slot, uint64_t key, uint64_t nowNs,
                                                        uint64_t currentEpoch, uint64_t inactiveNs) const noexcept
{
    if (key < kFirstValidWorkerHash || slot.candidateEpoch.load(std::memory_order_relaxed) == currentEpoch) {
        return false;
    }
    const uint64_t lastSeenNs = slot.lastCandidateSeenNs.load(std::memory_order_relaxed);
    return lastSeenNs != 0 && nowNs >= lastSeenNs && nowNs - lastSeenNs >= inactiveNs;
}

bool ClientReadBandwidthScheduler::Impl::TryRecycleSlot(WorkerSlot &slot, uint64_t nowNs, uint64_t currentEpoch,
                                                        uint64_t inactiveNs)
{
    const uint64_t key = slot.keyHash.load(std::memory_order_acquire);
    if (!CanRecycleSlot(slot, key, nowNs, currentEpoch, inactiveNs)) {
        return false;
    }
    uint8_t expectedBusy = 0;
    if (!slot.updateBusy.compare_exchange_strong(expectedBusy, 1, std::memory_order_acq_rel,
                                                 std::memory_order_relaxed)) {
        return false;
    }
    AtomicByteReleaseGuard updateGuard(slot.updateBusy);
    uint64_t expectedKey = key;
    if (!slot.keyHash.compare_exchange_strong(expectedKey, kPublishingKey, std::memory_order_seq_cst,
                                              std::memory_order_seq_cst)) {
        return false;
    }
    if (slot.identityReaders.load(std::memory_order_seq_cst) != 0
        || !CanRecycleSlot(slot, key, nowNs, currentEpoch, inactiveNs)) {
        slot.keyHash.store(key, std::memory_order_seq_cst);
        return false;
    }
    ResetRecycledSlot(slot);
    updateGuard.Release();
    slot.keyHash.store(kTombstoneKey, std::memory_order_seq_cst);
    tombstoneCount_.fetch_add(1, std::memory_order_relaxed);
    return true;
}

void ClientReadBandwidthScheduler::Impl::ResetRecycledSlot(WorkerSlot &slot) noexcept
{
    // Keep the old HostPort allocation. The publishing key prevents readers,
    // and the next owner overwrites it before publishing a valid key.
    slot.packedLatency.store(0, std::memory_order_relaxed);
    slot.latencyVersion.store(0, std::memory_order_relaxed);
    slot.affinityCostNs.store(0, std::memory_order_relaxed);
    slot.nonAffinityCostNs.store(0, std::memory_order_relaxed);
    slot.lastStateRefreshNs.store(0, std::memory_order_relaxed);
    slot.lastSelectedNs.store(0, std::memory_order_relaxed);
    slot.starvationClaimed.store(0, std::memory_order_relaxed);
    slot.lastStarvationForceNs.store(0, std::memory_order_relaxed);
    slot.candidateEpoch.store(0, std::memory_order_relaxed);
    slot.lastCandidateSeenNs.store(0, std::memory_order_relaxed);
}

void ClientReadBandwidthScheduler::Impl::MaybeRecycleInactiveSlots(uint64_t nowNs, uint64_t currentEpoch)
{
    const uint64_t intervalNs =
        SaturatingMultiply(std::max<uint64_t>(1, config_.latencyWorkerRecycleCheckMs), NANOSECONDS_PER_MILLISECOND);
    uint64_t lastNs = lastWorkerRecycleCheckNs_.load(std::memory_order_relaxed);
    if (lastNs != 0 && nowNs - lastNs < intervalNs) {
        return;
    }
    if (!lastWorkerRecycleCheckNs_.compare_exchange_strong(lastNs, nowNs, std::memory_order_acq_rel,
                                                           std::memory_order_relaxed)) {
        return;
    }
    const uint64_t inactiveNs =
        SaturatingMultiply(std::max<uint64_t>(1, config_.latencyWorkerInactiveRecycleMs), NANOSECONDS_PER_MILLISECOND);
    for (size_t index = 0; index < workerSlotCapacity_; ++index) {
        TryRecycleSlot(workerSlots_[index], nowNs, currentEpoch, inactiveNs);
    }
    MaybeWarnHashTable(nowNs);
}

void ClientReadBandwidthScheduler::Impl::ConsumeTombstone() noexcept
{
    uint64_t count = tombstoneCount_.load(std::memory_order_relaxed);
    bool keepTrying = count != 0;
    while (keepTrying) {
        const bool consumed =
            tombstoneCount_.compare_exchange_weak(count, count - 1, std::memory_order_relaxed,
                                                  std::memory_order_relaxed);
        keepTrying = !consumed && count != 0;
    }
}

void ClientReadBandwidthScheduler::Impl::RecordProbeLength(size_t probes) const noexcept
{
    if (probes > kLongProbeThreshold) {
        longProbeCount_.fetch_add(1, std::memory_order_relaxed);
    }
}

bool ClientReadBandwidthScheduler::Impl::CandidateRefreshDue(uint64_t nowNs) const noexcept
{
    const uint64_t intervalNs =
        SaturatingMultiply(std::max<uint64_t>(1, config_.latencyCandidateRefreshMs), NANOSECONDS_PER_MILLISECOND);
    const uint64_t lastNs = lastCandidateRefreshNs_.load(std::memory_order_acquire);
    return lastNs == 0 || nowNs - lastNs >= intervalNs;
}

void ClientReadBandwidthScheduler::Impl::BuildCandidateTable(CandidateTable &table,
                                                             const std::vector<HostPort> &workers, uint64_t nowNs,
                                                             uint64_t epoch)
{
    table.count = 0;
    for (const HostPort &worker : workers) {
        if (worker.Empty() || table.count >= workerSlotCapacity_) {
            continue;
        }
        SlotPin pin = FindOrCreateSlot(worker, nowNs);
        WorkerSlot *slot = pin.Get();
        if (slot == nullptr || slot->candidateEpoch.load(std::memory_order_relaxed) == epoch) {
            continue;
        }
        CandidateEntry &entry = table.entries[table.count];
        entry.worker = worker;
        entry.slot = slot;
        entry.slotGeneration = slot->generation.load(std::memory_order_acquire);
        slot->lastCandidateSeenNs.store(nowNs, std::memory_order_relaxed);
        slot->candidateEpoch.store(epoch, std::memory_order_relaxed);
        ++table.count;
    }
}

void ClientReadBandwidthScheduler::Impl::RefreshCandidates(const std::vector<HostPort> &workers)
{
    if (!Enabled()) {
        return;
    }
    const uint64_t nowNs = SteadyNowNs();
    if (!CandidateRefreshDue(nowNs)) {
        return;
    }
    uint8_t expected = 0;
    if (!candidateRefreshBusy_.compare_exchange_strong(expected, 1, std::memory_order_acq_rel,
                                                       std::memory_order_relaxed)) {
        return;
    }
    AtomicByteReleaseGuard refreshGuard(candidateRefreshBusy_);
    if (!CandidateRefreshDue(nowNs)) {
        return;
    }
    const uint32_t activeIndex = activeCandidateTableIndex_.load(std::memory_order_seq_cst);
    const uint32_t buildIndex = activeIndex == 0 ? 1 : 0;
    CandidateTable &table = *candidateTables_[buildIndex];
    if (table.readers.load(std::memory_order_seq_cst) != 0) {
        return;
    }
    const uint64_t epoch = candidateEpochCounter_.fetch_add(1, std::memory_order_relaxed) + 1;
    BuildCandidateTable(table, workers, nowNs, epoch);
    activeCandidateTableIndex_.store(buildIndex, std::memory_order_seq_cst);
    publishedCandidateEpoch_.store(epoch, std::memory_order_release);
    lastCandidateRefreshNs_.store(nowNs, std::memory_order_release);
    if (activeIndex > 1 || candidateTables_[activeIndex]->readers.load(std::memory_order_seq_cst) == 0) {
        MaybeRecycleInactiveSlots(nowNs, epoch);
    }
}

ClientReadBandwidthScheduler::Impl::TablePin ClientReadBandwidthScheduler::Impl::AcquireCandidateTable() noexcept
{
    const uint32_t index = activeCandidateTableIndex_.load(std::memory_order_seq_cst);
    if (index > 1) {
        return {};
    }
    CandidateTable *table = candidateTables_[index].get();
    table->readers.fetch_add(1, std::memory_order_seq_cst);
    if (activeCandidateTableIndex_.load(std::memory_order_seq_cst) != index) {
        table->readers.fetch_sub(1, std::memory_order_seq_cst);
        return {};
    }
    return TablePin(table);
}

ClientReadBandwidthScheduler::Impl::WorkerState ClientReadBandwidthScheduler::Impl::ReadWorkerState(
    const WorkerSlot &slot, uint64_t nowNs) const noexcept
{
    WorkerState state;
    state.lastStateRefreshNs = slot.lastStateRefreshNs.load(std::memory_order_acquire);
    if (state.lastStateRefreshNs == 0) {
        return state;
    }
    state.latencyVersion = slot.latencyVersion.load(std::memory_order_acquire);
    const uint64_t packed = slot.packedLatency.load(std::memory_order_relaxed);
    state.p50Ns = static_cast<uint32_t>(packed >> 32U);
    state.p99Ns = static_cast<uint32_t>(packed & 0xFFFFFFFFULL);
    state.exists = state.p50Ns > 0 && state.p99Ns > 0;
    state.fresh = state.exists && (nowNs < state.lastStateRefreshNs || nowNs - state.lastStateRefreshNs <= staleNs_);
    return state;
}

bool ClientReadBandwidthScheduler::Impl::IsCurrentCandidate(const WorkerSlot &slot) const noexcept
{
    const uint64_t epoch = publishedCandidateEpoch_.load(std::memory_order_acquire);
    return epoch != 0 && slot.candidateEpoch.load(std::memory_order_relaxed) == epoch;
}

bool ClientReadBandwidthScheduler::Impl::NeedsStarvationForce(const WorkerSlot &slot, uint64_t nowNs) const noexcept
{
    const uint64_t selectedNs = slot.lastSelectedNs.load(std::memory_order_relaxed);
    const uint64_t refreshedNs = slot.lastStateRefreshNs.load(std::memory_order_acquire);
    if (selectedNs == 0 || refreshedNs == 0) {
        return false;
    }
    const bool selectionStarved = nowNs >= selectedNs && nowNs - selectedNs >= starvationNs_;
    const uint64_t lastForceNs = slot.lastStarvationForceNs.load(std::memory_order_relaxed);
    const bool refreshStarved =
        nowNs >= refreshedNs && nowNs - refreshedNs >= starvationNs_ && lastForceNs < refreshedNs;
    return selectionStarved || refreshStarved;
}

ClientReadBandwidthScheduler::Impl::PortCounts ClientReadBandwidthScheduler::Impl::ReadPortCounts(
    const UbRoutingHealthSnapshot *snapshot, const HostPort &worker) const
{
    PortCounts result;
    if (snapshot == nullptr || worker.Empty()) {
        return result;
    }
    const auto iterator = snapshot->workers.find(worker);
    if (iterator == snapshot->workers.end()) {
        return result;
    }
    const auto &summary = iterator->second.portHealth;
    if (!HasKnownUbPortHealth(summary) || summary.totalPortCount == 0
        || summary.badPortCount > summary.totalPortCount) {
        return result;
    }
    result.failed = summary.badPortCount;
    result.total = summary.totalPortCount;
    result.known = true;
    return result;
}

uint64_t ClientReadBandwidthScheduler::Impl::CalculateLatencyCost(uint64_t p50Ns, uint64_t p99Ns,
                                                                  bool affinity) const noexcept
{
    if (p50Ns == 0 || p99Ns == 0 || p99Ns > cutInGuardNs_) {
        return std::numeric_limits<uint64_t>::max();
    }
    const uint64_t tailCost =
        p99Ns > std::numeric_limits<uint64_t>::max() / 3ULL ? std::numeric_limits<uint64_t>::max() : 3ULL * p99Ns;
    const uint64_t weightedLatency = SaturatingAdd(p50Ns, tailCost) / 4ULL;
    return affinity ? weightedLatency : SaturatingAdd(weightedLatency, nonAffinityPenaltyNs_);
}

uint64_t ClientReadBandwidthScheduler::Impl::CalculateLiveWeight(uint64_t latencyCostNs,
                                                                 const PortCounts &ports) const noexcept
{
    if (ports.AllFailed() || latencyCostNs == 0 || latencyCostNs == std::numeric_limits<uint64_t>::max()) {
        return 0;
    }

    const uint32_t totalPorts = ports.known ? ports.total : 1U;
    const uint32_t failedPorts = ports.known ? ports.failed : 0U;
    const uint32_t healthyPorts = totalPorts - failedPorts;

#if defined(__SIZEOF_INT128__)
    const __uint128_t numerator = static_cast<__uint128_t>(referenceLatencyNs_) * maxSelectionWeight_ * healthyPorts;

    const __uint128_t denominator = (static_cast<__uint128_t>(referenceLatencyNs_) + latencyCostNs) * totalPorts;

    const uint64_t weight = static_cast<uint64_t>(numerator / denominator);

    return std::max<uint64_t>(1, weight);
#else
    const uint64_t smoothedCost = SaturatingAdd(referenceLatencyNs_, latencyCostNs);
    if (smoothedCost > std::numeric_limits<uint64_t>::max() / totalPorts) {
        return 1;
    }

    const uint64_t healthAdjustedCost = smoothedCost * totalPorts / healthyPorts;

    if (referenceLatencyNs_ > std::numeric_limits<uint64_t>::max() / maxSelectionWeight_) {
        return 1;
    }

    const uint64_t weight = referenceLatencyNs_ * maxSelectionWeight_ / healthAdjustedCost;

    return std::max<uint64_t>(1, weight);
#endif
}

void ClientReadBandwidthScheduler::Impl::RefreshPrecomputedCosts(WorkerSlot &slot, uint32_t p50Ns, uint32_t p99Ns)
{
    const uint64_t invalidCost = std::numeric_limits<uint64_t>::max();
    const uint64_t affinityCost = CalculateLatencyCost(p50Ns, p99Ns, true);
    const uint64_t nonAffinityCost =
        affinityCost == invalidCost ? invalidCost : SaturatingAdd(affinityCost, nonAffinityPenaltyNs_);

    slot.affinityCostNs.store(affinityCost, std::memory_order_release);
    slot.nonAffinityCostNs.store(nonAffinityCost, std::memory_order_release);
}

uint64_t ClientReadBandwidthScheduler::Impl::LoadPrecomputedCost(const WorkerSlot &slot, bool affinity) const noexcept
{
    return affinity ? slot.affinityCostNs.load(std::memory_order_acquire)
                    : slot.nonAffinityCostNs.load(std::memory_order_acquire);
}

bool ClientReadBandwidthScheduler::Impl::AcceptWeightedProbe(const WeightedProbe &probe,
                                                             uint64_t randomValue) const noexcept
{
#if defined(__SIZEOF_INT128__)
    const uint32_t totalPorts = probe.ports.known ? probe.ports.total : 1U;
    const uint32_t failedPorts = probe.ports.known ? probe.ports.failed : 0U;
    const uint32_t healthyPorts = totalPorts - failedPorts;

    // Acceptance probability:
    // min(1, referenceLatency * healthyPorts / (latencyCost * totalPorts)).
    const __uint128_t numerator = static_cast<__uint128_t>(referenceLatencyNs_) * healthyPorts;
    const __uint128_t denominator = (static_cast<__uint128_t>(referenceLatencyNs_) + probe.latencyCostNs) * totalPorts;

    if (numerator >= denominator) {
        return true;
    }

    // The normal configuration fits in uint64_t. Multiply-high maps the random
    // value into [0, denominator) without integer division.
    constexpr __uint128_t kUint64Maximum = std::numeric_limits<uint64_t>::max();
    if (numerator <= kUint64Maximum && denominator <= kUint64Maximum) {
        const __uint128_t scaled = static_cast<__uint128_t>(randomValue) * static_cast<uint64_t>(denominator);
        return static_cast<uint64_t>(scaled >> 64U) < static_cast<uint64_t>(numerator);
    }

    // Extreme-value fallback. Using 32 random bits prevents 128-bit overflow.
    const uint32_t draw = static_cast<uint32_t>(randomValue >> ACCEPTANCE_RANDOM_BITS);
    return static_cast<__uint128_t>(draw) * denominator < (numerator << ACCEPTANCE_RANDOM_BITS);
#else
    // Compatibility path for compilers without 128-bit integer support.
    const uint32_t draw = static_cast<uint32_t>(randomValue >> ACCEPTANCE_RANDOM_BITS);
    const uint64_t weight = CalculateLiveWeight(probe.latencyCostNs, probe.ports);

    return weight != 0 && static_cast<uint64_t>(draw) * maxSelectionWeight_ < (weight << ACCEPTANCE_RANDOM_BITS);
#endif
}

size_t ClientReadBandwidthScheduler::Impl::MapRandomToIndex(uint64_t randomValue, size_t count) noexcept
{
    if (count <= 1) {
        return 0;
    }

    // Candidate capacity is uint32_t. Multiply-high avoids runtime modulo division.
    const uint64_t randomHigh = static_cast<uint32_t>(randomValue >> 32U);
    return static_cast<size_t>((randomHigh * static_cast<uint32_t>(count)) >> 32U);
}

ClientReadBandwidthScheduler::Impl::CandidateCheckResult ClientReadBandwidthScheduler::Impl::CheckCandidate(
    const CandidateEntry &entry, const std::vector<HostPort> &exclude,
    const std::array<const WorkerSlot *, kRejectedSlotCapacity> &rejected, size_t rejectedCount, uint64_t nowNs,
    SelectionMode mode) const
{
    WorkerSlot *slot = entry.slot;
    if (slot == nullptr) {
        return CandidateCheckResult::kInvalidSlot;
    }
    if (slot->generation.load(std::memory_order_acquire) != entry.slotGeneration) {
        return CandidateCheckResult::kInvalidGeneration;
    }
    if (IsRejected(slot, rejected, rejectedCount)) {
        return CandidateCheckResult::kRejected;
    }
    if (IsExcluded(entry.worker, exclude)) {
        return CandidateCheckResult::kExcluded;
    }
    if (mode == SelectionMode::kStarvation && !NeedsStarvationForce(*slot, nowNs)) {
        return CandidateCheckResult::kNotStarved;
    }
    // Keep stale latency samples selectable. This matches the original logic,
    // where the stale rejection was intentionally disabled; freshness remains
    // observable through GetWorkerStatus().
    return CandidateCheckResult::kUsable;
}

ClientReadBandwidthScheduler::Impl::WeightedProbe ClientReadBandwidthScheduler::Impl::PrepareWeightedProbe(
    CandidateEntry &entry, const std::vector<HostPort> &exclude, const HostPort &preferredWorker,
    const UbRoutingHealthSnapshot *snapshot, const std::array<const WorkerSlot *, kRejectedSlotCapacity> &rejected,
    size_t rejectedCount, uint64_t nowNs, SelectionMode mode) const
{
    if (CheckCandidate(entry, exclude, rejected, rejectedCount, nowNs, mode) != CandidateCheckResult::kUsable) {
        return {};
    }

    const PortCounts ports = ReadPortCounts(snapshot, entry.worker);
    if (ports.AllFailed()) {
        return {};
    }

    const uint64_t latencyCostNs = LoadPrecomputedCost(*entry.slot, entry.worker == preferredWorker);
    if (latencyCostNs == 0
        || (mode == SelectionMode::kWeighted && latencyCostNs == std::numeric_limits<uint64_t>::max())) {
        return {};
    }

    return WeightedProbe{ &entry, latencyCostNs, ports };
}

ClientReadBandwidthScheduler::Impl::CandidateEntry *ClientReadBandwidthScheduler::Impl::TryUseWeightedProbe(
    const WeightedProbe &probe, const std::vector<HostPort> &exclude,
    const std::vector<std::shared_ptr<IWorkerFilter>> &filters, WorkerAccessAction action,
    std::array<const WorkerSlot *, kRejectedSlotCapacity> &rejected, size_t &rejectedCount, uint64_t nowNs,
    size_t rejectLimit, bool &stop) const
{
    stop = false;
    if (probe.entry == nullptr) {
        return nullptr;
    }
    // Do not recalculate the accepted weight. The latency cost used by this
    // draw came from Observe(), and its fault factor already came from the
    // immutable snapshot in PrepareWeightedProbe(). Recheck only identity and
    // request-local eligibility that may have changed before filter probing.
    if (CheckCandidate(*probe.entry, exclude, rejected, rejectedCount, nowNs, SelectionMode::kWeighted)
        != CandidateCheckResult::kUsable) {
        return nullptr;
    }
    if (IsWorkerAvailable(probe.entry->worker, filters, action)) {
        return probe.entry;
    }
    AddRejectedSlot(probe.entry->slot, rejected, rejectedCount);
    stop = rejectedCount >= rejectLimit;
    return nullptr;
}

ClientReadBandwidthScheduler::Impl::CandidateEntry *ClientReadBandwidthScheduler::Impl::TryPickStarvedCandidate(
    CandidateTable &table, const std::vector<HostPort> &exclude, const HostPort &preferredWorker,
    const std::vector<std::shared_ptr<IWorkerFilter>> &filters, WorkerAccessAction action,
    const UbRoutingHealthSnapshot *snapshot,
    std::array<const WorkerSlot *, kRejectedSlotCapacity> &rejected, size_t &rejectedCount, uint64_t nowNs,
    size_t rejectLimit)
{
    const uint32_t probes = static_cast<uint32_t>(std::min<size_t>(kMaxStarvationProbes, table.count));
    for (uint32_t probeIndex = 0; probeIndex < probes; ++probeIndex) {
        const uint64_t cursor = starvationProbeCursor_.fetch_add(1, std::memory_order_relaxed);
        CandidateEntry &entry = table.entries[cursor % table.count];
        WeightedProbe probe = PrepareWeightedProbe(entry, exclude, preferredWorker, snapshot, rejected, rejectedCount,
                                                   nowNs, SelectionMode::kStarvation);
        if (probe.entry == nullptr) {
            continue;
        }
        uint8_t expectedClaim = 0;
        if (!entry.slot->starvationClaimed.compare_exchange_strong(expectedClaim, 1, std::memory_order_acq_rel,
                                                                   std::memory_order_relaxed)) {
            continue;
        }
        AtomicByteReleaseGuard claimGuard(entry.slot->starvationClaimed);
        if (!IsWorkerAvailable(entry.worker, filters, action)) {
            AddRejectedSlot(entry.slot, rejected, rejectedCount);
            if (rejectedCount >= rejectLimit) {
                return nullptr;
            }
            continue;
        }
        uint64_t expectedSelected = entry.slot->lastSelectedNs.load(std::memory_order_relaxed);
        if (NeedsStarvationForce(*entry.slot, nowNs)
            && entry.slot->lastSelectedNs.compare_exchange_strong(expectedSelected, nowNs, std::memory_order_acq_rel,
                                                                  std::memory_order_relaxed)) {
            AtomicMax(entry.slot->lastStarvationForceNs, nowNs);
            return &entry;
        }
    }
    return nullptr;
}

ClientReadBandwidthScheduler::Impl::CandidateEntry *ClientReadBandwidthScheduler::Impl::PickWeightedCandidate(
    const std::string &requestKey, CandidateTable &table, const std::vector<HostPort> &exclude,
    const HostPort &preferredWorker, const std::vector<std::shared_ptr<IWorkerFilter>> &filters,
    WorkerAccessAction action, const UbRoutingHealthSnapshot *snapshot,
    std::array<const WorkerSlot *, kRejectedSlotCapacity> &rejected, size_t &rejectedCount, uint64_t nowNs,
    size_t rejectLimit)
{
    const uint64_t seed = GenerateSelectionSeed(requestKey);

    for (uint32_t attempt = 0; attempt < kMaxWeightedDrawAttempts; ++attempt) {
        const uint64_t candidateValue = GenerateAttemptValue(seed, attempt, CANDIDATE_STREAM);
        const size_t candidateIndex = MapRandomToIndex(candidateValue, table.count);
        CandidateEntry &entry = table.entries[candidateIndex];

        const WeightedProbe probe = PrepareWeightedProbe(entry, exclude, preferredWorker, snapshot, rejected,
                                                         rejectedCount, nowNs, SelectionMode::kWeighted);
        if (probe.entry == nullptr) {
            continue;
        }

        const uint64_t acceptanceValue = GenerateAttemptValue(seed, attempt, ACCEPTANCE_STREAM);
        if (!AcceptWeightedProbe(probe, acceptanceValue)) {
            continue;
        }

        bool stop = false;
        CandidateEntry *selected =
            TryUseWeightedProbe(probe, exclude, filters, action, rejected, rejectedCount, nowNs, rejectLimit, stop);
        if (selected != nullptr || stop) {
            return selected;
        }
    }

    return nullptr;
}

ClientReadBandwidthScheduler::Impl::CandidateEntry *ClientReadBandwidthScheduler::Impl::PickOneCandidate(
    const std::string &requestKey, CandidateTable &table, const std::vector<HostPort> &exclude,
    const HostPort &preferredWorker, const std::vector<std::shared_ptr<IWorkerFilter>> &filters,
    WorkerAccessAction action, const UbRoutingHealthSnapshot *snapshot,
    std::array<const WorkerSlot *, kRejectedSlotCapacity> &rejected, size_t &rejectedCount, uint64_t nowNs,
    size_t rejectLimit, SelectionMode &mode)
{
    CandidateEntry *candidate = TryPickStarvedCandidate(table, exclude, preferredWorker, filters, action, snapshot,
                                                        rejected, rejectedCount, nowNs, rejectLimit);
    if (candidate != nullptr) {
        mode = SelectionMode::kStarvation;
        return candidate;
    }
    mode = SelectionMode::kWeighted;
    return PickWeightedCandidate(requestKey, table, exclude, preferredWorker, filters, action, snapshot, rejected,
                                 rejectedCount, nowNs, rejectLimit);
}

bool ClientReadBandwidthScheduler::Impl::IsWorkerAvailable(
    const HostPort &worker, const std::vector<std::shared_ptr<IWorkerFilter>> &filters,
    WorkerAccessAction action) const
{
    for (const std::shared_ptr<IWorkerFilter> &filter : filters) {
        if (filter != nullptr && !filter->IsAvailable(worker, action)) {
            return false;
        }
    }
    return true;
}

uint64_t ClientReadBandwidthScheduler::Impl::GenerateSelectionSeed(const std::string &requestKey) noexcept
{
    const uint64_t sequence = selectionSequence_.fetch_add(1, std::memory_order_relaxed);
    uint64_t value = Fnv1a64(requestKey) ^ clientSaltHash_;
    value ^= sequence * HASH_GOLDEN_RATIO;
    return Mix64(value);
}

uint64_t ClientReadBandwidthScheduler::Impl::GenerateAttemptValue(uint64_t seed, uint32_t attempt,
                                                                  uint64_t streamSalt) noexcept
{
    uint64_t value = seed ^ streamSalt;
    value += (static_cast<uint64_t>(attempt) + 1ULL) * HASH_GOLDEN_RATIO;
    return Mix64(value);
}

void ClientReadBandwidthScheduler::Impl::Observe(const HostPort &worker, uint32_t p50Ns, uint32_t p99Ns,
                                                 uint64_t latencyVersion, const std::string &callFrom)
{
    if (!Enabled() || worker.Empty() || p50Ns <= 0 || p99Ns <= 0 || p50Ns > p99Ns) {
        return;
    }
    const uint64_t nowNs = SteadyNowNs();
    SlotPin pin = FindOrCreateSlot(worker, nowNs);
    WorkerSlot *slot = pin.Get();
    if (slot == nullptr) {
        return;
    }
    bool refreshed = false;
    const uint64_t newPacked = (static_cast<uint64_t>(p50Ns) << 32U) | static_cast<uint64_t>(p99Ns);
    const uint64_t oldPacked = slot->packedLatency.load(std::memory_order_acquire);
    if (newPacked != oldPacked) {
        uint8_t expected = 0;
        if (!slot->updateBusy.compare_exchange_strong(expected, 1, std::memory_order_acq_rel,
                                                      std::memory_order_relaxed)) {
            return;
        }
        AtomicByteReleaseGuard updateGuard(slot->updateBusy);
        RefreshPrecomputedCosts(*slot, p50Ns, p99Ns);
        slot->packedLatency.store(newPacked, std::memory_order_relaxed);
        refreshed = true;
    }
    slot->lastStateRefreshNs.store(nowNs, std::memory_order_release);
    slot->latencyVersion.store(latencyVersion, std::memory_order_release);
    LogObserve(worker, callFrom, nowNs, p50Ns, p99Ns, latencyVersion, refreshed, LoadPrecomputedCost(*slot, true),
               LoadPrecomputedCost(*slot, false));
}

bool ClientReadBandwidthScheduler::Impl::GetWorkerStatus(const HostPort &worker,
                                                         const std::shared_ptr<const UbRoutingHealthSnapshot> &snapshot,
                                                         WorkerStatus &out) const
{
    SlotPin pin = FindSlot(worker);
    WorkerSlot *slot = pin.Get();
    if (slot == nullptr) {
        out = {};
        return false;
    }
    const WorkerState state = ReadWorkerState(*slot, SteadyNowNs());
    const PortCounts ports = ReadPortCounts(snapshot.get(), worker);
    out.p50Ns = static_cast<uint32_t>(state.p50Ns);
    out.p99Ns = static_cast<uint32_t>(state.p99Ns);
    out.latencyVersion = state.latencyVersion;
    out.affinityWeight = CalculateLiveWeight(LoadPrecomputedCost(*slot, true), ports);
    out.nonAffinityWeight = CalculateLiveWeight(LoadPrecomputedCost(*slot, false), ports);
    out.exists = state.exists;
    out.fresh = state.fresh;
    out.currentCandidate = IsCurrentCandidate(*slot);
    return state.exists;
}

bool ClientReadBandwidthScheduler::Impl::ShouldKeepAffinity(
    const HostPort &worker, const std::string &requestKey,
    const std::shared_ptr<const UbRoutingHealthSnapshot> &snapshot, uint64_t &taskId)
{
    if (!Enabled()) {
        taskId = 0;
        return true;
    }
    const uint64_t nowNs = SteadyNowNs();
    taskId = taskIdCounter_.fetch_add(1, std::memory_order_relaxed) + 1;
    const PortCounts ports = ReadPortCounts(snapshot.get(), worker);
    const bool keepAffinity = !ports.known || ports.failed == 0;
    if (keepAffinity) {
        SlotPin pin = FindOrCreateSlot(worker, nowNs);
        if (pin) {
            MarkSelected(*pin.Get(), nowNs);
        }
    }
    if (!keepAffinity) {
        LOG(INFO) << "[RBS_C_INFO] Enter scheduling algorithm." << " preferred_worker=" << worker.ToString()
                  << "failed port num: " << ports.failed;
    }
    LogDecisionEntry(worker, requestKey, ports, taskId, nowNs, keepAffinity);
    return keepAffinity;
}

bool ClientReadBandwidthScheduler::Impl::SelectWorkerFast(
    const std::string &requestKey, const std::vector<HostPort> &exclude, const HostPort &preferredWorker,
    const std::vector<std::shared_ptr<IWorkerFilter>> &filters,
    const std::shared_ptr<const UbRoutingHealthSnapshot> &snapshot, uint64_t taskId, HostPort &selected,
    WorkerAccessAction action)
{
    if (!Enabled()) {
        return false;
    }
    const uint64_t nowNs = SteadyNowNs();
    SelectionMode mode = SelectionMode::kWeighted;
    {
        TablePin tablePin = AcquireCandidateTable();
        CandidateTable *table = tablePin.Get();
        if (table != nullptr && table->count != 0) {
            std::array<const WorkerSlot *, kRejectedSlotCapacity> rejected{};
            size_t rejectedCount = 0;
            const size_t rejectLimit = std::min<size_t>(config_.latencyAvailabilityRetryCount, kRejectedSlotCapacity);
            CandidateEntry *candidate =
                PickOneCandidate(requestKey, *table, exclude, preferredWorker, filters, action, snapshot.get(),
                                 rejected, rejectedCount, nowNs, rejectLimit, mode);
            if (candidate != nullptr) {
                selected = candidate->worker;
                MarkSelected(*candidate->slot, nowNs);
                LogDecisionFinal(preferredWorker, &selected, taskId, nowNs, "algorithm_selected", mode);
                return true;
            }
        }
    }
    selected = preferredWorker;
    SlotPin pin = FindOrCreateSlot(selected, nowNs);
    if (pin) {
        MarkSelected(*pin.Get(), nowNs);
    }
    LogDecisionFinal(preferredWorker, &selected, taskId, nowNs, "fallback_preferred", mode);
    return true;
}

void ClientReadBandwidthScheduler::Impl::MaybeWarnHashTable(uint64_t nowNs) const
{
    const uint64_t tombstones = tombstoneCount_.load(std::memory_order_relaxed);
    const uint64_t longProbes = longProbeCount_.exchange(0, std::memory_order_relaxed);
    if (tombstones * HASH_WARNING_TOMBSTONE_SCALE < workerSlotCapacity_ && longProbes == 0) {
        return;
    }
    uint64_t lastNs = lastHashWarningNs_.load(std::memory_order_relaxed);
    if (lastNs != 0 && nowNs - lastNs < kWarningIntervalNs) {
        return;
    }
    if (!lastHashWarningNs_.compare_exchange_strong(lastNs, nowNs, std::memory_order_relaxed,
                                                    std::memory_order_relaxed)) {
        return;
    }
    LOG(WARNING) << "[RBS_C] event=SLOT_TABLE_DEGRADED"
                 << " tombstones=" << tombstones << " capacity=" << workerSlotCapacity_
                 << " long_probe_count=" << longProbes;
}

void ClientReadBandwidthScheduler::Impl::LogObserve(const HostPort &worker, const std::string &callFrom, uint64_t nowNs,
                                                    uint32_t p50Ns, uint32_t p99Ns, uint64_t latencyVersion,
                                                    bool refreshed, uint64_t affinityCost,
                                                    uint64_t nonAffinityCost) const
{
    LOG(INFO) << "[RBS_C] event=OBSERVE trace=" << Trace::Instance().GetTraceIDPtr() << " ts_ns=" << nowNs
              << " client_ip=" << clientIp_ << " call_from=" << callFrom << " worker=" << worker.ToString()
              << " p50_ns=" << p50Ns << " p99_ns=" << p99Ns << " latency_version=" << latencyVersion
              << " refreshed=" << (refreshed ? 1 : 0) << " affinityCost=" << affinityCost
              << " nonAffinityCost=" << nonAffinityCost;
}

void ClientReadBandwidthScheduler::Impl::LogDecisionEntry(const HostPort &worker, const std::string &requestKey,
                                                          const PortCounts &ports, uint64_t taskId, uint64_t nowNs,
                                                          bool keepAffinity) const
{
    LOG(INFO) << "[RBS_C] event=DECIDE_ENTRY trace=" << Trace::Instance().GetTraceIDPtr() << " task=" << taskId
              << " ts_ns=" << nowNs << " client_ip=" << clientIp_ << " request_key=" << requestKey
              << " preferred_worker=" << worker.ToString() << " health_known=" << (ports.known ? 1 : 0)
              << " failed_port_count=" << ports.failed << " total_port_count=" << ports.total
              << " entered_algorithm=" << (keepAffinity ? 0 : 1);
}

void ClientReadBandwidthScheduler::Impl::LogDecisionFinal(const HostPort &preferredWorker, const HostPort *selected,
                                                          uint64_t taskId, uint64_t nowNs, const char *reason,
                                                          SelectionMode mode) const
{
    LOG(INFO) << "[RBS_C] event=DECIDE_FINAL trace=" << Trace::Instance().GetTraceIDPtr() << " task=" << taskId
              << " ts_ns=" << nowNs << " client_ip=" << clientIp_ << " preferred_worker=" << preferredWorker.ToString()
              << " selected=" << (selected == nullptr ? "" : selected->ToString()) << " reason=" << reason
              << " mode=" << (mode == SelectionMode::kStarvation ? "starvation" : "weighted");
}

ClientReadBandwidthScheduler::ClientReadBandwidthScheduler(Config config)
    : impl_(new Impl(std::move(config)))
{
}

ClientReadBandwidthScheduler::~ClientReadBandwidthScheduler() = default;

bool ClientReadBandwidthScheduler::Enabled() const noexcept
{
    return impl_->Enabled();
}

void ClientReadBandwidthScheduler::Observe(const HostPort &worker, uint32_t p50Ns, uint32_t p99Ns,
                                           uint64_t latencyVersion, const std::string &callFrom)
{
    impl_->Observe(worker, p50Ns, p99Ns, latencyVersion, callFrom);
}

void ClientReadBandwidthScheduler::RefreshCandidates(const std::vector<HostPort> &availableWorkers)
{
    impl_->RefreshCandidates(availableWorkers);
}

bool ClientReadBandwidthScheduler::GetWorkerStatus(
    const HostPort &worker, const std::shared_ptr<const UbRoutingHealthSnapshot> &healthSnapshot,
    WorkerStatus &out) const
{
    return impl_->GetWorkerStatus(worker, healthSnapshot, out);
}

bool ClientReadBandwidthScheduler::ShouldKeepAffinity(
    const HostPort &affinityWorker, const std::string &requestKey,
    const std::shared_ptr<const UbRoutingHealthSnapshot> &healthSnapshot, std::uint64_t &taskId)
{
    return impl_->ShouldKeepAffinity(affinityWorker, requestKey, healthSnapshot, taskId);
}

bool ClientReadBandwidthScheduler::SelectWorkerFast(
    const std::string &requestKey, const std::vector<HostPort> &exclude, const HostPort &preferredWorker,
    const std::vector<std::shared_ptr<IWorkerFilter>> &filters,
    const std::shared_ptr<const UbRoutingHealthSnapshot> &healthSnapshot, std::uint64_t taskId,
    HostPort &selected, WorkerAccessAction action)
{
    return impl_->SelectWorkerFast(requestKey, exclude, preferredWorker, filters, healthSnapshot, taskId, selected,
                                   action);
}

bool ClientReadBandwidthScheduler::SelectWorker(
    const std::string &requestKey, const std::vector<std::shared_ptr<IWorkerFilter>> &filters,
    const std::vector<HostPort> &exclude, const HostPort &preferredWorker,
    const std::shared_ptr<const UbRoutingHealthSnapshot> &healthSnapshot, std::uint64_t taskId,
    HostPort &selected, WorkerAccessAction action)
{
    return SelectWorkerFast(requestKey, exclude, preferredWorker, filters, healthSnapshot, taskId, selected, action);
}

void ClientReadBandwidthScheduler::SetCutInGuardNs(std::uint64_t latencyHardLimitMs)
{
    impl_->SetCutInGuardNs(latencyHardLimitMs);
}

}  // namespace client
}  // namespace datasystem
