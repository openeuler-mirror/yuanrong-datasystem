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
 * Description: Eviction/rebalance/combined workload telemetry.
 *
 * This manual LEVEL1 ST exports per-worker memory-watermark and Get-hit timeseries while running
 * deterministic recommendation workloads plus an independent 12 MiB Block-KVC recall workload. By default they use
 * synthetic locality; setting DS_TLM_KUAIRAND_TRACE replays a normalized KuaiRand request trace:
 *   - recall: broad, long-tail access locality;
 *   - fine_rank: highly concentrated access locality.
 *
 * The test is deliberately policy-agnostic and can be copied to the baseline commit. Run the
 * baseline without DS_TLM_EXTRA_WORKER_GFLAGS, then run the modified tree with, for example,
 *   DS_TLM_EXTRA_WORKER_GFLAGS="-eviction_strategy=heat -rebalance_strategy=heat"
 * so both versions execute exactly the same key placement, warm-up, pressure, and read trace.
 * DS_TLM_VARIANT only labels the CSV (for example, baseline or heat); it does not alter behavior.
 *
 * The worker already exposes all sampled signals through resource.log:
 *   - SHARED_MEMORY: memory usage/limit/watermark fill ratio;
 *   - SPILL_HARD_DISK: spill usage/limit/fill ratio;
 *   - OC_HIT_NUM: cumulative mem/disk/l2/remote/miss Get counters.
 *   - OBJECT_COPY_WATERMARK: cached hot-primary/primary bytes and capacity-relative watermarks.
 * Interval hit rates are calculated from adjacent counter samples. The test gates only data
 * correctness and telemetry production; hit-rate/watermark improvements are comparison outputs,
 * not pass/fail thresholds.
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstring>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "client/kv_cache/kv_client_common.h"
#include "cluster/external_cluster.h"
#include "common.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/metrics/res_metric_collector.h"
#include "datasystem/common/util/format.h"
#include "datasystem/kv_client.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace st {
namespace {
constexpr int kLogIntervalSec = 1;
constexpr int kBaselineSampleIntervals = 2;
constexpr int kDefaultMeasureSec = 20;
constexpr int kGetTimeoutMs = 30'000;
constexpr int kSetRetryCount = 20;
constexpr int kSetRetryIntervalMs = 100;
constexpr int kHashRingReadyTimeoutSec = 30;
constexpr int kAccessPlanSize = 8000;  // large enough that sampled unique keys exceed 64MiB working set

// Fixed PRNG seed so synthetic baseline and heat runs use the same key sizes, popularity samples and oneHits set.
// KuaiRand replay uses the normalized file order and does not use this seed for request selection.
constexpr uint64_t kWorkloadSeed = 42;

constexpr char kExtraWorkerFlagsEnv[] = "DS_TLM_EXTRA_WORKER_GFLAGS";
constexpr char kVariantEnv[] = "DS_TLM_VARIANT";
constexpr char kCommitEnv[] = "DS_TLM_COMMIT";
constexpr char kMeasureSecEnv[] = "DS_TLM_MEASURE_SEC";
constexpr char kKuaiRandTraceEnv[] = "DS_TLM_KUAIRAND_TRACE";
constexpr char kCombinedMemoryMbEnv[] = "DS_TLM_COMBINED_MEMORY_MB";
constexpr char kBlockMemoryMbEnv[] = "DS_TLM_BLOCK_MEMORY_MB";
constexpr char kBlockResourceScaleEnv[] = "DS_TLM_BLOCK_RESOURCE_SCALE";
constexpr char kBlocksPerRequestEnv[] = "DS_TLM_BLOCKS_PER_REQUEST";
constexpr char kTokensPerBlockEnv[] = "DS_TLM_TOKENS_PER_BLOCK";
constexpr char kBytesPerTokenEnv[] = "DS_TLM_BYTES_PER_TOKEN";
constexpr char kTargetRequestQpsEnv[] = "DS_TLM_TARGET_REQUEST_QPS";
constexpr char kBlockRequestThreadsEnv[] = "DS_TLM_BLOCK_REQUEST_THREADS";
constexpr char kBlockWarmupRequestsEnv[] = "DS_TLM_BLOCK_WARMUP_REQUESTS";
constexpr char kBlockColdScanRequestsEnv[] = "DS_TLM_BLOCK_COLD_SCAN_REQUESTS";
constexpr char kBlockColdWriteQpsEnv[] = "DS_TLM_BLOCK_COLD_WRITE_QPS";
constexpr char kBlockOwnerAffinityEnv[] = "DS_TLM_BLOCK_OWNER_AFFINITY";
constexpr char kOutputDirEnv[] = "DS_TLM_OUTPUT_DIR";
// Physical capacity includes copy-on-Get/migration headroom. Combined eviction watermarks below
// independently bound the resident set used for the near-70% Clock/Memory baseline.
constexpr int kDefaultCombinedMemoryMb = 48;
// Resource-faithful accelerated model: three test workers represent three of the 64 production nodes. Memory and KVC
// payload bytes use the same 1:64 scale, preserving the 70 GiB / 12 MiB capacity ratio and the modeled miss bandwidth.
constexpr int kProductionClusterNodes = 64;
constexpr int kProductionClusterQps = 4'500;
constexpr int kProductionNodeMemoryMiB = 70 * 1024;
constexpr int kBlockTestWorkers = 3;
constexpr int kDefaultBlockResourceScale = 64;
constexpr int kDefaultBlockMemoryMb = kProductionNodeMemoryMiB / kDefaultBlockResourceScale;
constexpr double kBlockClockMinMemRate = 0.68;
constexpr double kBlockClockMaxMemRate = 0.72;
constexpr double kBlockHeatMinMemRate = 0.79;
constexpr int kDefaultBlocksPerRequest = 16;
constexpr int kDefaultTokensPerBlock = 64;
constexpr int kDefaultBytesPerToken = 12 * 1024;
constexpr int kDefaultTargetRequestQps =
    (kProductionClusterQps * kBlockTestWorkers + kProductionClusterNodes - 1) / kProductionClusterNodes;
// Each scaled in-flight logical request retains 192 KiB of ReadOnlyBuffers until validation finishes. Keep one request
// thread per simulated node so the per-node offered load remains faithful to 4,500 QPS / 64 nodes.
// Larger validation clusters can override this explicitly.
constexpr int kDefaultBlockRequestThreads = 3;
constexpr int kDefaultBlockWarmupRequests = 10'000;
constexpr int kDefaultBlockColdScanRequests = 800;
constexpr int kBlockContractMeasureSec = 60;
// The production model attributes 30% of 4,500 requests/s to cache misses. At the 1:64 acceleration scale this is
// 64 newly filled logical KVCs/s across the three test workers. Sustaining that pressure through MEASURE avoids a
// one-shot pre-measure scan whose effect disappears in longer runs.
constexpr int kDefaultBlockColdWriteQps = (kDefaultTargetRequestQps * 30 + 99) / 100;
constexpr size_t kBlockAccessThreadStride = 7919;

enum HitSlot : size_t { MEM = 0, DISK = 1, L2 = 2, REMOTE = 3, MISS = 4, HIT_SLOT_COUNT = 5 };

enum class WorkloadPhase : uint8_t { BASELINE, PRESSURE, WARMUP, MEASURE };

enum class AccessKeyPlacement : uint8_t { CONTIGUOUS, WORKER_ROUND_ROBIN };

// Object-size distribution. Shaped after Meta CacheLib's public kvcache_reg config
// (small-object-dominated with a long tail of larger objects). The weighted mean is set so that
// roughly a thousand keys cross the worker shared-memory limit (the goal is to exercise
// eviction/rebalance, not to mirror the 335-byte production mean); keeping the key count modest
// keeps the Set pressure phase fast enough to reach the measure window. Fixed content per size
// bucket so value correctness can still be asserted byte-for-byte.
struct SizeBucket {
    uint32_t bytes;
    double prob;
};
const std::vector<SizeBucket> kSizeBuckets = {
    { 1024, 0.25 }, { 4096, 0.25 }, { 16384, 0.25 }, { 65536, 0.20 }, { 131072, 0.05 }
};  // weighted mean ~24KB; ~2700 keys cross 64MiB. 128KB cap keeps the pressure Set phase fast.

// Synthetic workload profile. popularityWeights is a discrete probability vector over key buckets
// (heavy-tailed; the most popular bucket is sampled most). oneHitsRatio is the fraction of keys
// that are accessed exactly once during measure (never warmed). KuaiRand replay derives both roles
// from observed event counts instead.
struct WorkloadProfile {
    const char *name;
    std::vector<double> popularityWeights;
    double oneHitsRatio;
};

// recall: broad long tail (top ~10% of buckets carry ~44% of requests) -- after Meta kvcache_reg.
const WorkloadProfile kRecallProfile{ "recall",
    { 27638685, 11027400, 5522005, 4087968, 7081918, 6292706, 2471906, 1979646, 1919826, 1494831 },
    0.015 };
// fine_rank: highly concentrated (top ~10% of buckets carry ~85% of requests) -- after Meta cdn.
const WorkloadProfile kFineRankProfile{ "fine_rank",
    { 800000, 80000, 30000, 15000, 10000, 8000, 6000, 5000, 4000, 3000 },
    0.009 };
const WorkloadProfile kBlockRecallProfile{ "block_recall_12mb", kRecallProfile.popularityWeights, 0.0 };

std::string GetEnv(const char *name, const std::string &defaultValue = "")
{
    const char *value = std::getenv(name);
    return value == nullptr ? defaultValue : value;
}

int GetPositiveEnvInt(const char *name, int defaultValue)
{
    const std::string value = GetEnv(name);
    if (value.empty()) {
        return defaultValue;
    }
    errno = 0;
    char *end = nullptr;
    const long parsed = std::strtol(value.c_str(), &end, 10);
    if (errno != 0 || end == value.c_str() || *end != '\0' || parsed <= 0 || parsed > INT32_MAX) {
        return defaultValue;
    }
    return static_cast<int>(parsed);
}

int GetNonNegativeEnvInt(const char *name, int defaultValue)
{
    const std::string value = GetEnv(name);
    if (value.empty()) {
        return defaultValue;
    }
    errno = 0;
    char *end = nullptr;
    const long parsed = std::strtol(value.c_str(), &end, 10);
    if (errno != 0 || end == value.c_str() || *end != '\0' || parsed < 0 || parsed > INT32_MAX) {
        return defaultValue;
    }
    return static_cast<int>(parsed);
}

bool GetEnvBool(const char *name, bool defaultValue)
{
    const std::string value = GetEnv(name);
    if (value.empty()) {
        return defaultValue;
    }
    return value == "1" || value == "true" || value == "TRUE" || value == "yes";
}

struct BlockWorkloadConfig {
    uint64_t blocksPerRequest = 0;
    uint64_t tokensPerBlock = 0;
    uint64_t logicalBytesPerToken = 0;
    uint64_t payloadBytesPerToken = 0;
    uint64_t logicalBlockBytes = 0;
    uint64_t payloadBlockBytes = 0;
    uint64_t logicalRequestBytes = 0;
    uint64_t payloadRequestBytes = 0;
    uint64_t resourceScale = 0;
    uint64_t targetRequestQps = 0;
    uint64_t requestThreads = 0;
    uint64_t warmupRequests = 0;
    bool valid = false;
};

struct AggregateMemRateContract {
    bool enabled = false;
    double minimum = 0.0;
    double maximum = 1.0;
};

AggregateMemRateContract MakeBlockMemRateContract(int memoryMb, int resourceScale,
                                                  const std::string &evictionStrategy,
                                                  const std::string &rebalanceStrategy)
{
    const bool isDefaultGeometry = memoryMb == kDefaultBlockMemoryMb && resourceScale == kDefaultBlockResourceScale;
    if (isDefaultGeometry && evictionStrategy == "clock" && rebalanceStrategy == "memory") {
        return { true, kBlockClockMinMemRate, kBlockClockMaxMemRate };
    }
    if (isDefaultGeometry && evictionStrategy == "heat" && rebalanceStrategy == "heat") {
        return { true, kBlockHeatMinMemRate, 1.0 };
    }
    return {};
}

bool CheckedMultiply(uint64_t lhs, uint64_t rhs, uint64_t &result)
{
    if (lhs != 0 && rhs > std::numeric_limits<uint64_t>::max() / lhs) {
        return false;
    }
    result = lhs * rhs;
    return true;
}

BlockWorkloadConfig MakeBlockWorkloadConfig(uint64_t blocksPerRequest, uint64_t tokensPerBlock,
                                            uint64_t logicalBytesPerToken, uint64_t resourceScale,
                                            uint64_t targetRequestQps, uint64_t requestThreads,
                                            uint64_t warmupRequests)
{
    BlockWorkloadConfig config;
    config.blocksPerRequest = blocksPerRequest;
    config.tokensPerBlock = tokensPerBlock;
    config.logicalBytesPerToken = logicalBytesPerToken;
    config.resourceScale = resourceScale;
    config.targetRequestQps = targetRequestQps;
    config.requestThreads = requestThreads;
    config.warmupRequests = warmupRequests;
    config.valid = config.blocksPerRequest > 0 && config.blocksPerRequest <= 10'000 && config.tokensPerBlock > 0
                   && config.logicalBytesPerToken > 0 && config.resourceScale > 0
                   && config.logicalBytesPerToken % config.resourceScale == 0 && config.targetRequestQps > 0
                   && config.requestThreads > 0 && config.warmupRequests > 0
                   && config.requestThreads <= 1024
                   && CheckedMultiply(config.tokensPerBlock, config.logicalBytesPerToken, config.logicalBlockBytes)
                   && CheckedMultiply(config.blocksPerRequest, config.logicalBlockBytes, config.logicalRequestBytes);
    if (config.valid) {
        config.payloadBytesPerToken = config.logicalBytesPerToken / config.resourceScale;
        config.payloadBlockBytes = config.logicalBlockBytes / config.resourceScale;
        config.payloadRequestBytes = config.logicalRequestBytes / config.resourceScale;
        config.valid = config.payloadBytesPerToken > 0
                       && config.payloadBlockBytes <= std::numeric_limits<uint32_t>::max();
    }
    return config;
}

BlockWorkloadConfig GetBlockWorkloadConfig()
{
    return MakeBlockWorkloadConfig(
        static_cast<uint64_t>(GetPositiveEnvInt(kBlocksPerRequestEnv, kDefaultBlocksPerRequest)),
        static_cast<uint64_t>(GetPositiveEnvInt(kTokensPerBlockEnv, kDefaultTokensPerBlock)),
        static_cast<uint64_t>(GetPositiveEnvInt(kBytesPerTokenEnv, kDefaultBytesPerToken)),
        static_cast<uint64_t>(GetPositiveEnvInt(kBlockResourceScaleEnv, kDefaultBlockResourceScale)),
        static_cast<uint64_t>(GetPositiveEnvInt(kTargetRequestQpsEnv, kDefaultTargetRequestQps)),
        static_cast<uint64_t>(GetPositiveEnvInt(kBlockRequestThreadsEnv, kDefaultBlockRequestThreads)),
        static_cast<uint64_t>(GetPositiveEnvInt(kBlockWarmupRequestsEnv, kDefaultBlockWarmupRequests)));
}

uint64_t ToU64(const std::string &value)
{
    errno = 0;
    char *end = nullptr;
    const unsigned long long parsed = std::strtoull(value.c_str(), &end, 10);
    if (errno != 0 || end == value.c_str()) {
        return 0;
    }
    return static_cast<uint64_t>(parsed);
}

double ToDouble(const std::string &value)
{
    errno = 0;
    char *end = nullptr;
    const double parsed = std::strtod(value.c_str(), &end);
    if (errno != 0 || end == value.c_str()) {
        return 0.0;
    }
    return parsed;
}

int64_t NowMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

const char *PhaseName(WorkloadPhase phase)
{
    switch (phase) {
        case WorkloadPhase::BASELINE:
            return "baseline";
        case WorkloadPhase::PRESSURE:
            return "pressure";
        case WorkloadPhase::WARMUP:
            return "warmup";
        case WorkloadPhase::MEASURE:
            return "measure";
    }
    return "unknown";
}

std::string SanitizeLabel(std::string value)
{
    if (value.empty()) {
        return "default";
    }
    for (char &ch : value) {
        const bool valid = (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9')
                           || ch == '_' || ch == '-';
        if (!valid) {
            ch = '_';
        }
    }
    return value;
}

struct KuaiRandEvent {
    uint64_t videoId = 0;
    bool engaged = false;
};

bool ParseUint64(const std::string &text, uint64_t &value)
{
    if (text.empty()) {
        return false;
    }
    const char *begin = text.data();
    const char *end = begin + text.size();
    const auto result = std::from_chars(begin, end, value);
    return result.ec == std::errc{} && result.ptr == end;
}

bool ParseKuaiRandTrace(std::istream &input, std::vector<KuaiRandEvent> &events, std::string &error)
{
    events.clear();
    error.clear();
    std::string line;
    size_t lineNumber = 0;
    bool versionSeen = false;
    bool columnsSeen = false;
    uint64_t previousTimestamp = 0;
    while (std::getline(input, line)) {
        ++lineNumber;
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (line.empty()) {
            continue;
        }
        if (line[0] == '#') {
            versionSeen = versionSeen || line == "# kuairand-trace-v1";
            continue;
        }
        if (!columnsSeen) {
            if (line != "time_ms,video_id,engaged") {
                error = FormatString("line %zu: expected time_ms,video_id,engaged", lineNumber);
                return false;
            }
            columnsSeen = true;
            continue;
        }
        const auto fields = Split(line, ",");
        uint64_t timestamp = 0;
        uint64_t videoId = 0;
        uint64_t engaged = 0;
        if (fields.size() != 3 || !ParseUint64(fields[0], timestamp) || !ParseUint64(fields[1], videoId)
            || !ParseUint64(fields[2], engaged) || engaged > 1) {
            error = FormatString("line %zu: invalid KuaiRand event", lineNumber);
            return false;
        }
        if (!events.empty() && timestamp < previousTimestamp) {
            error = FormatString("line %zu: timestamps are not monotonic", lineNumber);
            return false;
        }
        previousTimestamp = timestamp;
        events.push_back({ videoId, engaged != 0 });
    }
    if (!versionSeen || !columnsSeen || events.empty()) {
        error = "missing version, columns, or events";
        return false;
    }
    return true;
}

std::vector<size_t> BuildAccessKeyOrder(const std::vector<size_t> &workerKeyCounts,
                                        AccessKeyPlacement placement)
{
    const size_t keyCount = std::accumulate(workerKeyCounts.begin(), workerKeyCounts.end(), size_t{ 0 });
    std::vector<size_t> order;
    order.reserve(keyCount);
    if (placement == AccessKeyPlacement::CONTIGUOUS) {
        order.resize(keyCount);
        std::iota(order.begin(), order.end(), 0);
        return order;
    }

    std::vector<size_t> workerBase(workerKeyCounts.size(), 0);
    for (size_t workerIdx = 1; workerIdx < workerKeyCounts.size(); ++workerIdx) {
        workerBase[workerIdx] = workerBase[workerIdx - 1] + workerKeyCounts[workerIdx - 1];
    }
    const size_t maxWorkerKeyCount = workerKeyCounts.empty()
                                         ? 0
                                         : *std::max_element(workerKeyCounts.begin(), workerKeyCounts.end());
    for (size_t localIdx = 0; localIdx < maxWorkerKeyCount; ++localIdx) {
        for (size_t workerIdx = 0; workerIdx < workerKeyCounts.size(); ++workerIdx) {
            if (localIdx < workerKeyCounts[workerIdx]) {
                order.push_back(workerBase[workerIdx] + localIdx);
            }
        }
    }
    return order;
}
}  // namespace

struct HitSample {
    int64_t tsMs = 0;
    bool valid = false;
    uint64_t memUsage = 0;
    uint64_t memLimit = 0;
    double memRatio = 0.0;
    uint64_t spillUsage = 0;
    uint64_t spillLimit = 0;
    double spillRatio = 0.0;
    uint64_t hotPrimaryBytes = 0;
    uint64_t primaryBytes = 0;
    uint64_t copyCapacity = 0;
    uint64_t coldPrimaryBytes = 0;
    uint64_t warmPrimaryBytes = 0;
    double hotPrimaryRatio = 0.0;
    double primaryRatio = 0.0;
    double hotWithinPrimaryRatio = 0.0;
    double coldWithinPrimaryRatio = 0.0;
    double warmWithinPrimaryRatio = 0.0;
    bool copyWatermarkValid = false;
    std::array<uint64_t, HIT_SLOT_COUNT> hit{};
};

struct IntervalRate {
    bool valid = false;
    double mem = 0.0;
    double local = 0.0;
    double remote = 0.0;
    double miss = 0.0;
};

struct WorkloadCounters {
    std::atomic<uint64_t> attempts{ 0 };
    std::atomic<uint64_t> successes{ 0 };
    std::atomic<uint64_t> getErrors{ 0 };
    std::atomic<uint64_t> wrongValues{ 0 };
};

struct BlockWorkloadCounters : WorkloadCounters {
    std::atomic<uint64_t> requestAttempts{ 0 };
    std::atomic<uint64_t> requestSuccesses{ 0 };
    std::atomic<uint64_t> requestErrors{ 0 };
    std::atomic<uint64_t> coldWriteAttempts{ 0 };
    std::atomic<uint64_t> coldWriteSuccesses{ 0 };
    std::atomic<uint64_t> coldWriteErrors{ 0 };
    uint64_t targetRequests = 0;
};

struct WorkerKeySet {
    std::vector<std::string> keys;
    std::vector<uint32_t> keySizeBytes;  // per-key object size, parallel to keys
    size_t residentKeyCount = 0;         // keys populated before pressure; only these may be warmed
    size_t hotKeyLimit = 0;              // maximum number of sampled-hot resident keys to warm
    std::vector<size_t> hotKeyIndices;   // actual sampled-hot resident key indices
    std::unordered_set<size_t> oneHitsIndices;  // key indices accessed exactly once, never warmed
};

struct WorkloadAccessPlan {
    std::vector<size_t> repeatingIndices;
    std::vector<size_t> oneHitIndices;
};

struct AggregateTelemetry {
    std::array<uint64_t, HIT_SLOT_COUNT> measureHit{};
    std::vector<uint64_t> baselineMemBytes;
    std::vector<uint64_t> pressureMemMax;
    std::vector<uint64_t> baselinePrimaryBytes;
    std::vector<uint64_t> postPressurePrimaryMax;
    std::vector<uint64_t> baselineSpillBytes;
    std::vector<uint64_t> postPressureSpillMax;
    uint64_t maxConsecutiveZeroGetSamples = 0;
};

// Global key pool: every worker's keys + sizes flattened into one universe so a client can Get any
// key cluster-wide (pseudo-random cross-worker access), not just its own worker's keys. The access
// plan is built over this global pool and shared by all read threads.
struct GlobalKeyPool {
    std::vector<std::string> keys;
    std::vector<uint32_t> keySizeBytes;  // parallel to keys
};

struct BlockRequestGroup {
    size_t ownerWorker = 0;
    std::vector<std::string> keys;
};

struct BlockAccessPlan {
    std::vector<BlockRequestGroup> groups;
    std::vector<size_t> repeatingGroupIndices;
    std::vector<size_t> coldScanGroupIndices;
};

using BlockRequestGroupsByWorker = std::vector<std::vector<BlockRequestGroup>>;

class KVClientWorkloadTelemetryTest : public KVClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        ConfigureCluster(opts);
        if (StrategyFromExtraFlags("eviction_strategy", "clock") == "heat") {
            opts.workerGflagParams.append(" ").append(HeatTestFlags());
        }
        const std::string extraFlags = GetEnv(kExtraWorkerFlagsEnv);
        if (!extraFlags.empty()) {
            opts.workerGflagParams.append(" ").append(extraFlags);
        }
    }

    virtual void ConfigureCluster(ExternalClusterOptions &opts) = 0;
    virtual const char *Topology() const = 0;
    virtual int WorkerNum() const = 0;
    virtual AccessKeyPlacement KeyPlacement() const
    {
        return AccessKeyPlacement::CONTIGUOUS;
    }
    virtual bool RequireCombinedSignals() const
    {
        return false;
    }

    virtual std::string HeatTestFlags() const
    {
        // Short legacy workloads need accelerated decay to make it observable inside a 20-second measurement.
        return "-eviction_heat_half_life_primary_s=15 -eviction_heat_half_life_local_s=15";
    }

    virtual AggregateMemRateContract RequiredAggregateMemRateContract() const
    {
        return {};
    }

    void SetUp() override
    {
        DS_ASSERT_OK(inject::Set("ObjectClientImpl.ClientWorkerWarmup.skip", "call()"));
        ExternalClusterTest::SetUp();
        if (WorkerNum() > 1) {
            // GenerateKeySets reads the topology table to choose keys owned by each worker.
            KVClientCommon::InitTestEtcdInstance();
        }
        for (int i = 0; i < WorkerNum(); ++i) {
            DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, i, "hitinfo.prefix", "call()"));
        }
        for (int i = 0; i < WorkerNum(); ++i) {
            std::shared_ptr<KVClient> client;
            InitTestKVClient(i, client);
            clients_.emplace_back(std::move(client));
        }
    }

    void TearDown() override
    {
        if (csv_.is_open()) {
            csv_.close();
        }
        for (int i = 0; i < WorkerNum(); ++i) {
            (void)cluster_->ClearInjectAction(ClusterNodeType::WORKER, i, "hitinfo.prefix");
        }
        (void)inject::Clear("ObjectClientImpl.ClientWorkerWarmup.skip");
        clients_.clear();
        ExternalClusterTest::TearDown();
    }

protected:
    std::vector<std::shared_ptr<KVClient>> clients_;

    int GetTestCaseTimeoutSecs() const override
    {
        // The base 80-second guard predates this manual workload. A 60-second measurement also needs cluster
        // startup, deterministic warm-up/pressure, and teardown time. Keep 60 seconds of bounded setup margin.
        constexpr size_t kSetupAndTeardownMarginSec = 60;
        constexpr size_t kDefaultStTimeoutSec = 80;
        return std::max(kDefaultStTimeoutSec,
                        static_cast<size_t>(GetPositiveEnvInt(kMeasureSecEnv, kDefaultMeasureSec))
                            + kSetupAndTeardownMarginSec);
    }

    std::string Variant() const
    {
        return SanitizeLabel(GetEnv(kVariantEnv, "default"));
    }

    std::string CommitLabel() const
    {
        return SanitizeLabel(GetEnv(kCommitEnv, "unknown"));
    }

    static std::string StrategyFromExtraFlags(const std::string &flagName, const std::string &defaultValue)
    {
        const std::string flags = GetEnv(kExtraWorkerFlagsEnv);
        const std::string prefix = "-" + flagName + "=";
        const size_t pos = flags.find(prefix);
        if (pos == std::string::npos) {
            return defaultValue;
        }
        const size_t valueBegin = pos + prefix.size();
        const size_t valueEnd = flags.find_first_of(" \t", valueBegin);
        return SanitizeLabel(flags.substr(valueBegin, valueEnd - valueBegin));
    }

    static IntervalRate IntervalRateOf(const HitSample &previous, const HitSample &current)
    {
        IntervalRate rate;
        if (!previous.valid || !current.valid) {
            return rate;
        }
        std::array<uint64_t, HIT_SLOT_COUNT> delta{};
        double total = 0.0;
        for (size_t i = 0; i < HIT_SLOT_COUNT; ++i) {
            delta[i] = current.hit[i] >= previous.hit[i] ? current.hit[i] - previous.hit[i] : 0;
            total += static_cast<double>(delta[i]);
        }
        if (total <= 0.0) {
            return rate;
        }
        rate.valid = true;
        rate.mem = static_cast<double>(delta[MEM]) / total;
        rate.local = static_cast<double>(delta[MEM] + delta[DISK] + delta[L2]) / total;
        rate.remote = static_cast<double>(delta[REMOTE]) / total;
        rate.miss = static_cast<double>(delta[MISS]) / total;
        return rate;
    }

    static std::array<uint64_t, HIT_SLOT_COUNT> HitDelta(const HitSample &previous, const HitSample &current)
    {
        std::array<uint64_t, HIT_SLOT_COUNT> delta{};
        if (!previous.valid || !current.valid) {
            return delta;
        }
        for (size_t i = 0; i < HIT_SLOT_COUNT; ++i) {
            delta[i] = current.hit[i] >= previous.hit[i] ? current.hit[i] - previous.hit[i] : 0;
        }
        return delta;
    }

    bool ReadResourceMetrics(int workerIdx, std::vector<std::string> &metrics)
    {
        const std::string path = FormatString("%s/worker%d/log/resource.log", cluster_->GetRootDir(), workerIdx);
        std::ifstream input(path);
        if (!input.is_open()) {
            return false;
        }
        std::string line;
        std::string lastLine;
        while (std::getline(input, line)) {
            if (!line.empty()) {
                lastLine = std::move(line);
            }
        }
        if (lastLine.empty()) {
            return false;
        }
        metrics = Split(lastLine, " | ");
        constexpr size_t kLogPrefixFieldCount = 7;
        if (metrics.size() <= kLogPrefixFieldCount) {
            return false;
        }
        metrics.erase(metrics.begin(), metrics.begin() + kLogPrefixFieldCount);
        return true;
    }

    static std::string GetMetric(const std::vector<std::string> &metrics, ResMetricName metricName)
    {
        const int index = static_cast<int>(metricName) - static_cast<int>(ResMetricName::SHARED_MEMORY);
        if (index < 0 || index >= static_cast<int>(metrics.size())) {
            return "";
        }
        return metrics[index];
    }

    HitSample ReadSample(int workerIdx)
    {
        HitSample sample;
        sample.tsMs = NowMs();
        std::vector<std::string> metrics;
        if (!ReadResourceMetrics(workerIdx, metrics)) {
            return sample;
        }

        const auto sharedMemory = Split(GetMetric(metrics, ResMetricName::SHARED_MEMORY), "/");
        if (sharedMemory.size() < 4) {
            return sample;
        }
        sample.memUsage = ToU64(sharedMemory[0]);
        sample.memLimit = ToU64(sharedMemory[2]);
        sample.memRatio = ToDouble(sharedMemory[3]);

        const auto spill = Split(GetMetric(metrics, ResMetricName::SPILL_HARD_DISK), "/");
        if (spill.size() >= 4) {
            sample.spillUsage = ToU64(spill[0]);
            sample.spillLimit = ToU64(spill[2]);
            sample.spillRatio = ToDouble(spill[3]);
        }

        const auto copyWatermark = Split(GetMetric(metrics, ResMetricName::OBJECT_COPY_WATERMARK), "/");
        if (copyWatermark.size() >= 7) {
            sample.hotPrimaryBytes = ToU64(copyWatermark[0]);
            sample.primaryBytes = ToU64(copyWatermark[1]);
            sample.copyCapacity = ToU64(copyWatermark[2]);
            sample.hotPrimaryRatio = ToDouble(copyWatermark[3]);
            sample.primaryRatio = ToDouble(copyWatermark[4]);
            sample.hotWithinPrimaryRatio = ToDouble(copyWatermark[5]);
            sample.copyWatermarkValid = ToU64(copyWatermark[6]) != 0;
            if (copyWatermark.size() >= 11) {
                sample.coldPrimaryBytes = ToU64(copyWatermark[7]);
                sample.warmPrimaryBytes = ToU64(copyWatermark[8]);
                sample.coldWithinPrimaryRatio = ToDouble(copyWatermark[9]);
                sample.warmWithinPrimaryRatio = ToDouble(copyWatermark[10]);
            }
        }

        std::string hitInfo = GetMetric(metrics, ResMetricName::OC_HIT_NUM);
        constexpr char kHitInfoPrefix[] = "hit_info:";
        if (hitInfo.rfind(kHitInfoPrefix, 0) == 0) {
            hitInfo.erase(0, sizeof(kHitInfoPrefix) - 1);
        }
        const auto hitFields = Split(hitInfo, "/");
        if (hitFields.size() < HIT_SLOT_COUNT) {
            return sample;
        }
        for (size_t i = 0; i < HIT_SLOT_COUNT; ++i) {
            sample.hit[i] = ToU64(hitFields[i]);
        }
        sample.valid = true;
        return sample;
    }

    void OpenCsv(const WorkloadProfile &profile)
    {
        const std::string fileName = FormatString("telemetry_%s_%s_%s.csv", Topology(), profile.name, Variant());
        const std::string outputDir = GetEnv(kOutputDirEnv);
        std::string path;
        if (outputDir.empty()) {
            path = FormatString("%s/%s", cluster_->GetRootDir(), fileName);
        } else {
            std::error_code error;
            std::filesystem::create_directories(outputDir, error);
            ASSERT_FALSE(error) << "failed to create telemetry output directory " << outputDir << ": "
                                << error.message();
            path = (std::filesystem::path(outputDir) / fileName).string();
        }
        csv_.open(path);
        ASSERT_TRUE(csv_.is_open()) << path;
        csv_ << "ts,worker,variant,workload,topology,phase,memUsage,memLimit,memRatio,"
                "spillUsage,spillLimit,spillRatio,deltaMem,deltaDisk,deltaL2,deltaRemote,deltaMiss,"
                "intervalMemRate,intervalLocalRate,intervalRemoteRate,intervalMissRate,"
                "attemptedGets,successfulGets,getErrors,wrongValues,commit,evictionStrategy,rebalanceStrategy,"
                "hotPrimaryBytes,primaryBytes,copyCapacity,hotPrimaryRatio,primaryRatio,"
                "hotWithinPrimaryRatio,copyWatermarkValid,traceSource,traceEvents,"
                "coldPrimaryBytes,warmPrimaryBytes,coldWithinPrimaryRatio,warmWithinPrimaryRatio\n";
        csv_.flush();
    }

    void WriteCsvRow(int workerIdx, const WorkloadProfile &profile, WorkloadPhase phase, const HitSample &current,
                     const HitSample &previous, const WorkloadCounters &counters)
    {
        const IntervalRate rate = IntervalRateOf(previous, current);
        const auto delta = HitDelta(previous, current);
        std::lock_guard<std::mutex> lock(csvMutex_);
        csv_ << current.tsMs << ',' << workerIdx << ',' << Variant() << ',' << profile.name << ',' << Topology() << ','
             << PhaseName(phase) << ',' << current.memUsage << ',' << current.memLimit << ',' << current.memRatio << ','
             << current.spillUsage << ',' << current.spillLimit << ',' << current.spillRatio << ',' << delta[MEM] << ','
             << delta[DISK] << ',' << delta[L2] << ',' << delta[REMOTE] << ',' << delta[MISS] << ',' << rate.mem << ','
             << rate.local << ',' << rate.remote << ',' << rate.miss << ','
             << counters.attempts.load(std::memory_order_relaxed) << ','
             << counters.successes.load(std::memory_order_relaxed) << ','
             << counters.getErrors.load(std::memory_order_relaxed) << ','
             << counters.wrongValues.load(std::memory_order_relaxed) << ',' << CommitLabel() << ','
             << StrategyFromExtraFlags("eviction_strategy", "clock") << ','
             << StrategyFromExtraFlags("rebalance_strategy", "memory") << ',' << current.hotPrimaryBytes << ','
             << current.primaryBytes << ',' << current.copyCapacity << ',' << current.hotPrimaryRatio << ','
             << current.primaryRatio << ',' << current.hotWithinPrimaryRatio << ','
             << (current.copyWatermarkValid ? 1 : 0) << ',' << traceSource_ << ',' << traceEvents_ << ','
             << current.coldPrimaryBytes << ',' << current.warmPrimaryBytes << ','
             << current.coldWithinPrimaryRatio << ',' << current.warmWithinPrimaryRatio << '\n';
        if (current.copyWatermarkValid) {
            ++validCopyWatermarkRows_;
            positivePrimaryRows_ += current.primaryBytes > 0 ? 1 : 0;
            positiveHotPrimaryRows_ += current.hotPrimaryBytes > 0 ? 1 : 0;
            const bool invalidBytes = current.hotPrimaryBytes > current.primaryBytes
                                      || current.primaryBytes > current.copyCapacity
                                      || current.coldPrimaryBytes + current.warmPrimaryBytes
                                                 + current.hotPrimaryBytes
                                             != current.primaryBytes;
            invalidCopyWatermarkRows_ += invalidBytes ? 1 : 0;
        }
        csv_.flush();
        ++csvRows_;
    }

    void SamplerLoop(std::atomic<bool> &stop, std::atomic<WorkloadPhase> &phase, const WorkloadProfile &profile,
                     const WorkloadCounters &counters)
    {
        std::vector<HitSample> previous(static_cast<size_t>(WorkerNum()));
        std::vector<bool> measureAnchorSeen(static_cast<size_t>(WorkerNum()), false);
        bool attemptAnchorSeen = false;
        uint64_t previousAttempts = 0;
        uint64_t consecutiveZeroGetSamples = 0;
        while (!stop.load(std::memory_order_acquire)) {
            const WorkloadPhase currentPhase = phase.load(std::memory_order_acquire);
            for (int workerIdx = 0; workerIdx < WorkerNum(); ++workerIdx) {
                const HitSample current = ReadSample(workerIdx);
                if (!current.valid) {
                    continue;
                }
                const IntervalRate rate = IntervalRateOf(previous[workerIdx], current);
                const size_t worker = static_cast<size_t>(workerIdx);
                if (currentPhase == WorkloadPhase::BASELINE) {
                    aggregateTelemetry_.baselineMemBytes[worker] = current.memUsage;
                    if (current.copyWatermarkValid) {
                        aggregateTelemetry_.baselinePrimaryBytes[worker] = current.primaryBytes;
                    }
                    aggregateTelemetry_.baselineSpillBytes[worker] = current.spillUsage;
                } else {
                    if (currentPhase == WorkloadPhase::PRESSURE) {
                        aggregateTelemetry_.pressureMemMax[worker] =
                            std::max(aggregateTelemetry_.pressureMemMax[worker], current.memUsage);
                    }
                    if (current.copyWatermarkValid) {
                        aggregateTelemetry_.postPressurePrimaryMax[worker] =
                            std::max(aggregateTelemetry_.postPressurePrimaryMax[worker], current.primaryBytes);
                    }
                    aggregateTelemetry_.postPressureSpillMax[worker] =
                        std::max(aggregateTelemetry_.postPressureSpillMax[worker], current.spillUsage);
                }
                if (currentPhase == WorkloadPhase::MEASURE) {
                    if (measureAnchorSeen[worker]) {
                        const auto delta = HitDelta(previous[worker], current);
                        for (size_t slot = 0; slot < HIT_SLOT_COUNT; ++slot) {
                            aggregateTelemetry_.measureHit[slot] += delta[slot];
                        }
                    } else {
                        measureAnchorSeen[worker] = true;
                    }
                }
                WriteCsvRow(workerIdx, profile, currentPhase, current, previous[workerIdx], counters);
                std::cout << "[hit_sample] variant=" << Variant() << " workload=" << profile.name
                          << " topology=" << Topology() << " phase=" << PhaseName(currentPhase)
                          << " worker=" << workerIdx << " memRatio=" << current.memRatio
                          << " spillRatio=" << current.spillRatio << " intMemRate=" << rate.mem
                          << " hotPrimaryRatio=" << current.hotPrimaryRatio
                          << " primaryRatio=" << current.primaryRatio
                          << " intLocalRate=" << rate.local << " intRemoteRate=" << rate.remote
                          << " intMissRate=" << rate.miss << std::endl;
                previous[workerIdx] = current;
            }
            if (currentPhase == WorkloadPhase::MEASURE) {
                const uint64_t attempts = counters.attempts.load(std::memory_order_relaxed);
                if (attemptAnchorSeen) {
                    if (attempts == previousAttempts) {
                        ++consecutiveZeroGetSamples;
                        aggregateTelemetry_.maxConsecutiveZeroGetSamples =
                            std::max(aggregateTelemetry_.maxConsecutiveZeroGetSamples, consecutiveZeroGetSamples);
                    } else {
                        consecutiveZeroGetSamples = 0;
                    }
                } else {
                    attemptAnchorSeen = true;
                }
                previousAttempts = attempts;
            }
            std::this_thread::sleep_for(std::chrono::seconds(kLogIntervalSec));
        }
    }

    Status RetrySet(const std::shared_ptr<KVClient> &client, const std::string &key, const std::string &value,
                    const SetParam &param)
    {
        Status status;
        for (int retry = 0; retry < kSetRetryCount; ++retry) {
            status = client->Set(key, value, param);
            if (status.IsOk()) {
                return status;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(kSetRetryIntervalMs));
        }
        return status;
    }

    bool WaitHashRingReady()
    {
        // Hash ring is no longer published as a single etcd key on master (topology engine replaced it).
        // Fall back to the cluster-level readiness check, which waits for workers to join and reach ready state.
        return cluster_->WaitUntilClusterReadyOrTimeout(kHashRingReadyTimeoutSec).IsOk();
    }

    // Assign a fixed, reproducible object size to every key from kSizeBuckets (discrete distribution).
    // Same seed for baseline and heat => identical per-key sizes across variants.
    static void AssignKeySizes(WorkerKeySet &keySet)
    {
        std::vector<double> probs;
        probs.reserve(kSizeBuckets.size());
        for (const auto &b : kSizeBuckets) {
            probs.push_back(b.prob);
        }
        std::mt19937_64 gen(kWorkloadSeed);
        std::discrete_distribution<size_t> dist(probs.begin(), probs.end());
        keySet.keySizeBytes.clear();
        keySet.keySizeBytes.reserve(keySet.keys.size());
        for (size_t i = 0; i < keySet.keys.size(); ++i) {
            keySet.keySizeBytes.push_back(kSizeBuckets[dist(gen)].bytes);
        }
    }

    std::vector<WorkerKeySet> GenerateKeySets(const std::vector<size_t> &counts,
                                              const std::vector<size_t> &residentKeyCounts,
                                              const std::vector<size_t> &hotKeyLimits)
    {
        EXPECT_EQ(counts.size(), residentKeyCounts.size());
        EXPECT_EQ(counts.size(), hotKeyLimits.size());
        EXPECT_EQ(counts.size(), static_cast<size_t>(WorkerNum()));
        if (WorkerNum() == 1) {
            WorkerKeySet keySet;
            keySet.keys.reserve(counts[0]);
            for (size_t keyIdx = 0; keyIdx < counts[0]; ++keyIdx) {
                keySet.keys.emplace_back("telemetry_single_worker_" + std::to_string(keyIdx));
            }
            keySet.residentKeyCount = std::min(residentKeyCounts[0], keySet.keys.size());
            keySet.hotKeyLimit = std::min(hotKeyLimits[0], keySet.residentKeyCount);
            AssignKeySizes(keySet);
            return { std::move(keySet) };
        }
        EXPECT_TRUE(WaitHashRingReady()) << "hash ring was not published in etcd";

        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(kHashRingReadyTimeoutSec);
        while (true) {
            std::vector<WorkerKeySet> result;
            bool enough = true;
            for (size_t workerIdx = 0; workerIdx < counts.size(); ++workerIdx) {
                WorkerKeySet keySet;
                GetObjectKeysHashToWorker(db_.get(), static_cast<uint32_t>(workerIdx), counts[workerIdx], keySet.keys);
                keySet.residentKeyCount = std::min(residentKeyCounts[workerIdx], keySet.keys.size());
                keySet.hotKeyLimit = std::min(hotKeyLimits[workerIdx], keySet.residentKeyCount);
                AssignKeySizes(keySet);
                enough = enough && keySet.keys.size() >= counts[workerIdx];
                result.emplace_back(std::move(keySet));
            }
            if (enough || std::chrono::steady_clock::now() >= deadline) {
                for (size_t workerIdx = 0; workerIdx < result.size(); ++workerIdx) {
                    EXPECT_GE(result[workerIdx].keys.size(), counts[workerIdx])
                        << "worker " << workerIdx << " did not receive enough hash-ring-owned keys";
                }
                return result;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
    }

    void PopulateRange(size_t workerIdx, const WorkerKeySet &keySet, size_t begin, size_t end,
                       char fillChar, const SetParam &param)
    {
        ASSERT_LT(workerIdx, clients_.size());
        ASSERT_LE(begin, end);
        ASSERT_LE(end, keySet.keys.size());
        ASSERT_EQ(keySet.keySizeBytes.size(), keySet.keys.size());
        for (size_t keyIdx = begin; keyIdx < end; ++keyIdx) {
            const std::string value(static_cast<size_t>(keySet.keySizeBytes[keyIdx]), fillChar);
            const Status status = RetrySet(clients_[workerIdx], keySet.keys[keyIdx], value, param);
            ASSERT_TRUE(status.IsOk()) << "Set failed for " << keySet.keys[keyIdx] << ": " << status.ToString();
        }
    }

    void WarmHotKeys(const std::vector<WorkerKeySet> &keySets, char fillChar)
    {
        constexpr size_t kWarmupHitsPerKey = 6;
        for (size_t workerIdx = 0; workerIdx < keySets.size(); ++workerIdx) {
            const auto &keySet = keySets[workerIdx];
            ASSERT_EQ(keySet.keySizeBytes.size(), keySet.keys.size());
            for (size_t round = 0; round < kWarmupHitsPerKey; ++round) {
                for (const size_t keyIdx : keySet.hotKeyIndices) {
                    ASSERT_LT(keyIdx, keySet.residentKeyCount);
                    std::string output;
                    const Status status = clients_[workerIdx]->Get(keySet.keys[keyIdx], output, kGetTimeoutMs);
                    ASSERT_TRUE(status.IsOk()) << status.ToString();
                    ASSERT_EQ(output, std::string(static_cast<size_t>(keySet.keySizeBytes[keyIdx]), fillChar));
                }
            }
        }
    }

    // Build the global key pool by flattening every worker's keys + sizes. The access plan is
    // sampled over this universe so a client Gets keys cluster-wide (pseudo-random cross-worker),
    // not just its own worker's keys.
    static GlobalKeyPool BuildGlobalKeyPool(const std::vector<WorkerKeySet> &keySets)
    {
        GlobalKeyPool pool;
        for (const auto &ks : keySets) {
            pool.keys.insert(pool.keys.end(), ks.keys.begin(), ks.keys.end());
            pool.keySizeBytes.insert(pool.keySizeBytes.end(), ks.keySizeBytes.begin(), ks.keySizeBytes.end());
        }
        return pool;
    }

    static std::vector<size_t> SampleGlobalKeys(const GlobalKeyPool &pool, const WorkloadProfile &profile,
                                                const std::vector<size_t> &keyOrder,
                                                std::vector<uint64_t> &sampleCount)
    {
        std::vector<size_t> sampledPlan;
        const size_t keyCount = pool.keys.size();
        const size_t bucketCount = profile.popularityWeights.size();
        if (keyCount == 0 || bucketCount == 0 || keyCount < bucketCount) {
            return sampledPlan;
        }
        std::mt19937_64 gen(kWorkloadSeed);
        std::discrete_distribution<size_t> dist(profile.popularityWeights.begin(),
                                                profile.popularityWeights.end());
        const size_t keysPerBucket = keyCount / bucketCount;
        sampledPlan.reserve(kAccessPlanSize);
        sampleCount.assign(keyCount, 0);
        for (int i = 0; i < kAccessPlanSize; ++i) {
            const size_t bucket = dist(gen);
            const size_t bucketStart = bucket * keysPerBucket;
            const size_t bucketEnd = (bucket + 1 == bucketCount) ? keyCount : bucketStart + keysPerBucket;
            std::uniform_int_distribution<size_t> withinBucket(bucketStart, bucketEnd - 1);
            const size_t logicalIdx = withinBucket(gen);
            const size_t idx = keyOrder[logicalIdx];
            sampledPlan.push_back(idx);
            ++sampleCount[idx];
        }
        return sampledPlan;
    }

    static std::vector<size_t> RankWorkerKeys(size_t localCount, size_t globalBase,
                                              const std::vector<uint64_t> &sampleCount)
    {
        std::vector<size_t> order(localCount);
        std::iota(order.begin(), order.end(), 0);
        std::sort(order.begin(), order.end(), [&](size_t a, size_t b) {
            if (sampleCount[globalBase + a] != sampleCount[globalBase + b]) {
                return sampleCount[globalBase + a] > sampleCount[globalBase + b];
            }
            return a < b;
        });
        return order;
    }

    static void SelectWorkerKeyRolesFromTrace(WorkerKeySet &keySet, size_t globalBase,
                                              const std::vector<uint64_t> &sampleCount,
                                              std::vector<bool> &isOneHit, WorkloadAccessPlan &accessPlan)
    {
        const auto order = RankWorkerKeys(keySet.keys.size(), globalBase, sampleCount);
        keySet.hotKeyIndices.clear();
        keySet.oneHitsIndices.clear();
        for (const size_t idx : order) {
            if (sampleCount[globalBase + idx] == 0) {
                break;
            }
            if (idx < keySet.residentKeyCount && keySet.hotKeyIndices.size() < keySet.hotKeyLimit) {
                keySet.hotKeyIndices.push_back(idx);
            }
        }
        const std::unordered_set<size_t> hotIndices(keySet.hotKeyIndices.begin(), keySet.hotKeyIndices.end());
        for (const size_t idx : order) {
            if (sampleCount[globalBase + idx] != 1 || hotIndices.find(idx) != hotIndices.end()) {
                continue;
            }
            keySet.oneHitsIndices.insert(idx);
            isOneHit[globalBase + idx] = true;
            accessPlan.oneHitIndices.push_back(globalBase + idx);
        }
    }

    static void SelectWorkerKeyRoles(WorkerKeySet &keySet, size_t globalBase, const WorkloadProfile &profile,
                                     const std::vector<uint64_t> &sampleCount, std::vector<bool> &isOneHit,
                                     WorkloadAccessPlan &accessPlan)
    {
        const auto order = RankWorkerKeys(keySet.keys.size(), globalBase, sampleCount);
        keySet.hotKeyIndices.clear();
        keySet.oneHitsIndices.clear();
        for (const size_t idx : order) {
            if (idx < keySet.residentKeyCount && keySet.hotKeyIndices.size() < keySet.hotKeyLimit) {
                keySet.hotKeyIndices.push_back(idx);
            }
        }
        const std::unordered_set<size_t> hotIndices(keySet.hotKeyIndices.begin(), keySet.hotKeyIndices.end());
        const size_t oneHitsCap =
            static_cast<size_t>(std::ceil(static_cast<double>(keySet.keys.size()) * profile.oneHitsRatio));
        for (auto iter = order.rbegin(); iter != order.rend() && keySet.oneHitsIndices.size() < oneHitsCap; ++iter) {
            const size_t idx = *iter;
            if (hotIndices.find(idx) == hotIndices.end()) {
                keySet.oneHitsIndices.insert(idx);
                isOneHit[globalBase + idx] = true;
                accessPlan.oneHitIndices.push_back(globalBase + idx);
            }
        }
    }

    WorkloadAccessPlan BuildKuaiRandAccessPlan(const GlobalKeyPool &pool, const WorkloadProfile &profile,
                                              std::vector<WorkerKeySet> &keySets,
                                              const std::vector<size_t> &keyOrder, const std::string &tracePath)
    {
        std::ifstream input(tracePath);
        std::vector<KuaiRandEvent> events;
        std::string error;
        if (!input.is_open() || !ParseKuaiRandTrace(input, events, error)) {
            ADD_FAILURE() << "failed to load DS_TLM_KUAIRAND_TRACE=" << tracePath << ": "
                          << (input.is_open() ? error : "file is not readable");
            return {};
        }

        const bool engagedOnly = std::string(profile.name) == kFineRankProfile.name;
        std::unordered_map<uint64_t, uint64_t> videoCounts;
        videoCounts.reserve(events.size());
        for (const auto &event : events) {
            if (!engagedOnly || event.engaged) {
                ++videoCounts[event.videoId];
            }
        }
        std::vector<std::pair<uint64_t, uint64_t>> rankedVideos(videoCounts.begin(), videoCounts.end());
        std::sort(rankedVideos.begin(), rankedVideos.end(), [](const auto &a, const auto &b) {
            return a.second != b.second ? a.second > b.second : a.first < b.first;
        });
        if (rankedVideos.size() > pool.keys.size()) {
            rankedVideos.resize(pool.keys.size());
        }

        std::unordered_map<uint64_t, size_t> videoToKey;
        videoToKey.reserve(rankedVideos.size());
        std::vector<uint64_t> sampleCount(pool.keys.size(), 0);
        for (size_t idx = 0; idx < rankedVideos.size(); ++idx) {
            const size_t keyIdx = keyOrder[idx];
            videoToKey.emplace(rankedVideos[idx].first, keyIdx);
            sampleCount[keyIdx] = rankedVideos[idx].second;
        }

        WorkloadAccessPlan accessPlan;
        std::vector<size_t> sampledPlan;
        sampledPlan.reserve(events.size());
        for (const auto &event : events) {
            if (engagedOnly && !event.engaged) {
                continue;
            }
            const auto iter = videoToKey.find(event.videoId);
            if (iter != videoToKey.end()) {
                sampledPlan.push_back(iter->second);
            }
        }

        std::vector<bool> isOneHit(pool.keys.size(), false);
        size_t globalBase = 0;
        for (auto &keySet : keySets) {
            SelectWorkerKeyRolesFromTrace(keySet, globalBase, sampleCount, isOneHit, accessPlan);
            globalBase += keySet.keys.size();
        }
        accessPlan.repeatingIndices.reserve(sampledPlan.size());
        for (const size_t idx : sampledPlan) {
            if (!isOneHit[idx]) {
                accessPlan.repeatingIndices.push_back(idx);
            }
        }
        traceSource_ = engagedOnly ? "kuairand_engaged" : "kuairand_all";
        traceEvents_ = sampledPlan.size();
        std::cout << "[workload_trace] source=" << traceSource_ << " inputEvents=" << events.size()
                  << " selectedEvents=" << traceEvents_ << " selectedVideos=" << rankedVideos.size() << std::endl;
        return accessPlan;
    }

    // Build the deterministic plan before warm-up. With DS_TLM_KUAIRAND_TRACE, real KuaiRand event order and
    // popularity select the hottest videos that fit this test's fixed key universe. Fine-rank uses explicit positive
    // feedback (click/like/follow/comment/forward/long-view) while recall replays every exposure. Without the env var,
    // retain the existing synthetic distribution so ordinary CI does not depend on an external dataset.
    WorkloadAccessPlan BuildGlobalAccessPlan(const GlobalKeyPool &pool, const WorkloadProfile &profile,
                                             std::vector<WorkerKeySet> &keySets)
    {
        std::vector<size_t> workerKeyCounts;
        workerKeyCounts.reserve(keySets.size());
        for (const auto &keySet : keySets) {
            workerKeyCounts.push_back(keySet.keys.size());
        }
        const auto keyOrder = BuildAccessKeyOrder(workerKeyCounts, KeyPlacement());
        if (keyOrder.size() != pool.keys.size()) {
            ADD_FAILURE() << "access-key order size " << keyOrder.size() << " differs from key pool size "
                          << pool.keys.size();
            return {};
        }
        std::vector<bool> keySeen(pool.keys.size(), false);
        for (const size_t idx : keyOrder) {
            if (idx >= pool.keys.size() || keySeen[idx]) {
                ADD_FAILURE() << "access-key order contains invalid or duplicate index " << idx;
                return {};
            }
            keySeen[idx] = true;
        }
        const std::string tracePath = GetEnv(kKuaiRandTraceEnv);
        if (!tracePath.empty()) {
            auto accessPlan = BuildKuaiRandAccessPlan(pool, profile, keySets, keyOrder, tracePath);
            ValidateAccessPlanDistribution(keySets, accessPlan);
            return accessPlan;
        }
        traceSource_ = "synthetic";
        traceEvents_ = kAccessPlanSize;
        WorkloadAccessPlan accessPlan;
        std::vector<uint64_t> sampleCount;
        const auto sampledPlan = SampleGlobalKeys(pool, profile, keyOrder, sampleCount);
        if (sampledPlan.empty()) {
            return accessPlan;
        }
        std::vector<bool> isOneHit(pool.keys.size(), false);
        size_t globalBase = 0;
        for (auto &keySet : keySets) {
            SelectWorkerKeyRoles(keySet, globalBase, profile, sampleCount, isOneHit, accessPlan);
            globalBase += keySet.keys.size();
        }
        accessPlan.repeatingIndices.reserve(sampledPlan.size());
        for (const size_t idx : sampledPlan) {
            if (!isOneHit[idx]) {
                accessPlan.repeatingIndices.push_back(idx);
            }
        }
        ValidateAccessPlanDistribution(keySets, accessPlan);
        return accessPlan;
    }

    void ValidateAccessPlanDistribution(const std::vector<WorkerKeySet> &keySets,
                                        const WorkloadAccessPlan &accessPlan) const
    {
        if (KeyPlacement() != AccessKeyPlacement::WORKER_ROUND_ROBIN) {
            return;
        }
        std::vector<size_t> workerBase(keySets.size() + 1, 0);
        for (size_t workerIdx = 0; workerIdx < keySets.size(); ++workerIdx) {
            workerBase[workerIdx + 1] = workerBase[workerIdx] + keySets[workerIdx].keys.size();
        }
        std::vector<uint64_t> requestCount(keySets.size(), 0);
        const auto countRequest = [&](size_t keyIdx) {
            const auto upper = std::upper_bound(workerBase.begin(), workerBase.end(), keyIdx);
            ASSERT_NE(upper, workerBase.begin());
            const size_t workerIdx = static_cast<size_t>(std::distance(workerBase.begin(), upper) - 1);
            ASSERT_LT(workerIdx, requestCount.size());
            ++requestCount[workerIdx];
        };
        for (const size_t keyIdx : accessPlan.repeatingIndices) {
            countRequest(keyIdx);
        }
        for (const size_t keyIdx : accessPlan.oneHitIndices) {
            countRequest(keyIdx);
        }
        for (size_t workerIdx = 0; workerIdx < requestCount.size(); ++workerIdx) {
            EXPECT_GT(requestCount[workerIdx], 0UL) << "worker " << workerIdx << " received no workload requests";
        }
    }

    static size_t ExpectedOneHitCount(const std::vector<WorkerKeySet> &keySets)
    {
        size_t expected = 0;
        for (const auto &keySet : keySets) {
            EXPECT_EQ(keySet.hotKeyIndices.size(), keySet.hotKeyLimit)
                << "the sampled hot set must fill the configured resident-key limit";
            expected += keySet.oneHitsIndices.size();
        }
        return expected;
    }

    BlockAccessPlan BuildBlockAccessPlan(std::vector<WorkerKeySet> &keySets, const WorkloadProfile &profile,
                                         const BlockWorkloadConfig &config)
    {
        BlockAccessPlan accessPlan;
        std::vector<size_t> workerGroupCounts;
        workerGroupCounts.reserve(keySets.size());
        for (size_t workerIdx = 0; workerIdx < keySets.size(); ++workerIdx) {
            auto &keySet = keySets[workerIdx];
            if (keySet.keys.size() % config.blocksPerRequest != 0
                || keySet.residentKeyCount % config.blocksPerRequest != 0) {
                ADD_FAILURE() << "worker " << workerIdx << " block keys are not request aligned";
                return {};
            }
            const size_t groupCount = keySet.keys.size() / config.blocksPerRequest;
            workerGroupCounts.push_back(groupCount);
            for (size_t groupIdx = 0; groupIdx < groupCount; ++groupIdx) {
                BlockRequestGroup group;
                group.ownerWorker = workerIdx;
                const auto begin = keySet.keys.begin() + static_cast<ptrdiff_t>(groupIdx * config.blocksPerRequest);
                group.keys.assign(begin, begin + static_cast<ptrdiff_t>(config.blocksPerRequest));
                accessPlan.groups.emplace_back(std::move(group));
            }
        }
        const auto groupOrder = BuildAccessKeyOrder(workerGroupCounts, KeyPlacement());
        if (groupOrder.size() != accessPlan.groups.size() || accessPlan.groups.empty()) {
            ADD_FAILURE() << "invalid logical request group order";
            return {};
        }

        std::vector<uint64_t> sampleCount(accessPlan.groups.size(), 0);
        const std::string tracePath = GetEnv(kKuaiRandTraceEnv);
        if (!tracePath.empty()) {
            std::ifstream input(tracePath);
            std::vector<KuaiRandEvent> events;
            std::string error;
            if (!input.is_open() || !ParseKuaiRandTrace(input, events, error)) {
                ADD_FAILURE() << "failed to load DS_TLM_KUAIRAND_TRACE=" << tracePath << ": "
                              << (input.is_open() ? error : "file is not readable");
                return {};
            }
            std::unordered_map<uint64_t, uint64_t> videoCounts;
            videoCounts.reserve(events.size());
            for (const auto &event : events) {
                ++videoCounts[event.videoId];
            }
            std::vector<std::pair<uint64_t, uint64_t>> rankedVideos(videoCounts.begin(), videoCounts.end());
            std::sort(rankedVideos.begin(), rankedVideos.end(), [](const auto &lhs, const auto &rhs) {
                return lhs.second != rhs.second ? lhs.second > rhs.second : lhs.first < rhs.first;
            });
            rankedVideos.resize(std::min(rankedVideos.size(), accessPlan.groups.size()));
            std::unordered_map<uint64_t, size_t> videoToGroup;
            videoToGroup.reserve(rankedVideos.size());
            for (size_t rank = 0; rank < rankedVideos.size(); ++rank) {
                videoToGroup.emplace(rankedVideos[rank].first, groupOrder[rank]);
            }
            accessPlan.repeatingGroupIndices.reserve(events.size());
            for (const auto &event : events) {
                const auto iter = videoToGroup.find(event.videoId);
                if (iter != videoToGroup.end()) {
                    accessPlan.repeatingGroupIndices.push_back(iter->second);
                    ++sampleCount[iter->second];
                }
            }
            traceSource_ = "kuairand_all";
            traceEvents_ = accessPlan.repeatingGroupIndices.size();
        } else {
            const size_t bucketCount = profile.popularityWeights.size();
            if (bucketCount == 0 || accessPlan.groups.size() < bucketCount) {
                ADD_FAILURE() << "logical request group count is smaller than the popularity bucket count";
                return {};
            }
            std::mt19937_64 gen(kWorkloadSeed);
            std::discrete_distribution<size_t> bucketDist(profile.popularityWeights.begin(),
                                                          profile.popularityWeights.end());
            const size_t groupsPerBucket = accessPlan.groups.size() / bucketCount;
            accessPlan.repeatingGroupIndices.reserve(kAccessPlanSize);
            for (int request = 0; request < kAccessPlanSize; ++request) {
                const size_t bucket = bucketDist(gen);
                const size_t bucketBegin = bucket * groupsPerBucket;
                const size_t bucketEnd = bucket + 1 == bucketCount ? accessPlan.groups.size()
                                                                  : bucketBegin + groupsPerBucket;
                std::uniform_int_distribution<size_t> groupDist(bucketBegin, bucketEnd - 1);
                const size_t groupIdx = groupOrder[groupDist(gen)];
                accessPlan.repeatingGroupIndices.push_back(groupIdx);
                ++sampleCount[groupIdx];
            }
            traceSource_ = "synthetic_block_recall";
            traceEvents_ = accessPlan.repeatingGroupIndices.size();
        }

        accessPlan.coldScanGroupIndices.resize(accessPlan.groups.size());
        std::iota(accessPlan.coldScanGroupIndices.begin(), accessPlan.coldScanGroupIndices.end(), 0);
        std::sort(accessPlan.coldScanGroupIndices.begin(), accessPlan.coldScanGroupIndices.end(),
                  [&](size_t lhs, size_t rhs) {
                      return sampleCount[lhs] != sampleCount[rhs] ? sampleCount[lhs] < sampleCount[rhs] : lhs < rhs;
                  });

        size_t globalGroupBase = 0;
        for (size_t workerIdx = 0; workerIdx < keySets.size(); ++workerIdx) {
            auto &keySet = keySets[workerIdx];
            const size_t localGroupCount = workerGroupCounts[workerIdx];
            const size_t residentGroupCount = keySet.residentKeyCount / config.blocksPerRequest;
            const size_t hotGroupLimit = keySet.hotKeyLimit / config.blocksPerRequest;
            std::vector<size_t> localOrder(localGroupCount);
            std::iota(localOrder.begin(), localOrder.end(), 0);
            std::sort(localOrder.begin(), localOrder.end(), [&](size_t lhs, size_t rhs) {
                const uint64_t lhsCount = sampleCount[globalGroupBase + lhs];
                const uint64_t rhsCount = sampleCount[globalGroupBase + rhs];
                return lhsCount != rhsCount ? lhsCount > rhsCount : lhs < rhs;
            });
            keySet.hotKeyIndices.clear();
            size_t hotGroups = 0;
            for (const size_t localGroupIdx : localOrder) {
                if (localGroupIdx >= residentGroupCount || hotGroups >= hotGroupLimit) {
                    continue;
                }
                const size_t firstBlock = localGroupIdx * config.blocksPerRequest;
                for (size_t blockIdx = 0; blockIdx < config.blocksPerRequest; ++blockIdx) {
                    keySet.hotKeyIndices.push_back(firstBlock + blockIdx);
                }
                ++hotGroups;
            }
            globalGroupBase += localGroupCount;
        }

        std::vector<uint64_t> requestsByWorker(keySets.size(), 0);
        for (const size_t groupIdx : accessPlan.repeatingGroupIndices) {
            if (groupIdx >= accessPlan.groups.size()) {
                ADD_FAILURE() << "block access plan contains an invalid group index";
                return {};
            }
            ++requestsByWorker[accessPlan.groups[groupIdx].ownerWorker];
        }
        for (size_t workerIdx = 0; workerIdx < requestsByWorker.size(); ++workerIdx) {
            EXPECT_GT(requestsByWorker[workerIdx], 0UL)
                << "worker " << workerIdx << " received no block workload requests";
        }
        std::cout << "[block_workload_trace] source=" << traceSource_ << " selectedEvents=" << traceEvents_
                  << " logicalKvCaches=" << accessPlan.groups.size()
                  << " blocksPerRequest=" << config.blocksPerRequest << std::endl;
        return accessPlan;
    }

    void ValidateTelemetryResult(const WorkloadProfile &profile, const WorkloadCounters &counters)
    {
        for (int workerIdx = 0; workerIdx < WorkerNum(); ++workerIdx) {
            EXPECT_TRUE(cluster_->CheckWorkerProcess(static_cast<uint32_t>(workerIdx)))
                << "worker " << workerIdx << " exited during telemetry measurement";
        }
        EXPECT_EQ(counters.wrongValues.load(std::memory_order_relaxed), 0UL)
            << "a successful Get returned a value different from the published payload";
        EXPECT_EQ(counters.getErrors.load(std::memory_order_relaxed), 0UL)
            << "Get failed during the measured workload";
        EXPECT_GT(counters.successes.load(std::memory_order_relaxed), 0UL) << "no successful Gets were observed";
        EXPECT_GT(csvRows_, 0UL) << "no telemetry rows were written";
        EXPECT_GT(validCopyWatermarkRows_, 0UL) << "no initialized copy-watermark sample was written";
        EXPECT_GT(positivePrimaryRows_, 0UL) << "no resident primary bytes were observed";
        EXPECT_EQ(invalidCopyWatermarkRows_, 0UL)
            << "copy watermark must satisfy hotPrimaryBytes <= primaryBytes <= copyCapacity";
        if (StrategyFromExtraFlags("eviction_strategy", "clock") == "heat") {
            EXPECT_GT(positiveHotPrimaryRows_, 0UL) << "heat workload did not produce a hot-primary sample";
        }
        const uint64_t aggregateHits =
            std::accumulate(aggregateTelemetry_.measureHit.begin(), aggregateTelemetry_.measureHit.end(), uint64_t{ 0 });
        EXPECT_GT(aggregateHits, 0UL) << "no post-anchor measure hit deltas were observed";
        const double aggregateMemRate = aggregateHits == 0
                                            ? 0.0
                                            : static_cast<double>(aggregateTelemetry_.measureHit[MEM])
                                                  / static_cast<double>(aggregateHits);
        const double aggregateLocalRate = aggregateHits == 0
                                              ? 0.0
                                              : static_cast<double>(aggregateTelemetry_.measureHit[MEM]
                                                                    + aggregateTelemetry_.measureHit[DISK]
                                                                    + aggregateTelemetry_.measureHit[L2])
                                                    / static_cast<double>(aggregateHits);
        const AggregateMemRateContract memRateContract = RequiredAggregateMemRateContract();
        if (memRateContract.enabled) {
            EXPECT_GE(aggregateMemRate, memRateContract.minimum)
                << "measured memory hit rate is below the workload capacity contract";
            EXPECT_LE(aggregateMemRate, memRateContract.maximum)
                << "measured memory hit rate is above the workload capacity contract";
        }
        bool evictionObserved = false;
        bool rebalanceObserved = false;
        for (size_t workerIdx = 0; workerIdx < aggregateTelemetry_.baselineSpillBytes.size(); ++workerIdx) {
            evictionObserved = evictionObserved
                               || aggregateTelemetry_.postPressureSpillMax[workerIdx]
                                      > aggregateTelemetry_.baselineSpillBytes[workerIdx];
            if (workerIdx > 0) {
                rebalanceObserved = rebalanceObserved
                                    || aggregateTelemetry_.postPressurePrimaryMax[workerIdx]
                                           > aggregateTelemetry_.baselinePrimaryBytes[workerIdx]
                                    || aggregateTelemetry_.pressureMemMax[workerIdx]
                                           > aggregateTelemetry_.baselineMemBytes[workerIdx];
            }
        }
        if (RequireCombinedSignals()) {
            EXPECT_TRUE(evictionObserved) << "combined workload did not increase spill usage";
            EXPECT_TRUE(rebalanceObserved) << "combined workload did not migrate primary data to a target worker";
            EXPECT_LT(aggregateTelemetry_.maxConsecutiveZeroGetSamples, 5UL)
                << "combined workload made no Get progress for at least five samples";
        }
        std::cout << "[aggregate_telemetry] variant=" << Variant() << " workload=" << profile.name
                  << " topology=" << Topology() << " deltaMem=" << aggregateTelemetry_.measureHit[MEM]
                  << " deltaDisk=" << aggregateTelemetry_.measureHit[DISK]
                  << " deltaL2=" << aggregateTelemetry_.measureHit[L2]
                  << " deltaRemote=" << aggregateTelemetry_.measureHit[REMOTE]
                  << " deltaMiss=" << aggregateTelemetry_.measureHit[MISS]
                  << " memRate=" << aggregateMemRate << " localRate=" << aggregateLocalRate
                  << " requiredMemRateMin=" << memRateContract.minimum
                  << " requiredMemRateMax=" << memRateContract.maximum
                  << " maxZeroGetSamples=" << aggregateTelemetry_.maxConsecutiveZeroGetSamples
                  << " evictionObserved=" << (evictionObserved ? 1 : 0)
                  << " rebalanceObserved=" << (rebalanceObserved ? 1 : 0) << std::endl;
        std::cout << "[telemetry_summary] variant=" << Variant() << " workload=" << profile.name
                  << " topology=" << Topology() << " attempts=" << counters.attempts.load(std::memory_order_relaxed)
                  << " successes=" << counters.successes.load(std::memory_order_relaxed)
                  << " getErrors=" << counters.getErrors.load(std::memory_order_relaxed)
                  << " wrongValues=" << counters.wrongValues.load(std::memory_order_relaxed)
                  << " csvRows=" << csvRows_ << " traceSource=" << traceSource_
                  << " traceEvents=" << traceEvents_ << std::endl;
    }

    void RunReadWorkload(const GlobalKeyPool &pool, const WorkloadAccessPlan &accessPlan, char fillChar,
                         WorkloadCounters &counters, std::atomic<bool> &stop)
    {
        if (accessPlan.repeatingIndices.empty()) {
            return;
        }
        auto getAndCount = [&](size_t workerIdx, size_t idx) {
            const std::string &key = pool.keys[idx];
            std::string output;
            const Status status = clients_[workerIdx]->Get(key, output, kGetTimeoutMs);
            counters.attempts.fetch_add(1, std::memory_order_relaxed);
            if (status.IsError()) {
                counters.getErrors.fetch_add(1, std::memory_order_relaxed);
                return;
            }
            counters.successes.fetch_add(1, std::memory_order_relaxed);
            if (output != std::string(static_cast<size_t>(pool.keySizeBytes[idx]), fillChar)) {
                counters.wrongValues.fetch_add(1, std::memory_order_relaxed);
            }
        };

        std::vector<std::thread> threads;
        for (size_t workerIdx = 0; workerIdx < clients_.size(); ++workerIdx) {
            threads.emplace_back([&, workerIdx]() {
                for (size_t pos = workerIdx; pos < accessPlan.oneHitIndices.size(); pos += clients_.size()) {
                    getAndCount(workerIdx, accessPlan.oneHitIndices[pos]);
                }
                // Offset each thread's start cursor by a prime stride so threads spread across the
                // shared plan rather than all hitting the same key at the same instant.
                size_t cursor = workerIdx * 7919;
                while (!stop.load(std::memory_order_acquire)) {
                    const size_t idx = accessPlan.repeatingIndices[cursor++ % accessPlan.repeatingIndices.size()];
                    getAndCount(workerIdx, idx);
                }
            });
        }

        std::this_thread::sleep_for(std::chrono::seconds(GetPositiveEnvInt(kMeasureSecEnv, kDefaultMeasureSec)));
        stop.store(true, std::memory_order_release);
        for (auto &thread : threads) {
            thread.join();
        }
    }

    Status WarmBlockAccessPlan(const BlockAccessPlan &accessPlan, const BlockWorkloadConfig &config)
    {
        if (accessPlan.repeatingGroupIndices.empty()) {
            return Status(K_RUNTIME_ERROR, "block access plan is empty");
        }
        const bool ownerAffinity = GetEnvBool(kBlockOwnerAffinityEnv, false);
        for (uint64_t requestIdx = 0; requestIdx < config.warmupRequests; ++requestIdx) {
            // Sample the complete trace rather than the prefix immediately preceding measure. This establishes
            // frequency history without preloading the exact next request range and keeps scan pollution observable.
            const size_t groupIdx = accessPlan.repeatingGroupIndices[static_cast<size_t>(
                (requestIdx * kBlockAccessThreadStride) % accessPlan.repeatingGroupIndices.size())];
            const auto &group = accessPlan.groups[groupIdx];
            const size_t firstClient = ownerAffinity ? group.ownerWorker : 0;
            const size_t clientCount = ownerAffinity ? 1 : clients_.size();
            for (size_t clientOffset = 0; clientOffset < clientCount; ++clientOffset) {
                std::vector<Optional<ReadOnlyBuffer>> buffers;
                const Status status = clients_[firstClient + clientOffset]->Get(group.keys, buffers, kGetTimeoutMs);
                if (status.IsError()) {
                    return status;
                }
                if (buffers.size() != group.keys.size()) {
                    return Status(K_RUNTIME_ERROR, "block warm-up returned an incomplete buffer vector");
                }
                for (const auto &buffer : buffers) {
                    if (!buffer || buffer->GetSize() != static_cast<int64_t>(config.payloadBlockBytes)) {
                        return Status(K_RUNTIME_ERROR, "block warm-up returned an invalid buffer");
                    }
                }
            }
        }
        std::cout << "[block_workload_warmup] requests=" << config.warmupRequests
                  << " clientsPerRequest=" << (ownerAffinity ? 1 : clients_.size())
                  << " blockGets=" << config.warmupRequests * config.blocksPerRequest
                                             * (ownerAffinity ? 1 : clients_.size())
                  << std::endl;
        return Status::OK();
    }

    Status RunBlockColdScan(const BlockAccessPlan &accessPlan, const BlockWorkloadConfig &config)
    {
        const size_t requested =
            static_cast<size_t>(GetNonNegativeEnvInt(kBlockColdScanRequestsEnv, kDefaultBlockColdScanRequests));
        if (requested == 0) {
            return Status::OK();
        }
        const size_t scanCount = std::min(requested, accessPlan.coldScanGroupIndices.size());
        const bool ownerAffinity = GetEnvBool(kBlockOwnerAffinityEnv, false);
        for (size_t pos = 0; pos < scanCount; ++pos) {
            const auto &group = accessPlan.groups[accessPlan.coldScanGroupIndices[pos]];
            const size_t firstClient = ownerAffinity ? group.ownerWorker : 0;
            const size_t clientCount = ownerAffinity ? 1 : clients_.size();
            for (size_t clientOffset = 0; clientOffset < clientCount; ++clientOffset) {
                std::vector<Optional<ReadOnlyBuffer>> buffers;
                RETURN_IF_NOT_OK(clients_[firstClient + clientOffset]->Get(group.keys, buffers, kGetTimeoutMs));
                if (buffers.size() != group.keys.size()) {
                    return Status(K_RUNTIME_ERROR, "block cold scan returned an incomplete buffer vector");
                }
                for (const auto &buffer : buffers) {
                    if (!buffer || buffer->GetSize() != static_cast<int64_t>(config.payloadBlockBytes)) {
                        return Status(K_RUNTIME_ERROR, "block cold scan returned an invalid buffer");
                    }
                }
            }
        }
        std::cout << "[block_workload_cold_scan] requests=" << scanCount
                  << " clientsPerRequest=" << (ownerAffinity ? 1 : clients_.size())
                  << " blockGets=" << scanCount * config.blocksPerRequest * (ownerAffinity ? 1 : clients_.size())
                  << std::endl;
        return Status::OK();
    }

    void RunBlockReadWorkload(const BlockAccessPlan &accessPlan, const BlockRequestGroupsByWorker &coldWriteGroups,
                              const BlockWorkloadConfig &config, char fillChar, BlockWorkloadCounters &counters,
                              std::atomic<bool> &stop)
    {
        if (accessPlan.repeatingGroupIndices.empty()) {
            return;
        }
        const int measureSec = GetPositiveEnvInt(kMeasureSecEnv, kDefaultMeasureSec);
        const auto start = std::chrono::steady_clock::now();
        const auto deadline = start + std::chrono::seconds(measureSec);
        counters.targetRequests = config.targetRequestQps * static_cast<uint64_t>(measureSec);
        std::atomic<uint64_t> nextRequest{ 0 };
        const std::string expectedBlock(static_cast<size_t>(config.payloadBlockBytes), fillChar);
        const bool ownerAffinity = GetEnvBool(kBlockOwnerAffinityEnv, false);
        const uint64_t coldWriteQps =
            static_cast<uint64_t>(GetNonNegativeEnvInt(kBlockColdWriteQpsEnv, kDefaultBlockColdWriteQps));

        std::vector<std::thread> coldWriteThreads;
        coldWriteThreads.reserve(coldWriteGroups.size());
        const std::string coldValue(static_cast<size_t>(config.payloadBlockBytes), fillChar);
        for (size_t workerIdx = 0; workerIdx < coldWriteGroups.size(); ++workerIdx) {
            coldWriteThreads.emplace_back([&, workerIdx]() {
                MSetParam param{ .writeMode = WriteMode::NONE_L2_CACHE_EVICT };
                for (uint64_t requestId = workerIdx, localIdx = 0; requestId < coldWriteQps * measureSec;
                     requestId += coldWriteGroups.size(), ++localIdx) {
                    if (localIdx >= coldWriteGroups[workerIdx].size()) {
                        counters.coldWriteErrors.fetch_add(1, std::memory_order_relaxed);
                        break;
                    }
                    const auto scheduledOffset = std::chrono::duration<double>(
                        static_cast<double>(requestId) / static_cast<double>(coldWriteQps));
                    const auto scheduledAt = start
                                             + std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                                                 scheduledOffset);
                    if (scheduledAt > std::chrono::steady_clock::now()) {
                        std::this_thread::sleep_until(scheduledAt);
                    }
                    if (stop.load(std::memory_order_acquire) || std::chrono::steady_clock::now() >= deadline) {
                        break;
                    }

                    counters.coldWriteAttempts.fetch_add(1, std::memory_order_relaxed);
                    std::vector<std::string> pending = coldWriteGroups[workerIdx][localIdx].keys;
                    bool complete = false;
                    for (int retry = 0; retry < kSetRetryCount && !pending.empty(); ++retry) {
                        std::vector<StringView> values(pending.size(), StringView(coldValue));
                        std::vector<std::string> failedKeys;
                        const Status status = clients_[workerIdx]->MSet(pending, values, failedKeys, param);
                        if (status.IsOk() && failedKeys.empty()) {
                            complete = true;
                            break;
                        }
                        if (!failedKeys.empty()) {
                            pending = std::move(failedKeys);
                        }
                        std::this_thread::sleep_for(std::chrono::milliseconds(kSetRetryIntervalMs));
                    }
                    if (complete) {
                        counters.coldWriteSuccesses.fetch_add(1, std::memory_order_relaxed);
                    } else {
                        counters.coldWriteErrors.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            });
        }

        std::vector<std::thread> threads;
        threads.reserve(static_cast<size_t>(config.requestThreads));
        for (size_t threadIdx = 0; threadIdx < config.requestThreads; ++threadIdx) {
            (void)threadIdx;
            threads.emplace_back([&]() {
                while (!stop.load(std::memory_order_acquire)) {
                    const uint64_t requestId = nextRequest.fetch_add(1, std::memory_order_relaxed);
                    if (requestId >= counters.targetRequests) {
                        break;
                    }
                    const auto scheduledOffset = std::chrono::duration<double>(
                        static_cast<double>(requestId) / static_cast<double>(config.targetRequestQps));
                    const auto scheduledAt = start
                                             + std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                                                 scheduledOffset);
                    if (scheduledAt > std::chrono::steady_clock::now()) {
                        std::this_thread::sleep_until(scheduledAt);
                    }
                    if (stop.load(std::memory_order_acquire) || std::chrono::steady_clock::now() >= deadline) {
                        break;
                    }

                    // Bind requester and trace phase to requestId rather than to the thread that wins the atomic
                    // fetch. Threads provide concurrency only; the same requestId must produce the same key×worker
                    // pair in every run so Clock/Heat comparisons replay an identical distributed trace.
                    const size_t deterministicClient = static_cast<size_t>(requestId % clients_.size());
                    const size_t planPos = static_cast<size_t>(
                        (requestId + deterministicClient * kBlockAccessThreadStride)
                        % accessPlan.repeatingGroupIndices.size());
                    const auto &group = accessPlan.groups[accessPlan.repeatingGroupIndices[planPos]];
                    const size_t clientIdx = ownerAffinity ? group.ownerWorker : deterministicClient;
                    std::vector<Optional<ReadOnlyBuffer>> buffers;
                    const Status status = clients_[clientIdx]->Get(group.keys, buffers, kGetTimeoutMs);
                    counters.requestAttempts.fetch_add(1, std::memory_order_relaxed);
                    counters.attempts.fetch_add(group.keys.size(), std::memory_order_relaxed);
                    if (status.IsError()) {
                        counters.requestErrors.fetch_add(1, std::memory_order_relaxed);
                        counters.getErrors.fetch_add(group.keys.size(), std::memory_order_relaxed);
                        continue;
                    }

                    bool requestOk = buffers.size() == group.keys.size();
                    if (!requestOk) {
                        counters.getErrors.fetch_add(group.keys.size(), std::memory_order_relaxed);
                    } else {
                        for (auto &buffer : buffers) {
                            if (!buffer || buffer->GetSize() != static_cast<int64_t>(config.payloadBlockBytes)
                                || buffer->ImmutableData() == nullptr) {
                                counters.getErrors.fetch_add(1, std::memory_order_relaxed);
                                requestOk = false;
                                continue;
                            }
                            counters.successes.fetch_add(1, std::memory_order_relaxed);
                            if (std::memcmp(buffer->ImmutableData(), expectedBlock.data(), expectedBlock.size()) != 0) {
                                counters.wrongValues.fetch_add(1, std::memory_order_relaxed);
                                requestOk = false;
                            }
                        }
                    }
                    if (requestOk) {
                        counters.requestSuccesses.fetch_add(1, std::memory_order_relaxed);
                    } else {
                        counters.requestErrors.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            });
        }

        std::this_thread::sleep_until(deadline);
        stop.store(true, std::memory_order_release);
        for (auto &thread : threads) {
            thread.join();
        }
        for (auto &thread : coldWriteThreads) {
            thread.join();
        }
    }

    void WriteBlockWorkloadSummary(const WorkloadProfile &profile, const BlockWorkloadConfig &config,
                                   const BlockWorkloadCounters &counters)
    {
        const uint64_t requestAttempts = counters.requestAttempts.load(std::memory_order_relaxed);
        const uint64_t requestSuccesses = counters.requestSuccesses.load(std::memory_order_relaxed);
        const uint64_t requestErrors = counters.requestErrors.load(std::memory_order_relaxed);
        const uint64_t coldWriteAttempts = counters.coldWriteAttempts.load(std::memory_order_relaxed);
        const uint64_t coldWriteSuccesses = counters.coldWriteSuccesses.load(std::memory_order_relaxed);
        const uint64_t coldWriteErrors = counters.coldWriteErrors.load(std::memory_order_relaxed);
        const uint64_t blockAttempts = counters.attempts.load(std::memory_order_relaxed);
        const int measureSec = GetPositiveEnvInt(kMeasureSecEnv, kDefaultMeasureSec);
        const double requestQps = static_cast<double>(requestAttempts) / measureSec;
        const double blockGetQps = static_cast<double>(blockAttempts) / measureSec;
        const double payloadGiBPerSec =
            blockGetQps * static_cast<double>(config.payloadBlockBytes) / (1024.0 * 1024.0 * 1024.0);
        const double logicalGiBPerSec = payloadGiBPerSec * static_cast<double>(config.resourceScale);
        constexpr double kModeledMissRatio = 0.30;
        const double modeledMissMiBPerSec = requestQps * static_cast<double>(config.logicalRequestBytes)
                                            * kModeledMissRatio / (1024.0 * 1024.0);
        const uint64_t backpressuredRequests = counters.targetRequests > requestAttempts
                                                   ? counters.targetRequests - requestAttempts
                                                   : 0;
        std::cout << "[block_workload_summary] variant=" << Variant() << " workload=" << profile.name
                  << " topology=" << Topology() << " targetRequestQps=" << config.targetRequestQps
                  << " achievedRequestQps=" << requestQps << " achievedBlockGetQps=" << blockGetQps
                  << " dataGiBPerSec=" << logicalGiBPerSec << " payloadGiBPerSec=" << payloadGiBPerSec
                  << " modeledMissMiBPerSec=" << modeledMissMiBPerSec
                  << " targetRequests=" << counters.targetRequests
                  << " requestAttempts=" << requestAttempts << " requestSuccesses=" << requestSuccesses
                  << " requestErrors=" << requestErrors << " backpressuredRequests=" << backpressuredRequests
                  << " coldWriteQps=" << GetNonNegativeEnvInt(kBlockColdWriteQpsEnv, kDefaultBlockColdWriteQps)
                  << " coldWriteAttempts=" << coldWriteAttempts
                  << " coldWriteSuccesses=" << coldWriteSuccesses << " coldWriteErrors=" << coldWriteErrors
                  << " blocksPerRequest=" << config.blocksPerRequest
                  << " tokensPerBlock=" << config.tokensPerBlock
                  << " bytesPerToken=" << config.logicalBytesPerToken
                  << " payloadBytesPerToken=" << config.payloadBytesPerToken
                  << " blockBytes=" << config.logicalBlockBytes
                  << " payloadBlockBytes=" << config.payloadBlockBytes
                  << " requestBytes=" << config.logicalRequestBytes
                  << " payloadRequestBytes=" << config.payloadRequestBytes
                  << " resourceScale=" << config.resourceScale
                  << " requestThreads=" << config.requestThreads << " warmupRequests=" << config.warmupRequests
                  << std::endl;

        const std::string fileName =
            FormatString("block_summary_%s_%s_%s.csv", Topology(), profile.name, Variant());
        const std::string outputDir = GetEnv(kOutputDirEnv);
        const std::filesystem::path path = outputDir.empty()
                                               ? std::filesystem::path(cluster_->GetRootDir()) / fileName
                                               : std::filesystem::path(outputDir) / fileName;
        if (!outputDir.empty()) {
            std::error_code error;
            std::filesystem::create_directories(outputDir, error);
            EXPECT_FALSE(error) << "failed to create block summary directory: " << error.message();
            if (error) {
                return;
            }
        }
        std::ofstream output(path);
        ASSERT_TRUE(output.is_open()) << path.string();
        output << "variant,workload,topology,targetRequestQps,achievedRequestQps,achievedBlockGetQps,"
                  "dataGiBPerSec,targetRequests,requestAttempts,requestSuccesses,requestErrors,backpressuredRequests,"
                  "blocksPerRequest,tokensPerBlock,bytesPerToken,blockBytes,requestBytes,requestThreads,measureSec,"
                  "payloadBytesPerToken,payloadBlockBytes,payloadRequestBytes,resourceScale,payloadGiBPerSec,"
                  "modeledMissMiBPerSec,warmupRequests,ownerAffinity,coldWriteQps,coldWriteAttempts,"
                  "coldWriteSuccesses,coldWriteErrors\n";
        output << Variant() << ',' << profile.name << ',' << Topology() << ',' << config.targetRequestQps << ','
               << requestQps << ',' << blockGetQps << ',' << logicalGiBPerSec << ',' << counters.targetRequests << ','
               << requestAttempts << ',' << requestSuccesses << ',' << requestErrors << ',' << backpressuredRequests
               << ',' << config.blocksPerRequest << ',' << config.tokensPerBlock << ','
               << config.logicalBytesPerToken << ',' << config.logicalBlockBytes << ',' << config.logicalRequestBytes
               << ',' << config.requestThreads << ',' << measureSec << ',' << config.payloadBytesPerToken << ','
               << config.payloadBlockBytes << ',' << config.payloadRequestBytes << ',' << config.resourceScale << ','
               << payloadGiBPerSec << ',' << modeledMissMiBPerSec << ',' << config.warmupRequests << ','
               << GetEnvBool(kBlockOwnerAffinityEnv, false) << ','
               << GetNonNegativeEnvInt(kBlockColdWriteQpsEnv, kDefaultBlockColdWriteQps) << ',' << coldWriteAttempts
               << ',' << coldWriteSuccesses << ',' << coldWriteErrors << '\n';
    }

    template <typename PressureFn>
    void MeasureBlockScenario(const WorkloadProfile &profile, std::vector<WorkerKeySet> &keySets,
                              const BlockRequestGroupsByWorker &coldWriteGroups, const BlockWorkloadConfig &config,
                              char fillChar, PressureFn &&applyPressure)
    {
        aggregateTelemetry_ = AggregateTelemetry{};
        aggregateTelemetry_.baselineMemBytes.resize(static_cast<size_t>(WorkerNum()), 0);
        aggregateTelemetry_.pressureMemMax.resize(static_cast<size_t>(WorkerNum()), 0);
        aggregateTelemetry_.baselinePrimaryBytes.resize(static_cast<size_t>(WorkerNum()), 0);
        aggregateTelemetry_.postPressurePrimaryMax.resize(static_cast<size_t>(WorkerNum()), 0);
        aggregateTelemetry_.baselineSpillBytes.resize(static_cast<size_t>(WorkerNum()), 0);
        aggregateTelemetry_.postPressureSpillMax.resize(static_cast<size_t>(WorkerNum()), 0);
        const BlockAccessPlan accessPlan = BuildBlockAccessPlan(keySets, profile, config);
        ASSERT_FALSE(accessPlan.repeatingGroupIndices.empty());
        OpenCsv(profile);
        WarmHotKeys(keySets, fillChar);

        std::atomic<bool> stop{ false };
        std::atomic<WorkloadPhase> phase{ WorkloadPhase::BASELINE };
        BlockWorkloadCounters counters;
        std::thread sampler([&]() { SamplerLoop(stop, phase, profile, counters); });
        std::this_thread::sleep_for(std::chrono::seconds(kLogIntervalSec * kBaselineSampleIntervals));

        phase.store(WorkloadPhase::PRESSURE, std::memory_order_release);
        applyPressure();
        phase.store(WorkloadPhase::WARMUP, std::memory_order_release);
        const Status warmupStatus = WarmBlockAccessPlan(accessPlan, config);
        if (warmupStatus.IsError()) {
            stop.store(true, std::memory_order_release);
            sampler.join();
            FAIL() << "block steady-state warm-up failed: " << warmupStatus.ToString();
            return;
        }
        const Status coldScanStatus = RunBlockColdScan(accessPlan, config);
        if (coldScanStatus.IsError()) {
            stop.store(true, std::memory_order_release);
            sampler.join();
            FAIL() << "block cold scan failed: " << coldScanStatus.ToString();
            return;
        }
        std::this_thread::sleep_for(std::chrono::seconds(kLogIntervalSec));

        phase.store(WorkloadPhase::MEASURE, std::memory_order_release);
        RunBlockReadWorkload(accessPlan, coldWriteGroups, config, fillChar, counters, stop);
        sampler.join();
        ValidateTelemetryResult(profile, counters);
        EXPECT_GT(counters.requestAttempts.load(std::memory_order_relaxed), 0UL)
            << "no logical block requests were attempted";
        EXPECT_EQ(counters.requestErrors.load(std::memory_order_relaxed), 0UL)
            << "at least one logical block request was incomplete";
        EXPECT_EQ(counters.coldWriteErrors.load(std::memory_order_relaxed), 0UL)
            << "at least one miss-driven cold KVC refill failed";
        WriteBlockWorkloadSummary(profile, config, counters);
    }

    template <typename PressureFn>
    void MeasureScenario(const WorkloadProfile &profile, std::vector<WorkerKeySet> &keySets,
                         char fillChar, PressureFn &&applyPressure)
    {
        aggregateTelemetry_ = AggregateTelemetry{};
        aggregateTelemetry_.baselineMemBytes.resize(static_cast<size_t>(WorkerNum()), 0);
        aggregateTelemetry_.pressureMemMax.resize(static_cast<size_t>(WorkerNum()), 0);
        aggregateTelemetry_.baselinePrimaryBytes.resize(static_cast<size_t>(WorkerNum()), 0);
        aggregateTelemetry_.postPressurePrimaryMax.resize(static_cast<size_t>(WorkerNum()), 0);
        aggregateTelemetry_.baselineSpillBytes.resize(static_cast<size_t>(WorkerNum()), 0);
        aggregateTelemetry_.postPressureSpillMax.resize(static_cast<size_t>(WorkerNum()), 0);
        const GlobalKeyPool pool = BuildGlobalKeyPool(keySets);
        const WorkloadAccessPlan accessPlan = BuildGlobalAccessPlan(pool, profile, keySets);
        ASSERT_FALSE(accessPlan.repeatingIndices.empty());
        EXPECT_EQ(accessPlan.oneHitIndices.size(), ExpectedOneHitCount(keySets))
            << "one-hit keys must be represented exactly once in the global access plan";
        OpenCsv(profile);
        WarmHotKeys(keySets, fillChar);

        std::atomic<bool> stop{ false };
        std::atomic<WorkloadPhase> phase{ WorkloadPhase::BASELINE };
        WorkloadCounters counters;
        std::thread sampler([&]() { SamplerLoop(stop, phase, profile, counters); });
        std::this_thread::sleep_for(std::chrono::seconds(kLogIntervalSec * kBaselineSampleIntervals));

        phase.store(WorkloadPhase::PRESSURE, std::memory_order_release);
        applyPressure();
        std::this_thread::sleep_for(std::chrono::seconds(kLogIntervalSec));

        phase.store(WorkloadPhase::MEASURE, std::memory_order_release);
        RunReadWorkload(pool, accessPlan, fillChar, counters, stop);
        sampler.join();
        ValidateTelemetryResult(profile, counters);
    }

private:
    std::ofstream csv_;
    std::mutex csvMutex_;
    uint64_t csvRows_ = 0;
    uint64_t validCopyWatermarkRows_ = 0;
    uint64_t positivePrimaryRows_ = 0;
    uint64_t positiveHotPrimaryRows_ = 0;
    uint64_t invalidCopyWatermarkRows_ = 0;
    AggregateTelemetry aggregateTelemetry_;
    std::string traceSource_ = "synthetic";
    size_t traceEvents_ = 0;
};

enum class TelemetryScenario : uint8_t { EVICTION, REBALANCE, COMBINED };

struct TelemetryCase {
    TelemetryScenario scenario;
    const WorkloadProfile *profile;
};

struct ScenarioWorkload {
    std::vector<size_t> totalKeys;
    std::vector<size_t> residentKeys;
    std::vector<size_t> hotKeys;
    WriteMode writeMode;
    char fillChar;
};

class EXCLUSIVE_LEVEL1_KVClientWorkloadTelemetryTest : public KVClientWorkloadTelemetryTest,
                                                        public testing::WithParamInterface<TelemetryCase> {
public:
    void ConfigureCluster(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numOBS = 0;
        switch (GetParam().scenario) {
            case TelemetryScenario::EVICTION:
                ConfigureEviction(opts);
                break;
            case TelemetryScenario::REBALANCE:
                ConfigureRebalance(opts);
                break;
            case TelemetryScenario::COMBINED:
                ConfigureCombined(opts);
                break;
        }
    }

    const char *Topology() const override
    {
        switch (GetParam().scenario) {
            case TelemetryScenario::EVICTION:
                return "eviction";
            case TelemetryScenario::REBALANCE:
                return "rebalance";
            case TelemetryScenario::COMBINED:
                return "combined";
        }
        return "unknown";
    }

    int WorkerNum() const override
    {
        return GetParam().scenario == TelemetryScenario::EVICTION ? 1 : 3;
    }

    AccessKeyPlacement KeyPlacement() const override
    {
        return GetParam().scenario == TelemetryScenario::COMBINED ? AccessKeyPlacement::WORKER_ROUND_ROBIN
                                                                  : AccessKeyPlacement::CONTIGUOUS;
    }

    bool RequireCombinedSignals() const override
    {
        return GetParam().scenario == TelemetryScenario::COMBINED;
    }

protected:
    void RunCase()
    {
        const auto workload = WorkloadFor(GetParam().scenario);
        struct SetParam param{ .writeMode = workload.writeMode };
        auto keySets = GenerateKeySets(workload.totalKeys, workload.residentKeys, workload.hotKeys);
        ASSERT_EQ(keySets.size(), workload.totalKeys.size());
        for (size_t workerIdx = 0; workerIdx < keySets.size(); ++workerIdx) {
            ASSERT_EQ(keySets[workerIdx].keys.size(), workload.totalKeys[workerIdx]);
            PopulateRange(workerIdx, keySets[workerIdx], 0, workload.residentKeys[workerIdx], workload.fillChar,
                          param);
        }
        MeasureScenario(*GetParam().profile, keySets, workload.fillChar, [&]() {
            PopulateRange(0, keySets[0], workload.residentKeys[0], workload.totalKeys[0], workload.fillChar, param);
        });
    }

private:
    static constexpr const char *kRebalanceInjectActions =
        "NodeSelector.setInterval:call(200);ResourceManager.setInterval:call(200);"
        "WorkerOCServer.heatMaintenanceIntervalMs:call(1000);"
        "WorkerOCServer.copyWatermarkTelemetryIntervalMs:call(1000)";

    static ScenarioWorkload WorkloadFor(TelemetryScenario scenario)
    {
        switch (scenario) {
            case TelemetryScenario::EVICTION:
                return { { 1800 }, { 700 }, { 360 }, WriteMode::NONE_L2_CACHE_EVICT, 'e' };
            case TelemetryScenario::REBALANCE:
                return { { 3500, 60, 60 }, { 1500, 60, 60 }, { 700, 12, 12 }, WriteMode::NONE_L2_CACHE, 'r' };
            case TelemetryScenario::COMBINED:
                return { { 1800, 400, 400 }, { 400, 400, 400 }, { 200, 200, 200 },
                         WriteMode::NONE_L2_CACHE_EVICT, 'c' };
        }
        return {};
    }

    void ConfigureEviction(ExternalClusterOptions &opts)
    {
        opts.numWorkers = 1;
        opts.enableSpill = true;
        opts.enableDistributedMaster = "false";
        opts.workerGflagParams =
            "-shared_memory_size_mb=32 -log_monitor=true -json_log_monitor=true "
            "-log_monitor_interval_ms=1000 -skip_authenticate=true -v=1 -spill_size_limit=134217728 "
            "-enable_memory_rebalance=false -arena_per_tenant=1";
        opts.workerSpecifyGflagParams[0] = FormatString(
            "-shared_disk_directory=%s/worker0/shared_disk -shared_disk_size_mb=256", GetTestCaseDataDir());
        opts.injectActions = "NodeSelector.setInterval:call(200);WorkerOCServer.heatMaintenanceIntervalMs:call(1000);"
                             "WorkerOCServer.copyWatermarkTelemetryIntervalMs:call(1000)";
    }

    void ConfigureRebalance(ExternalClusterOptions &opts)
    {
        opts.numWorkers = 3;
        opts.workerGflagParams =
            "-shared_memory_size_mb=64 -log_monitor=true -json_log_monitor=true "
            "-log_monitor_interval_ms=1000 -skip_authenticate=true -v=1 -enable_data_replication=true "
            "-enable_memory_rebalance=true -rebalance_usage_gap_percent=20 -rebalance_source_usage_percent=60 "
            "-rebalance_task_report_grace_ms=500 -data_migrate_rate_limit_mb=1024 "
            "-arena_per_tenant=1";
        opts.injectActions = kRebalanceInjectActions;
    }

    void ConfigureCombined(ExternalClusterOptions &opts)
    {
        constexpr size_t kSpillLimitBytes = 128UL * 1024UL * 1024UL;
        const int memoryMb = GetPositiveEnvInt(kCombinedMemoryMbEnv, kDefaultCombinedMemoryMb);
        opts.numWorkers = 3;
        opts.enableSpill = true;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = FormatString(
            "-shared_memory_size_mb=%d -eviction_high_watermark_ratio=0.75 "
            "-eviction_low_watermark_ratio=0.65 -log_monitor=true -json_log_monitor=true "
            "-log_monitor_interval_ms=1000 -skip_authenticate=true -v=1 -spill_size_limit=%zu "
            "-enable_data_replication=true -enable_memory_rebalance=true -rebalance_usage_gap_percent=20 "
            "-rebalance_source_usage_percent=60 -rebalance_task_report_grace_ms=500 "
            "-data_migrate_rate_limit_mb=1024 -rebalance_keep_local_copy=true -arena_per_tenant=1",
            memoryMb, kSpillLimitBytes);
        for (int workerIdx = 0; workerIdx < WorkerNum(); ++workerIdx) {
            opts.workerSpecifyGflagParams[workerIdx] =
                FormatString("-shared_disk_directory=%s/worker%d/shared_disk -shared_disk_size_mb=256",
                             GetTestCaseDataDir(), workerIdx);
        }
        opts.injectActions = kRebalanceInjectActions;
    }

};

class EXCLUSIVE_LEVEL1_KVClientBlockRecallWorkloadTelemetryTest : public KVClientWorkloadTelemetryTest {
public:
    void ConfigureCluster(ExternalClusterOptions &opts) override
    {
        // The source catalog is 960 MiB at the default payload scale. Keep spill capacity above the complete source
        // working set so memory calibration measures replacement quality instead of exhausting the test disk.
        constexpr size_t kSpillLimitBytes = 2UL * 1024UL * 1024UL * 1024UL;
        const int memoryMb = GetPositiveEnvInt(kBlockMemoryMbEnv, kDefaultBlockMemoryMb);
        opts.numWorkers = kBlockTestWorkers;
        opts.numEtcd = 1;
        opts.numOBS = 0;
        opts.enableSpill = true;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = FormatString(
            "-shared_memory_size_mb=%d -eviction_high_watermark_ratio=0.75 "
            "-eviction_low_watermark_ratio=0.65 -log_monitor=true -json_log_monitor=true "
            "-log_monitor_interval_ms=1000 -skip_authenticate=true -v=1 -spill_size_limit=%zu "
            "-enable_data_replication=true -enable_memory_rebalance=true -rebalance_usage_gap_percent=20 "
            "-rebalance_source_usage_percent=60 -rebalance_task_report_grace_ms=500 "
            "-data_migrate_rate_limit_mb=2048 -rebalance_keep_local_copy=true -arena_per_tenant=1",
            memoryMb, kSpillLimitBytes);
        for (int workerIdx = 0; workerIdx < WorkerNum(); ++workerIdx) {
            opts.workerSpecifyGflagParams[workerIdx] =
                FormatString("-shared_disk_directory=%s/worker%d/shared_disk -shared_disk_size_mb=2048",
                             GetTestCaseDataDir(), workerIdx);
        }
        opts.injectActions = "NodeSelector.setInterval:call(200);ResourceManager.setInterval:call(200);"
                             "MemoryRebalanceScheduler.CooldownSeconds:call(1);"
                             "WorkerOCServer.heatMaintenanceIntervalMs:call(1000);"
                             "WorkerOCServer.copyWatermarkTelemetryIntervalMs:call(1000)";
    }

    const char *Topology() const override
    {
        return "block_combined";
    }

    int WorkerNum() const override
    {
        return kBlockTestWorkers;
    }

    AccessKeyPlacement KeyPlacement() const override
    {
        return AccessKeyPlacement::WORKER_ROUND_ROBIN;
    }

    bool RequireCombinedSignals() const override
    {
        return true;
    }

    std::string HeatTestFlags() const override
    {
        // Preserve primary heat across roughly two modeled cache-turnover periods while allowing retained local copies
        // to age out sooner. Keep the calibrated workload explicit even though these values match product defaults.
        return "-eviction_heat_half_life_primary_s=600 -eviction_heat_half_life_local_s=300";
    }

    AggregateMemRateContract RequiredAggregateMemRateContract() const override
    {
        const int memoryMb = GetPositiveEnvInt(kBlockMemoryMbEnv, kDefaultBlockMemoryMb);
        const int resourceScale = GetPositiveEnvInt(kBlockResourceScaleEnv, kDefaultBlockResourceScale);
        const bool calibratedWorkload =
            GetPositiveEnvInt(kMeasureSecEnv, kDefaultMeasureSec) == kBlockContractMeasureSec
            && GetPositiveEnvInt(kBlockWarmupRequestsEnv, kDefaultBlockWarmupRequests)
                   == kDefaultBlockWarmupRequests
            && GetNonNegativeEnvInt(kBlockColdScanRequestsEnv, kDefaultBlockColdScanRequests)
                   == kDefaultBlockColdScanRequests
            && GetNonNegativeEnvInt(kBlockColdWriteQpsEnv, kDefaultBlockColdWriteQps)
                   == kDefaultBlockColdWriteQps;
        if (!calibratedWorkload) {
            return {};
        }
        return MakeBlockMemRateContract(memoryMb, resourceScale, StrategyFromExtraFlags("eviction_strategy", "clock"),
                                        StrategyFromExtraFlags("rebalance_strategy", "memory"));
    }

protected:
    int GetTestCaseTimeoutSecs() const override
    {
        constexpr int kLargeBlockSetupMarginSec = 240;
        return GetPositiveEnvInt(kMeasureSecEnv, kDefaultMeasureSec) + kLargeBlockSetupMarginSec;
    }

    void RunBlockRecallProfile()
    {
        // 7,680 logical KVCs cover all 7,552 unique videos in the normalized KuaiRand 100k trace. Initial placement is
        // balanced at 1,280 KVCs/worker; pressure adds 3,840 KVCs only to worker0. With the default 1:64 payload scale,
        // this moves worker0 from 240 MiB to 960 MiB against an 840 MiB high watermark and triggers both mechanisms.
        constexpr size_t kSourceLogicalKvCacheCount = 5'120;
        constexpr size_t kResidentLogicalKvCacheCount = 1'280;
        constexpr size_t kTargetLogicalKvCacheCount = 1'280;
        constexpr size_t kHotLogicalKvCacheCount = 256;
        constexpr char kFillChar = 'b';
        const BlockWorkloadConfig config = GetBlockWorkloadConfig();
        ASSERT_TRUE(config.valid) << "invalid block workload geometry";
        ASSERT_EQ(config.logicalRequestBytes, config.blocksPerRequest * config.logicalBlockBytes);
        ASSERT_EQ(config.payloadRequestBytes, config.blocksPerRequest * config.payloadBlockBytes);
        std::cout << "[block_workload_config] targetRequestQps=" << config.targetRequestQps
                  << " blocksPerRequest=" << config.blocksPerRequest
                  << " tokensPerBlock=" << config.tokensPerBlock
                  << " logicalBytesPerToken=" << config.logicalBytesPerToken
                  << " payloadBytesPerToken=" << config.payloadBytesPerToken
                  << " logicalRequestBytes=" << config.logicalRequestBytes
                  << " payloadRequestBytes=" << config.payloadRequestBytes
                  << " resourceScale=" << config.resourceScale
                  << " requestThreads=" << config.requestThreads << " warmupRequests=" << config.warmupRequests
                  << " coldWriteQps=" << GetNonNegativeEnvInt(kBlockColdWriteQpsEnv, kDefaultBlockColdWriteQps)
                  << " ownerAffinity=" << GetEnvBool(kBlockOwnerAffinityEnv, false)
                  << std::endl;

        const size_t blocksPerRequest = static_cast<size_t>(config.blocksPerRequest);
        const size_t sourceBlockCount = kSourceLogicalKvCacheCount * blocksPerRequest;
        const size_t residentBlockCount = kResidentLogicalKvCacheCount * blocksPerRequest;
        const size_t targetBlockCount = kTargetLogicalKvCacheCount * blocksPerRequest;
        const size_t hotBlockCount = kHotLogicalKvCacheCount * blocksPerRequest;
        const uint64_t coldWriteQps =
            static_cast<uint64_t>(GetNonNegativeEnvInt(kBlockColdWriteQpsEnv, kDefaultBlockColdWriteQps));
        const uint64_t coldWriteRequestCount =
            coldWriteQps * static_cast<uint64_t>(GetPositiveEnvInt(kMeasureSecEnv, kDefaultMeasureSec));
        std::vector<size_t> mainBlockCounts{ sourceBlockCount, targetBlockCount, targetBlockCount };
        std::vector<size_t> generatedBlockCounts = mainBlockCounts;
        for (size_t workerIdx = 0; workerIdx < generatedBlockCounts.size(); ++workerIdx) {
            const uint64_t workerColdRequests = coldWriteRequestCount <= workerIdx
                                                    ? 0
                                                    : (coldWriteRequestCount - 1 - workerIdx) / WorkerNum() + 1;
            generatedBlockCounts[workerIdx] += static_cast<size_t>(workerColdRequests) * blocksPerRequest;
        }
        SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE_EVICT };
        auto keySets = GenerateKeySets(generatedBlockCounts,
                                       { residentBlockCount, residentBlockCount, residentBlockCount },
                                       { hotBlockCount, hotBlockCount, hotBlockCount });
        BlockRequestGroupsByWorker coldWriteGroups(keySets.size());
        for (size_t workerIdx = 0; workerIdx < keySets.size(); ++workerIdx) {
            auto &keySet = keySets[workerIdx];
            const size_t mainBlockCount = mainBlockCounts[workerIdx];
            ASSERT_EQ((keySet.keys.size() - mainBlockCount) % blocksPerRequest, 0UL);
            for (size_t begin = mainBlockCount; begin < keySet.keys.size(); begin += blocksPerRequest) {
                BlockRequestGroup group;
                group.ownerWorker = workerIdx;
                group.keys.assign(keySet.keys.begin() + static_cast<ptrdiff_t>(begin),
                                  keySet.keys.begin() + static_cast<ptrdiff_t>(begin + blocksPerRequest));
                coldWriteGroups[workerIdx].emplace_back(std::move(group));
            }
            keySet.keys.resize(mainBlockCount);
            keySet.keySizeBytes.assign(keySet.keys.size(), static_cast<uint32_t>(config.payloadBlockBytes));
        }
        for (size_t workerIdx = 0; workerIdx < keySets.size(); ++workerIdx) {
            PopulateRange(workerIdx, keySets[workerIdx], 0, residentBlockCount, kFillChar, param);
        }

        MeasureBlockScenario(kBlockRecallProfile, keySets, coldWriteGroups, config, kFillChar, [&]() {
            PopulateRange(0, keySets[0], residentBlockCount, sourceBlockCount, kFillChar, param);
        });
    }
};

TEST_P(EXCLUSIVE_LEVEL1_KVClientWorkloadTelemetryTest, Workload)
{
    RunCase();
}

std::string TelemetryCaseName(const testing::TestParamInfo<TelemetryCase> &info)
{
    const char *scenario = "Combined";
    if (info.param.scenario == TelemetryScenario::EVICTION) {
        scenario = "Eviction";
    } else if (info.param.scenario == TelemetryScenario::REBALANCE) {
        scenario = "Rebalance";
    }
    return std::string(scenario) + info.param.profile->name;
}

INSTANTIATE_TEST_SUITE_P(
    Scenarios, EXCLUSIVE_LEVEL1_KVClientWorkloadTelemetryTest,
    testing::Values(TelemetryCase{ TelemetryScenario::EVICTION, &kRecallProfile },
                    TelemetryCase{ TelemetryScenario::EVICTION, &kFineRankProfile },
                    TelemetryCase{ TelemetryScenario::REBALANCE, &kRecallProfile },
                    TelemetryCase{ TelemetryScenario::REBALANCE, &kFineRankProfile },
                    TelemetryCase{ TelemetryScenario::COMBINED, &kRecallProfile },
                    TelemetryCase{ TelemetryScenario::COMBINED, &kFineRankProfile }),
    TelemetryCaseName);

// Full-scale calibration exceeds the routine ST budget; run explicitly on an isolated performance runner.
TEST_F(EXCLUSIVE_LEVEL1_KVClientBlockRecallWorkloadTelemetryTest, DISABLED_RecallBlockWorkload)
{
    RunBlockRecallProfile();
}

TEST(AccessKeyOrderTest, PreservesLegacyContiguousPlacement)
{
    EXPECT_EQ(BuildAccessKeyOrder({ 4, 2, 1 }, AccessKeyPlacement::CONTIGUOUS),
              (std::vector<size_t>{ 0, 1, 2, 3, 4, 5, 6 }));
}

TEST(AccessKeyOrderTest, InterleavesWorkersWithoutDuplicates)
{
    EXPECT_EQ(BuildAccessKeyOrder({ 4, 2, 1 }, AccessKeyPlacement::WORKER_ROUND_ROBIN),
              (std::vector<size_t>{ 0, 4, 6, 1, 5, 2, 3 }));
}

TEST(KuaiRandTraceFormatTest, ParsesChronologicalNormalizedEvents)
{
    std::istringstream input("# kuairand-trace-v1\n"
                             "# source=KuaiRand-Pure\n"
                             "time_ms,video_id,engaged\n"
                             "100,7,0\n"
                             "100,8,1\n"
                             "101,7,1\n");
    std::vector<KuaiRandEvent> events;
    std::string error;
    ASSERT_TRUE(ParseKuaiRandTrace(input, events, error)) << error;
    ASSERT_EQ(events.size(), 3UL);
    EXPECT_EQ(events[0].videoId, 7UL);
    EXPECT_FALSE(events[0].engaged);
    EXPECT_EQ(events[1].videoId, 8UL);
    EXPECT_TRUE(events[1].engaged);
}

TEST(KuaiRandTraceFormatTest, RejectsOutOfOrderOrInvalidFeedback)
{
    for (const auto &data : {
             std::string("# kuairand-trace-v1\ntime_ms,video_id,engaged\n101,7,0\n100,8,1\n"),
             std::string("# kuairand-trace-v1\ntime_ms,video_id,engaged\n100,7,2\n") }) {
        std::istringstream input(data);
        std::vector<KuaiRandEvent> events;
        std::string error;
        EXPECT_FALSE(ParseKuaiRandTrace(input, events, error));
        EXPECT_FALSE(error.empty());
    }
}

TEST(BlockWorkloadGeometryTest, ScalesProductionGeometryWithoutChangingCapacityRatio)
{
    const BlockWorkloadConfig config =
        MakeBlockWorkloadConfig(kDefaultBlocksPerRequest, kDefaultTokensPerBlock, kDefaultBytesPerToken,
                                kDefaultBlockResourceScale, kDefaultTargetRequestQps, kDefaultBlockRequestThreads,
                                kDefaultBlockWarmupRequests);
    ASSERT_TRUE(config.valid);
    EXPECT_EQ(config.blocksPerRequest, 16UL);
    EXPECT_EQ(config.tokensPerBlock, 64UL);
    EXPECT_EQ(config.logicalBytesPerToken, 12UL * 1024UL);
    EXPECT_EQ(config.payloadBytesPerToken, 192UL);
    EXPECT_EQ(config.logicalBlockBytes, 768UL * 1024UL);
    EXPECT_EQ(config.payloadBlockBytes, 12UL * 1024UL);
    EXPECT_EQ(config.logicalRequestBytes, 12UL * 1024UL * 1024UL);
    EXPECT_EQ(config.payloadRequestBytes, 192UL * 1024UL);
    EXPECT_EQ(config.resourceScale, 64UL);
    EXPECT_EQ(kDefaultBlockMemoryMb, 1'120);
    EXPECT_EQ(config.targetRequestQps, 211UL);
    EXPECT_EQ(config.warmupRequests, 10'000UL);
    EXPECT_EQ(kDefaultBlockColdScanRequests, 800);
    EXPECT_EQ(kDefaultBlockColdWriteQps, 64);
}

TEST(BlockWorkloadGeometryTest, RejectsZeroAndOverflowingGeometry)
{
    EXPECT_FALSE(MakeBlockWorkloadConfig(0, 64, 12 * 1024, 64, 211, 3, 5'000).valid);
    EXPECT_FALSE(MakeBlockWorkloadConfig(16, 0, 12 * 1024, 64, 211, 3, 5'000).valid);
    EXPECT_FALSE(
        MakeBlockWorkloadConfig(16, std::numeric_limits<uint64_t>::max(), 2, 1, 211, 3, 5'000).valid);
    EXPECT_FALSE(MakeBlockWorkloadConfig(16, 64, 12 * 1024, 0, 211, 3, 5'000).valid);
    EXPECT_FALSE(MakeBlockWorkloadConfig(16, 64, 12 * 1024, 100, 211, 3, 5'000).valid);
    EXPECT_FALSE(MakeBlockWorkloadConfig(16, 64, 12 * 1024, 64, 211, 1025, 5'000).valid);
    EXPECT_FALSE(MakeBlockWorkloadConfig(16, 64, 12 * 1024, 64, 211, 3, 0).valid);
}

TEST(BlockWorkloadGeometryTest, EnforcesStrategyContractsOnlyForDefaultResourceGeometry)
{
    const auto clockContract =
        MakeBlockMemRateContract(kDefaultBlockMemoryMb, kDefaultBlockResourceScale, "clock", "memory");
    ASSERT_TRUE(clockContract.enabled);
    EXPECT_DOUBLE_EQ(clockContract.minimum, 0.68);
    EXPECT_DOUBLE_EQ(clockContract.maximum, 0.72);

    const auto heatContract =
        MakeBlockMemRateContract(kDefaultBlockMemoryMb, kDefaultBlockResourceScale, "heat", "heat");
    ASSERT_TRUE(heatContract.enabled);
    EXPECT_DOUBLE_EQ(heatContract.minimum, 0.79);
    EXPECT_DOUBLE_EQ(heatContract.maximum, 1.0);

    EXPECT_FALSE(MakeBlockMemRateContract(384, kDefaultBlockResourceScale, "clock", "memory").enabled);
    EXPECT_FALSE(MakeBlockMemRateContract(kDefaultBlockMemoryMb, 1, "clock", "memory").enabled);
}
}  // namespace st
}  // namespace datasystem
