#include "benchmark_runner.h"
#include <algorithm>
#include <cstdint>

int CalcKeysPerRound(int workerMemoryMb, uint64_t dataSize) {
    if (workerMemoryMb <= 0 || dataSize == 0) return 0;
    uint64_t memBytes = static_cast<uint64_t>(workerMemoryMb) * 1024 * 1024;
    uint64_t usable = memBytes * 80 / 100;  // 80% safety margin
    uint64_t keys64 = usable / dataSize;
    // Cap to avoid int overflow for 1B payloads with large worker memory.
    // 1M keys per round provides enough samples for robust percentile
    // calculations without making small-payload benchmarks take hours.
    constexpr uint64_t kMaxKeys = 1000000;
    if (keys64 > kMaxKeys) keys64 = kMaxKeys;
    return static_cast<int>(std::max(keys64, static_cast<uint64_t>(1)));
}

std::string MakeBenchKey(int instanceId, int round, int index) {
    return "bench_" + std::to_string(instanceId) + "_" + std::to_string(round) + "_" + std::to_string(index);
}

std::pair<int, int> ThreadKeyRange(int totalKeys, int numThreads, int threadId) {
    if (numThreads <= 0 || threadId >= numThreads) return {0, 0};
    int base = totalKeys / numThreads;
    int rem = totalKeys % numThreads;
    int start = threadId * base + std::min(threadId, rem);
    int count = base + (threadId < rem ? 1 : 0);
    return {start, count};
}
