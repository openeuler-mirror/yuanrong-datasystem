#include "benchmark_runner.h"
#include <algorithm>
#include <cstdint>

int CalcKeysPerRound(int workerMemoryMb, uint64_t dataSize) {
    if (workerMemoryMb <= 0 || dataSize == 0) return 0;
    uint64_t memBytes = static_cast<uint64_t>(workerMemoryMb) * 1024 * 1024;
    uint64_t usable = memBytes * 80 / 100;  // 80% safety margin
    int keys = static_cast<int>(usable / dataSize);
    return std::max(keys, 1);
}

std::string MakeBenchKey(int round, int index) {
    return "bench_" + std::to_string(round) + "_" + std::to_string(index);
}

std::pair<int, int> ThreadKeyRange(int totalKeys, int numThreads, int threadId) {
    if (numThreads <= 0 || threadId >= numThreads) return {0, 0};
    int base = totalKeys / numThreads;
    int rem = totalKeys % numThreads;
    int start = threadId * base + std::min(threadId, rem);
    int count = base + (threadId < rem ? 1 : 0);
    return {start, count};
}
