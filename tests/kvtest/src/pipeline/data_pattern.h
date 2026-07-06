#pragma once
#include <cstdint>
#include <string>

inline std::string GeneratePatternData(uint64_t size, int senderId) {
    std::string data(size, '\0');
    for (uint64_t i = 0; i < size; i++) {
        data[i] = static_cast<char>((senderId + i) % 256);
    }
    return data;
}

// Verify data matches the pattern without allocating memory
inline bool VerifyPatternData(const char *data, uint64_t size, int senderId) {
    for (uint64_t i = 0; i < size; i++) {
        if (data[i] != static_cast<char>((senderId + i) % 256)) return false;
    }
    return true;
}

// ---- Configurable data verification for get paths ----

// Expected pattern byte at offset i for a given senderId.
inline char PatternByteAt(uint64_t i, int senderId) {
    return static_cast<char>((senderId + i) % 256);
}

enum class VerifyLevel { OFF = 0, SIZE = 1, SAMPLE = 2, FULL = 3 };

enum class VerifyFailReason { NONE = 0, SIZE = 1, CONTENT = 2 };

struct VerifyConfig {
    VerifyLevel level = VerifyLevel::SIZE;
    uint64_t sampleBytes = 4096;              // per-segment sample length
    uint64_t sampleStepBytes = 1024 * 1024;   // distance between sample segment starts
    bool failOp = false;                      // true => verify failure fails the op
};

// Build a VerifyConfig from raw config scalars. Unknown level strings fall
// back to SIZE (defensive; config.cpp rejects unknown levels at load time).
inline VerifyConfig BuildVerifyConfig(const std::string &level,
                                      uint64_t sampleBytes,
                                      uint64_t sampleStepBytes,
                                      bool failOp) {
    VerifyConfig vc;
    if (level == "off")          vc.level = VerifyLevel::OFF;
    else if (level == "size")    vc.level = VerifyLevel::SIZE;
    else if (level == "sample")  vc.level = VerifyLevel::SAMPLE;
    else if (level == "full")    vc.level = VerifyLevel::FULL;
    vc.sampleBytes = sampleBytes ? sampleBytes : 4096;
    vc.sampleStepBytes = sampleStepBytes ? sampleStepBytes : (1024ULL * 1024);
    vc.failOp = failOp;
    return vc;
}

// Sampled content check: head segment, evenly-spaced middle segments, tail
// segment. Returns true iff all sampled bytes match the pattern. Pure, no
// allocation. For size==0 returns true.
inline bool VerifySamplePattern(const char *buf, uint64_t size, int senderId,
                                uint64_t sampleBytes, uint64_t sampleStepBytes) {
    if (size == 0) return true;
    uint64_t segLen = sampleBytes ? sampleBytes : 1;
    if (segLen > size) segLen = size;
    uint64_t step = sampleStepBytes ? sampleStepBytes : 1;

    auto checkSeg = [&](uint64_t off, uint64_t len) -> bool {
        for (uint64_t i = 0; i < len; i++) {
            if (buf[off + i] != PatternByteAt(off + i, senderId)) return false;
        }
        return true;
    };

    // Head
    if (!checkSeg(0, segLen)) return false;
    if (size <= segLen) return true;  // head covered everything

    // Tail (skip if it would overlap the head segment)
    uint64_t tailOff = size - segLen;
    if (tailOff >= segLen) {
        if (!checkSeg(tailOff, segLen)) return false;
    }

    // Middle samples strictly between head and tail
    for (uint64_t off = step; off + segLen <= tailOff; off += step) {
        if (!checkSeg(off, segLen)) return false;
    }
    return true;
}

// Verify a get-returned buffer against the deterministic pattern. Pure: no
// logging, no atomic side effects — caller owns observability.
//   - OFF:    always true
//   - SIZE:   true iff bufSize == expectedSize
//   - SAMPLE: SIZE + head/tail/middle sampled segments match
//   - FULL:   SIZE + full content match (VerifyPatternData)
// On failure, sets *reason (if non-null) to SIZE or CONTENT.
inline bool VerifyBuffer(const void *bufData, uint64_t bufSize,
                         uint64_t expectedSize, int senderId,
                         const VerifyConfig &cfg,
                         VerifyFailReason *reason = nullptr) {
    if (cfg.level == VerifyLevel::OFF) {
        if (reason) *reason = VerifyFailReason::NONE;
        return true;
    }
    if (bufSize != expectedSize) {
        if (reason) *reason = VerifyFailReason::SIZE;
        return false;
    }
    if (cfg.level == VerifyLevel::SIZE) {
        if (reason) *reason = VerifyFailReason::NONE;
        return true;
    }
    const char *buf = static_cast<const char *>(bufData);
    bool ok;
    if (cfg.level == VerifyLevel::FULL) {
        ok = VerifyPatternData(buf, bufSize, senderId);
    } else {
        ok = VerifySamplePattern(buf, bufSize, senderId, cfg.sampleBytes, cfg.sampleStepBytes);
    }
    if (reason) *reason = ok ? VerifyFailReason::NONE : VerifyFailReason::CONTENT;
    return ok;
}
