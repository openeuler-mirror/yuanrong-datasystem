#pragma once
#include <cstdint>
#include <chrono>
#include <optional>
#include <string>

inline std::string GeneratePatternData(uint64_t size, int senderId) {
    std::string data(size, '\0');
    for (uint64_t i = 0; i < size; i++) {
        data[i] = static_cast<char>((senderId + i) % 256);
    }
    return data;
}

// Cache update value: a sender-independent (global) pattern body with a
// version stamp in the first 8 bytes. The body is pattern[i] = (0 + i) % 256,
// i.e. independent of the writer, so ANY instance can content-verify it in the
// multi-writer shared-key model. version is little-endian; keys shorter than 8
// bytes keep the plain pattern (version not applied).
inline std::string GenerateVersionedData(uint64_t size, uint64_t version) {
    std::string data = GeneratePatternData(size, /*senderId=*/0);
    for (int i = 0; i < 8 && i < static_cast<int>(size); i++) {
        data[static_cast<size_t>(i)] = static_cast<char>((version >> (i * 8)) & 0xFF);
    }
    return data;
}

// Current wall-clock time in nanoseconds since epoch. Globally comparable
// across machines under NTP; used as the cache-update version stamp so read/
// write instances can order updates to the same key. Resolution is typically
// sub-microsecond, so two writes within the same tick may share a version.
inline uint64_t NowNanos() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
}

// Read the version stamp from the first 8 bytes of a value (little-endian),
// matching GenerateVersionedData. Returns 0 for buffers shorter than 8 bytes.
inline uint64_t ReadVersion(const void *data, uint64_t size) {
    if (size < 8) return 0;
    const unsigned char *p = static_cast<const unsigned char *>(data);
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) {
        v |= static_cast<uint64_t>(p[i]) << (i * 8);
    }
    return v;
}

// Verify data matches the pattern without allocating memory. skipPrefix bytes
// at the head (e.g. a version stamp) are not compared.
inline bool VerifyPatternData(const char *data, uint64_t size, int senderId,
                              uint64_t &missMatchPos, uint64_t skipPrefix = 0) {
    for (uint64_t i = skipPrefix; i < size; i++) {
        if (data[i] != static_cast<char>((senderId + i) % 256)) {
            missMatchPos = i;
            return false;
        }
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
    // Skip this many leading bytes in the CONTENT comparison (sample/full).
    // Cache mode values carry an 8-byte version stamp that is not part of the
    // deterministic pattern; the version is checked separately via ReadVersion.
    uint64_t versionPrefixBytes = 0;
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
                                uint64_t sampleBytes, uint64_t sampleStepBytes,
                                std::optional<uint64_t> &mismatchPos,
                                uint64_t skipPrefix = 0) {
    if (size <= skipPrefix) return true;  // only the version head (or empty)
    uint64_t bodySize = size - skipPrefix;
    uint64_t segLen = sampleBytes ? sampleBytes : 1;
    if (segLen > bodySize) segLen = bodySize;
    uint64_t step = sampleStepBytes ? sampleStepBytes : 1;

    auto checkSeg = [&](uint64_t off, uint64_t len) -> bool {
        for (uint64_t i = 0; i < len; i++) {
            if (buf[off + i] != PatternByteAt(off + i, senderId)) {
                mismatchPos = off + i;
                return false;
            }
        }
        return true;
    };

    // Head (starts after the version prefix)
    if (!checkSeg(skipPrefix, segLen)) return false;
    if (bodySize <= segLen) return true;  // head covered the whole body

    // Tail (last segLen bytes; skip if it would overlap the head segment)
    uint64_t tailOff = size - segLen;
    if (tailOff >= skipPrefix + segLen) {
        if (!checkSeg(tailOff, segLen)) return false;
    }

    // Middle samples strictly between head and tail
    for (uint64_t off = skipPrefix + step; off + segLen <= tailOff; off += step) {
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
// On failure, sets *reason (if non-null) to SIZE or CONTENT. Sets mismatchPos only for CONTENT failures.
inline bool VerifyBuffer(const void *bufData, uint64_t bufSize,
                         uint64_t expectedSize, int senderId,
                         const VerifyConfig &cfg,
                         VerifyFailReason *reason = nullptr,
                         std::optional<uint64_t> *mismatchPos = nullptr) {
    if (mismatchPos) mismatchPos->reset();
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
        uint64_t pos = 0;
        ok = VerifyPatternData(buf, bufSize, senderId, pos, cfg.versionPrefixBytes);
        if (!ok && mismatchPos) *mismatchPos = pos;
    } else {
        std::optional<uint64_t> sampleMismatchPos;
        ok = VerifySamplePattern(buf, bufSize, senderId, cfg.sampleBytes, cfg.sampleStepBytes,
                                 sampleMismatchPos, cfg.versionPrefixBytes);
        if (!ok && mismatchPos) *mismatchPos = sampleMismatchPos;
    }
    if (reason) *reason = ok ? VerifyFailReason::NONE : VerifyFailReason::CONTENT;
    return ok;
}
