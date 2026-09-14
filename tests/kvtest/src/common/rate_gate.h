#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <random>

namespace kvtest {

inline int64_t SteadyNowUs()
{
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

inline std::chrono::steady_clock::time_point SteadyFromUs(int64_t us)
{
    return std::chrono::steady_clock::time_point(std::chrono::microseconds(us));
}

// Spreads target QPS across a thread pool and paces each thread on an absolute
// slot grid. Two layers do the spreading: every thread owns the lane
// [threadId * laneWidth, (threadId + 1) * laneWidth) inside each slot, where
// laneWidth = interval / numThreads (layer 1), and the random offset is drawn
// only inside that lane (layer 2). Because the jitter cannot leave the lane, N
// threads cover one interval exactly once each instead of all jittering across
// the full interval.
//
// A slot whose jittered instant has already elapsed is dropped rather than
// deferred: the grid moves on without emitting the late request, so a thread
// that fell behind never fires a catch-up burst and the offered rate can never
// exceed the configured QPS.
class RateGate {
public:
    void Configure(int64_t intervalUs, int threadId, int numThreads, bool enableJitter)
    {
        intervalUs_ = intervalUs;
        if (intervalUs <= 0) {
            laneOffsetUs_ = 0;
            jitterDist_ = std::uniform_int_distribution<int64_t>(0, 0);
            return;
        }
        int64_t laneWidthUs = std::max<int64_t>(1, intervalUs / std::max(1, numThreads));
        laneOffsetUs_ = threadId * laneWidthUs;
        jitterDist_ = std::uniform_int_distribution<int64_t>(
            0, enableJitter ? laneWidthUs - 1 : 0);
    }

    // Anchors the slot grid at nowUs. Call on thread start and whenever the
    // interval changes, so the grid is never re-anchored to a lagging request.
    void ResetGrid(int64_t nowUs) { nextSlotUs_ = nowUs; }

    bool Active() const { return intervalUs_ > 0; }

    // Returns the fire time of the first slot whose jittered instant is still
    // ahead of nowUs, skipping any slot that already elapsed.
    int64_t NextFireUs(int64_t nowUs, std::mt19937 &rng)
    {
        if (intervalUs_ <= 0) return nowUs;
        for (;;) {
            int64_t fireUs = nextSlotUs_ + laneOffsetUs_ + jitterDist_(rng);
            if (fireUs > nowUs) return fireUs;
            nextSlotUs_ += intervalUs_;
        }
    }

    // Moves the grid past the slot that was just emitted.
    void Advance()
    {
        if (intervalUs_ > 0) nextSlotUs_ += intervalUs_;
    }

private:
    int64_t intervalUs_ = 0;
    int64_t laneOffsetUs_ = 0;
    int64_t nextSlotUs_ = 0;
    std::uniform_int_distribution<int64_t> jitterDist_{0, 0};
};

}  // namespace kvtest
