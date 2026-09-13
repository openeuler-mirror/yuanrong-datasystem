#include "test_harness.h"
#include "common/rate_gate.h"
#include <algorithm>
#include <random>
#include <utility>
#include <vector>

namespace {

int64_t SlotIndexOf(int64_t fireUs, int64_t originUs, int64_t intervalUs)
{
    return (fireUs - originUs) / intervalUs;
}

}  // namespace

// With jitter enabled every thread must stay inside its own lane, and with zero
// request latency exactly one slot must be consumed per iteration.
TEST(rate_gate_stays_in_lane) {
    const int64_t intervalUs = 4000;
    const int numThreads = 4;
    const int64_t laneWidthUs = intervalUs / numThreads;
    const int64_t originUs = 1000000;

    for (int tid = 0; tid < numThreads; tid++) {
        kvtest::RateGate gate;
        gate.Configure(intervalUs, tid, numThreads, /*enableJitter=*/true);
        gate.ResetGrid(originUs);
        std::mt19937 rng(1234 + tid);

        int64_t nowUs = originUs;
        for (int i = 0; i < 500; i++) {
            int64_t fireUs = gate.NextFireUs(nowUs, rng);
            ASSERT_EQ(SlotIndexOf(fireUs, originUs, intervalUs), i);

            int64_t withinSlotUs = fireUs - (originUs + i * intervalUs);
            ASSERT_TRUE(withinSlotUs >= tid * laneWidthUs);
            ASSERT_TRUE(withinSlotUs < (tid + 1) * laneWidthUs);

            nowUs = fireUs;
            gate.Advance();
        }
    }
}

// Jitter off must produce a deterministic comb on the slot grid with no
// residual microsecond of offset injected by the RNG.
TEST(rate_gate_jitter_off_is_exact) {
    kvtest::RateGate gate;
    gate.Configure(4000, 2, 4, /*enableJitter=*/false);
    gate.ResetGrid(0);
    std::mt19937 rng(7);

    ASSERT_EQ(gate.NextFireUs(0, rng), 2000);
    gate.Advance();
    ASSERT_EQ(gate.NextFireUs(2000, rng), 6000);
    gate.Advance();
    ASSERT_EQ(gate.NextFireUs(6000, rng), 10000);
}

// A thread that fell behind must not replay the slots it missed. Slots whose
// jittered instant already elapsed are dropped; the request lands in the first
// slot still ahead of now.
TEST(rate_gate_drops_missed_slots) {
    kvtest::RateGate gate;
    gate.Configure(4000, 1, 4, /*enableJitter=*/false);  // lane [1000, 2000)
    gate.ResetGrid(0);
    std::mt19937 rng(7);

    ASSERT_EQ(gate.NextFireUs(0, rng), 1000);
    gate.Advance();

    // now is 12ms in: slot 1 (fire 5000) and slot 2 (fire 9000) both elapsed.
    int64_t fireUs = gate.NextFireUs(12000, rng);
    ASSERT_EQ(fireUs, 13000);
}

// The offered rate is capped at the configured QPS: however slow the request
// path is, a slot is never emitted twice and emissions never exceed the number
// of slots elapsed.
TEST(rate_gate_caps_rate_under_latency) {
    const int64_t intervalUs = 4000;
    const int64_t workUs = 4500;  // slower than the interval -> slots must drop
    kvtest::RateGate gate;
    gate.Configure(intervalUs, 0, 1, /*enableJitter=*/true);
    gate.ResetGrid(0);
    std::mt19937 rng(99);

    const int64_t durationUs = 1000000;
    int64_t nowUs = 0;
    int emitted = 0;
    int64_t lastSlot = -1;

    while (nowUs < durationUs) {
        int64_t fireUs = gate.NextFireUs(nowUs, rng);
        int64_t slot = SlotIndexOf(fireUs, 0, intervalUs);
        ASSERT_TRUE(slot > lastSlot);
        lastSlot = slot;
        emitted++;
        nowUs = fireUs + workUs;
        gate.Advance();
    }

    ASSERT_TRUE(emitted <= durationUs / intervalUs);
    ASSERT_TRUE(emitted > 0);
}

// A thread id beyond the lane count must not produce an out-of-range
// distribution when the interval is shorter than the thread count.
TEST(rate_gate_degenerate_intervals) {
    kvtest::RateGate gate;
    gate.Configure(2, 15, 16, /*enableJitter=*/true);
    gate.ResetGrid(0);
    std::mt19937 rng(3);
    ASSERT_TRUE(gate.NextFireUs(0, rng) > 0);

    gate.Configure(1, 0, 0, /*enableJitter=*/true);  // numThreads guard
    gate.ResetGrid(0);
    ASSERT_TRUE(gate.NextFireUs(0, rng) > 0);
}

// interval <= 0 means "unlimited QPS": the gate is inactive and never delays.
TEST(rate_gate_inactive_when_unlimited) {
    kvtest::RateGate gate;
    gate.Configure(0, 0, 4, /*enableJitter=*/true);
    ASSERT_FALSE(gate.Active());
    std::mt19937 rng(5);
    ASSERT_EQ(gate.NextFireUs(4242, rng), 4242);
    gate.Advance();
    ASSERT_EQ(gate.NextFireUs(4242, rng), 4242);
}

// Across all threads the lanes must tile the interval exactly once, so the
// merged schedule covers every interval with one request per lane.
TEST(rate_gate_lanes_tile_the_interval) {
    const int64_t intervalUs = 4000;
    const int numThreads = 4;
    std::vector<std::pair<int64_t, int64_t>> covered;
    for (int tid = 0; tid < numThreads; tid++) {
        kvtest::RateGate gate;
        gate.Configure(intervalUs, tid, numThreads, /*enableJitter=*/true);
        gate.ResetGrid(0);
        std::mt19937 rng(11 + tid);
        int64_t lo = intervalUs, hi = -1;
        for (int i = 0; i < 200; i++) {
            // ready 1us before the slot boundary, so a zero jitter draw still
            // falls inside slot i instead of being dropped as missed
            int64_t t = gate.NextFireUs(i * intervalUs - 1, rng) - i * intervalUs;
            lo = std::min(lo, t);
            hi = std::max(hi, t);
            gate.Advance();
        }
        covered.emplace_back(lo, hi);
    }
    for (int tid = 1; tid < numThreads; tid++) {
        ASSERT_TRUE(covered[tid].first >= covered[tid - 1].second);
    }
    ASSERT_TRUE(covered.back().second < intervalUs);
}
