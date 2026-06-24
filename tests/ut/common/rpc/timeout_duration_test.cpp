#include <gtest/gtest.h>

#include "datasystem/common/rpc/timeout_duration.h"

namespace datasystem {

TEST(TimeoutDurationTest, ScaleTimeoutMsRoundsSmallTimeoutsByMicroseconds)
{
    EXPECT_EQ(TimeoutDuration::ScaleTimeoutMs(10, 0.9), 9);
    EXPECT_EQ(TimeoutDuration::ScaleTimeoutMs(9, 0.8), 7);
    EXPECT_EQ(TimeoutDuration::ScaleTimeoutMs(5, 0.9), 5);
    EXPECT_EQ(TimeoutDuration::ScaleTimeoutMs(4, 0.8), 3);
    EXPECT_EQ(TimeoutDuration::ScaleTimeoutMs(3, 0.9), 3);
    EXPECT_EQ(TimeoutDuration::ScaleTimeoutMs(2, 0.8), 2);
    EXPECT_EQ(TimeoutDuration::ScaleTimeoutMs(1, 0.8), 1);
    EXPECT_EQ(TimeoutDuration::ScaleTimeoutMs(1, 0.9), 1);
}

TEST(TimeoutDurationTest, ScaleTimeoutMsKeepsLargeTimeoutBehavior)
{
    EXPECT_EQ(TimeoutDuration::ScaleTimeoutMs(1'000, 0.8), 800);
    EXPECT_EQ(TimeoutDuration::ScaleTimeoutMs(60'000, 0.9), 54'000);
}

TEST(TimeoutDurationTest, ScaleTimeoutMsKeepsNonPositiveTimeout)
{
    EXPECT_EQ(TimeoutDuration::ScaleTimeoutMs(0, 0.8), 0);
    EXPECT_EQ(TimeoutDuration::ScaleTimeoutMs(-1, 0.8), -1);
    EXPECT_EQ(TimeoutDuration::ScaleTimeoutMs(-10, 0.8), -10);
}

TEST(TimeoutDurationTest, ConvertRemainingTimeUsToMsHandlesPositive)
{
    EXPECT_EQ(TimeoutDuration::ConvertRemainingTimeUsToMs(8'962), 9);
    EXPECT_EQ(TimeoutDuration::ConvertRemainingTimeUsToMs(7'824), 8);
    EXPECT_EQ(TimeoutDuration::ConvertRemainingTimeUsToMs(1'499), 1);
    EXPECT_EQ(TimeoutDuration::ConvertRemainingTimeUsToMs(1'500), 2);
    EXPECT_EQ(TimeoutDuration::ConvertRemainingTimeUsToMs(1), 1);
    EXPECT_EQ(TimeoutDuration::ConvertRemainingTimeUsToMs(499), 1);
    EXPECT_EQ(TimeoutDuration::ConvertRemainingTimeUsToMs(10'001), 10);
    EXPECT_EQ(TimeoutDuration::ConvertRemainingTimeUsToMs(60'999), 60);
}

TEST(TimeoutDurationTest, ConvertRemainingTimeUsToMsHandlesNonPositiveAndLarge)
{
    EXPECT_EQ(TimeoutDuration::ConvertRemainingTimeUsToMs(0), 0);
    EXPECT_EQ(TimeoutDuration::ConvertRemainingTimeUsToMs(-1), 0);
    EXPECT_EQ(TimeoutDuration::ConvertRemainingTimeUsToMs(-1'000), -1);
}

TEST(TimeoutDurationTest, CalcRemainingTimeUsesScaledDefaultWhenNotInitialized)
{
    TimeoutDuration timeoutDuration(5);
    EXPECT_EQ(timeoutDuration.CalcRealRemainingTime(), 5);
    EXPECT_EQ(timeoutDuration.CalcRemainingTime(), 4);
}

TEST(TimeoutDurationTest, InitUsSetsMicrosecondDeadline)
{
    TimeoutDuration td;
    td.InitUs(5000);
    int64_t remainingUs = td.CalcRealRemainingTimeUs();
    EXPECT_GE(remainingUs, 3000);
    EXPECT_LE(remainingUs, 5100);
}

TEST(TimeoutDurationTest, InitWithPositiveTimeUsFallsBackOnNonPositive)
{
    TimeoutDuration td;
    td.InitWithPositiveTimeUs(0);
    EXPECT_GT(td.CalcRealRemainingTimeUs(), 0);
    td.InitWithPositiveTimeUs(-100);
    EXPECT_GT(td.CalcRealRemainingTimeUs(), 0);
}

TEST(TimeoutDurationTest, CalcRemainingTimeUsShortTimeoutReturnsRealRemaining)
{
    TimeoutDuration td;
    td.InitUs(3 * 1000);
    int64_t remainingUs = td.CalcRemainingTimeUs();
    int64_t realRemainingUs = td.CalcRealRemainingTimeUs();
    EXPECT_EQ(remainingUs, realRemainingUs);
}

TEST(TimeoutDurationTest, CalcRemainingTimeUsLargeTimeoutAppliesDecay)
{
    TimeoutDuration td;
    td.InitUs(60 * 1000 * 1000);
    int64_t remainingUs = td.CalcRemainingTimeUs();
    int64_t realRemainingUs = td.CalcRealRemainingTimeUs();
    EXPECT_LT(remainingUs, realRemainingUs);
    EXPECT_GE(remainingUs, realRemainingUs * 0.79);
    EXPECT_LE(remainingUs, realRemainingUs * 0.81);
}

TEST(TimeoutDurationTest, CalcRemainingAfterDeductionUsReturnsZeroWhenExhausted)
{
    NetworkLatencyEstimator::Instance().Reset();
    TimeoutDuration td;
    td.InitUs(100000);
    int64_t remaining = td.CalcRemainingAfterDeductionUs();
    EXPECT_GT(remaining, 0);
    NetworkLatencyEstimator::Instance().Update(100);
    int64_t afterDeduct = td.CalcRemainingAfterDeductionUs();
    EXPECT_LE(afterDeduct, remaining);
    td.InitUs(50);
    EXPECT_EQ(td.CalcRemainingAfterDeductionUs(), 0);
    NetworkLatencyEstimator::Instance().Reset();
}

TEST(TimeoutDurationTest, CeilUsToMsBasic)
{
    EXPECT_EQ(TimeoutDuration::CeilUsToMs(400), 1);
    EXPECT_EQ(TimeoutDuration::CeilUsToMs(1001), 2);
    EXPECT_EQ(TimeoutDuration::CeilUsToMs(0), 0);
    EXPECT_EQ(TimeoutDuration::CeilUsToMs(-1), 0);
    EXPECT_EQ(TimeoutDuration::CeilUsToMs(-1000), -1);
    EXPECT_EQ(TimeoutDuration::CeilUsToMs(1000), 1);
    EXPECT_EQ(TimeoutDuration::CeilUsToMs(999), 1);
    EXPECT_EQ(TimeoutDuration::CeilUsToMs(1001), 2);
}

TEST(TimeoutDurationTest, ScaleTimeoutUsBasic)
{
    EXPECT_EQ(TimeoutDuration::ScaleTimeoutUs(10000, 0.8), 8000);
    EXPECT_EQ(TimeoutDuration::ScaleTimeoutUs(0, 0.8), 0);
    EXPECT_EQ(TimeoutDuration::ScaleTimeoutUs(-1, 0.8), -1);
}

TEST(TimeoutDurationTest, PublicConstants)
{
    EXPECT_EQ(TimeoutDuration::SHORT_TIMEOUT_THRESHOLD_US, 10000);
    EXPECT_EQ(TimeoutDuration::SLOW_PATH_LOG_THRESHOLD_US, 3000);
}

}  // namespace datasystem
