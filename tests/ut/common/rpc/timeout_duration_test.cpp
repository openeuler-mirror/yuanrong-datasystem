#include <gtest/gtest.h>

#include "datasystem/common/rpc/timeout_duration.h"

namespace datasystem {

TEST(TimeoutDurationTest, ScaleTimeoutMsRoundsSmallTimeoutsByMicroseconds)
{
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

TEST(TimeoutDurationTest, CalcRemainingTimeUsesScaledDefaultWhenNotInitialized)
{
    TimeoutDuration timeoutDuration(5);
    EXPECT_EQ(timeoutDuration.CalcRealRemainingTime(), 5);
    EXPECT_EQ(timeoutDuration.CalcRemainingTime(), 4);
}

}  // namespace datasystem
