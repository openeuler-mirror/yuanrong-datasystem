#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include "datasystem/common/rpc/network_latency_estimator.h"

namespace datasystem {

TEST(NetworkLatencyEstimatorTest, GetDeductUsReturnsZeroBeforeUpdate)
{
    NetworkLatencyEstimator::Instance().Reset();
    EXPECT_EQ(NetworkLatencyEstimator::Instance().GetDeductUs(), 0);
}

TEST(NetworkLatencyEstimatorTest, UpdateNormalSample)
{
    NetworkLatencyEstimator::Instance().Reset();
    NetworkLatencyEstimator::Instance().Update(100);
    EXPECT_EQ(NetworkLatencyEstimator::Instance().GetDeductUs(), 100);
}

TEST(NetworkLatencyEstimatorTest, UpdateFiltersInvalidSamples)
{
    NetworkLatencyEstimator::Instance().Reset();
    NetworkLatencyEstimator::Instance().Update(100);
    NetworkLatencyEstimator::Instance().Update(-50);
    EXPECT_EQ(NetworkLatencyEstimator::Instance().GetDeductUs(), 100);
    NetworkLatencyEstimator::Instance().Update(0);
    EXPECT_EQ(NetworkLatencyEstimator::Instance().GetDeductUs(), 100);
    NetworkLatencyEstimator::Instance().Reset();
}

TEST(NetworkLatencyEstimatorTest, UpdateFiltersAbnormalLarge)
{
    NetworkLatencyEstimator::Instance().Reset();
    NetworkLatencyEstimator::Instance().Update(100);
    NetworkLatencyEstimator::Instance().Update(600000);
    EXPECT_EQ(NetworkLatencyEstimator::Instance().GetDeductUs(), 100);
}

TEST(NetworkLatencyEstimatorTest, DeductCapsAt200)
{
    NetworkLatencyEstimator::Instance().Reset();
    NetworkLatencyEstimator::Instance().Update(300);
    EXPECT_EQ(NetworkLatencyEstimator::Instance().GetDeductUs(), 200);
}

TEST(NetworkLatencyEstimatorTest, EwmaBlendsSamples)
{
    NetworkLatencyEstimator::Instance().Reset();
    NetworkLatencyEstimator::Instance().Update(100);
    EXPECT_EQ(NetworkLatencyEstimator::Instance().GetDeductUs(), 100);
    NetworkLatencyEstimator::Instance().Update(200);
    int64_t expected = static_cast<int64_t>(0.2 * 200 + 0.8 * 100);
    EXPECT_EQ(NetworkLatencyEstimator::Instance().GetDeductUs(), expected);
}

TEST(NetworkLatencyEstimatorTest, ResetClearsEwma)
{
    NetworkLatencyEstimator::Instance().Reset();
    NetworkLatencyEstimator::Instance().Update(100);
    EXPECT_EQ(NetworkLatencyEstimator::Instance().GetDeductUs(), 100);
    NetworkLatencyEstimator::Instance().Reset();
    EXPECT_EQ(NetworkLatencyEstimator::Instance().GetDeductUs(), 0);
}

TEST(NetworkLatencyEstimatorTest, ConcurrentUpdateNoLostUpdates)
{
    NetworkLatencyEstimator::Instance().Reset();
    const int numThreads = 8;
    const int updatesPerThread = 1000;
    std::vector<std::thread> threads;
    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([]() {
            for (int i = 0; i < updatesPerThread; ++i) {
                NetworkLatencyEstimator::Instance().Update(1000);
            }
        });
    }
    for (auto &th : threads) {
        th.join();
    }
    int64_t deduct = NetworkLatencyEstimator::Instance().GetDeductUs();
    EXPECT_GT(deduct, 0);
    NetworkLatencyEstimator::Instance().Reset();
}

}  // namespace datasystem
