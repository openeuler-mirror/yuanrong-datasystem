#include <gtest/gtest.h>
#include <chrono>
#include <thread>
#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/rpc/timeout_duration.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/thread_local.h"

namespace datasystem {

TEST(ApiDeadlineTest, InitMsSetsDeadline)
{
    ApiDeadline::Instance().Init(5);
    int64_t remainingUs = ApiDeadline::Instance().ApiRemainingUs();
    EXPECT_GE(remainingUs, 3000);
    EXPECT_LE(remainingUs, 6000);
    ApiDeadline::Instance().Reset();
}

TEST(ApiDeadlineTest, InitUsSetsDeadline)
{
    ApiDeadline::Instance().InitUs(5000);
    int64_t remainingUs = ApiDeadline::Instance().ApiRemainingUs();
    EXPECT_GE(remainingUs, 3000);
    EXPECT_LE(remainingUs, 5100);
    ApiDeadline::Instance().Reset();
}

TEST(ApiDeadlineTest, ApiRemainingUsReturnsDefaultWhenNotInitialized)
{
    ApiDeadline::Instance().Reset();
    int64_t remainingUs = ApiDeadline::Instance().ApiRemainingUs();
    EXPECT_GT(remainingUs, 0);
}

TEST(ApiDeadlineTest, CheckApiDeadlineReturnsOkWhenTimeRemaining)
{
    ApiDeadline::Instance().InitUs(5 * 1000 * 1000);
    EXPECT_EQ(ApiDeadline::Instance().CheckApiDeadline(), Status::OK());
    ApiDeadline::Instance().Reset();
}

TEST(ApiDeadlineTest, CheckApiDeadlineReturnsExceededWhenExpired)
{
    ApiDeadline::Instance().InitUs(0);
    EXPECT_EQ(ApiDeadline::Instance().CheckApiDeadline().GetCode(), K_RPC_DEADLINE_EXCEEDED);
    ApiDeadline::Instance().Reset();
}

TEST(ApiDeadlineTest, PushPopRestoresDeadline)
{
    ApiDeadline::Instance().InitUs(10000);
    int64_t beforePush = ApiDeadline::Instance().ApiRemainingUs();
    ApiDeadline::Instance().Push();
    ApiDeadline::Instance().InitUs(5000);
    int64_t afterPush = ApiDeadline::Instance().ApiRemainingUs();
    EXPECT_LT(afterPush, beforePush);
    ApiDeadline::Instance().Pop();
    int64_t afterPop = ApiDeadline::Instance().ApiRemainingUs();
    EXPECT_GT(afterPop, afterPush);
    EXPECT_NEAR(afterPop, beforePush, 5000);
    ApiDeadline::Instance().Reset();
}

TEST(ApiDeadlineTest, PushPopPreservesUninitializedState)
{
    ApiDeadline::Instance().Reset();
    EXPECT_FALSE(ApiDeadline::Instance().IsInitialized());
    ApiDeadline::Instance().Push();
    ApiDeadline::Instance().InitUs(5000);
    EXPECT_TRUE(ApiDeadline::Instance().IsInitialized());
    ApiDeadline::Instance().Pop();
    EXPECT_FALSE(ApiDeadline::Instance().IsInitialized());
    ApiDeadline::Instance().Reset();
}

TEST(ApiDeadlineTest, ApiDeadlineGuardMsInitAndAutoPop)
{
    ApiDeadline::Instance().Reset();
    {
        ApiDeadlineGuard guard(5);
        EXPECT_TRUE(ApiDeadline::Instance().IsInitialized());
        int64_t remainingUs = ApiDeadline::Instance().ApiRemainingUs();
        EXPECT_GE(remainingUs, 3000);
    }
    EXPECT_FALSE(ApiDeadline::Instance().IsInitialized());
    ApiDeadline::Instance().Reset();
}

TEST(ApiDeadlineTest, ApiDeadlineGuardUsInUsInitAndAutoPop)
{
    ApiDeadline::Instance().Reset();
    {
        ApiDeadlineGuard guard(5000, InUs{});
        EXPECT_TRUE(ApiDeadline::Instance().IsInitialized());
        int64_t remainingUs = ApiDeadline::Instance().ApiRemainingUs();
        EXPECT_GE(remainingUs, 3000);
        EXPECT_LE(remainingUs, 6000);
    }
    EXPECT_FALSE(ApiDeadline::Instance().IsInitialized());
    ApiDeadline::Instance().Reset();
}

TEST(ApiDeadlineTest, PopOnEmptyStackLogsWarningAndDoesNotCrash)
{
    ApiDeadline::Instance().InitUs(10000);
    EXPECT_TRUE(ApiDeadline::Instance().IsInitialized());
    ApiDeadline::Instance().Pop();
    EXPECT_FALSE(ApiDeadline::Instance().IsInitialized());
    int64_t afterPop = ApiDeadline::Instance().ApiRemainingUs();
    EXPECT_GT(afterPop, 1000000);
    ApiDeadline::Instance().Reset();
}

TEST(ApiDeadlineTest, ResetClearsSavedStatesStack)
{
    ApiDeadline::Instance().Reset();
    ApiDeadline::Instance().InitUs(10000);
    ApiDeadline::Instance().Push();
    ApiDeadline::Instance().InitUs(5000);
    EXPECT_TRUE(ApiDeadline::Instance().IsInitialized());
    ApiDeadline::Instance().Reset();
    EXPECT_FALSE(ApiDeadline::Instance().IsInitialized());
    ApiDeadline::Instance().Pop();
    EXPECT_FALSE(ApiDeadline::Instance().IsInitialized());
    ApiDeadline::Instance().Reset();
}

TEST(ApiDeadlineTest, GuardSurvivesInnerReqTimeoutInitWithoutReset)
{
    // Regression guard for BatchRemoveMeta's empty-batchKeyVersions branch: an
    // outer deadline is active and an ApiDeadlineGuard has Push'd its state. The
    // inner scope only calls GetRequestContext()->reqTimeoutDuration.Init(RPC_TIMEOUT) for the RPC
    // budget and must NOT call ApiDeadline::Reset(). The guard's Pop() must
    // restore the outer deadline instead of hitting an empty saved stack.
    ApiDeadline::Instance().Reset();
    GetRequestContext()->reqTimeoutDuration.Reset();
    ApiDeadline::Instance().InitUs(10 * 1000 * 1000);  // 10s outer deadline
    EXPECT_TRUE(ApiDeadline::Instance().IsInitialized());
    {
        ApiDeadlineGuard guard(5 * 1000 * 1000, InUs{});  // Push outer state, install 5s budget
        EXPECT_TRUE(ApiDeadline::Instance().IsInitialized());
        GetRequestContext()->reqTimeoutDuration.Init(RPC_TIMEOUT);  // inner RPC budget only; no Reset()
        EXPECT_TRUE(ApiDeadline::Instance().IsInitialized());
    }
    // Pop() restored the saved 10s state. If Reset() had cleared savedStates_,
    // Pop() would warn and leave initialized_ false instead.
    EXPECT_TRUE(ApiDeadline::Instance().IsInitialized());
    int64_t remainingUs = ApiDeadline::Instance().ApiRemainingUs();
    EXPECT_GT(remainingUs, 0);
    EXPECT_LE(remainingUs, 10 * 1000 * 1000);
    ApiDeadline::Instance().Reset();
    GetRequestContext()->reqTimeoutDuration.Reset();
}

// Regression for issue #615: Set(buffer)'s UB data-plane write (Buffer::Publish, which
// runs before the Publish RPC) reads thread-local GetRequestContext()->reqTimeoutDuration. A prior RPC (e.g.
// Create) could leave it with an expired budget, making the UB write return
// K_URMA_WAIT_TIMEOUT. Set(buffer) must re-init reqTimeoutDuration from the fresh
// ApiDeadline at entry. Covers the <=10ms (no-decay) and >10ms (scaled) regimes.
TEST(ApiDeadlineTest, SetBufferReinitsReqTimeoutDurationBeforePreRpcDataPlane)
{
    ApiDeadline::Instance().Reset();
    GetRequestContext()->reqTimeoutDuration.Reset();
    GetRequestContext()->reqTimeoutDuration.InitUs(5'000);  // 5ms, as a prior RPC would
    std::this_thread::sleep_for(std::chrono::milliseconds(20));  // exceed the prior budget
    EXPECT_LE(GetRequestContext()->reqTimeoutDuration.CalcRemainingTime(), 0);  // stale read the UB write would hit
    {
        ApiDeadlineGuard guard(8);  // <=10ms regime: decay bypassed
        GetRequestContext()->reqTimeoutDuration.InitUs(ApiDeadline::Instance().ApiRemainingUs());
        EXPECT_GT(GetRequestContext()->reqTimeoutDuration.CalcRemainingTime(), 0);
    }
    {
        ApiDeadlineGuard guard(200);  // >10ms regime: decay applies
        GetRequestContext()->reqTimeoutDuration.InitUs(ApiDeadline::Instance().ApiRemainingUs());
        EXPECT_GT(GetRequestContext()->reqTimeoutDuration.CalcRemainingTime(), 0);
    }
    ApiDeadline::Instance().Reset();
    GetRequestContext()->reqTimeoutDuration.Reset();
}

TEST(InitTimeoutsFromDispatchTest, InitsBothTimeoutsWhenPositive)
{
    GetRequestContext()->reqTimeoutDuration.Reset();
    ApiDeadline::Instance().Reset();
    auto dispatchTime = std::chrono::steady_clock::now();
    auto rc = InitTimeoutsFromDispatch(10000, dispatchTime);
    EXPECT_EQ(rc, Status::OK());
    int64_t remainingUs = GetRequestContext()->reqTimeoutDuration.CalcRealRemainingTimeUs();
    EXPECT_GT(remainingUs, 0);
    EXPECT_GE(remainingUs, 8000);
    EXPECT_LE(remainingUs, 11000);
    EXPECT_TRUE(ApiDeadline::Instance().IsInitialized());
    GetRequestContext()->reqTimeoutDuration.Reset();
    ApiDeadline::Instance().Reset();
}

TEST(InitTimeoutsFromDispatchTest, ReturnsExceededWhenQueueDelayExhaustsBudget)
{
    GetRequestContext()->reqTimeoutDuration.Reset();
    ApiDeadline::Instance().Reset();
    auto oldTime = std::chrono::steady_clock::now() - std::chrono::hours(1);
    auto rc = InitTimeoutsFromDispatch(5000, oldTime);
    EXPECT_EQ(rc.GetCode(), K_RPC_DEADLINE_EXCEEDED);
    GetRequestContext()->reqTimeoutDuration.Reset();
    ApiDeadline::Instance().Reset();
}

TEST(InitTimeoutsFromDispatchTest, ReturnsExceededWhenZeroBudget)
{
    GetRequestContext()->reqTimeoutDuration.Reset();
    ApiDeadline::Instance().Reset();
    auto dispatchTime = std::chrono::steady_clock::now();
    auto rc = InitTimeoutsFromDispatch(0, dispatchTime);
    EXPECT_EQ(rc.GetCode(), K_RPC_DEADLINE_EXCEEDED);
    GetRequestContext()->reqTimeoutDuration.Reset();
    ApiDeadline::Instance().Reset();
}

TEST(InitTimeoutsFromDispatchTest, ReturnsExceededWhenNegativeBudget)
{
    GetRequestContext()->reqTimeoutDuration.Reset();
    ApiDeadline::Instance().Reset();
    auto dispatchTime = std::chrono::steady_clock::now();
    auto rc = InitTimeoutsFromDispatch(-5000, dispatchTime);
    EXPECT_EQ(rc.GetCode(), K_RPC_DEADLINE_EXCEEDED);
    GetRequestContext()->reqTimeoutDuration.Reset();
    ApiDeadline::Instance().Reset();
}

TEST(InitTimeoutsFromDispatchTest, ResetOnExhaustedBudgetClearsInitializedState)
{
    GetRequestContext()->reqTimeoutDuration.Reset();
    ApiDeadline::Instance().Reset();
    ApiDeadline::Instance().InitUs(5 * 1000 * 1000);
    EXPECT_TRUE(ApiDeadline::Instance().IsInitialized());
    auto dispatchTime = std::chrono::steady_clock::now();
    auto rc = InitTimeoutsFromDispatch(0, dispatchTime);
    EXPECT_EQ(rc.GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_FALSE(ApiDeadline::Instance().IsInitialized());
    EXPECT_GT(ApiDeadline::Instance().ApiRemainingUs(), 0);
    GetRequestContext()->reqTimeoutDuration.Reset();
    ApiDeadline::Instance().Reset();
}

TEST(ApiDeadlineTest, ApiDeadlineGuardZeroFallsBackToRpcTimeout)
{
    // timeoutMs <= 0 falls back to RPC_TIMEOUT; a WARNING is logged (not asserted here).
    ApiDeadline::Instance().Reset();
    {
        ApiDeadlineGuard guard(0);
        EXPECT_TRUE(ApiDeadline::Instance().IsInitialized());
        EXPECT_GT(ApiDeadline::Instance().ApiRemainingUs(), 0);
    }
    EXPECT_FALSE(ApiDeadline::Instance().IsInitialized());
    ApiDeadline::Instance().Reset();
}

TEST(ApiDeadlineTest, ApiDeadlineGuardNegativeFallsBackToRpcTimeout)
{
    // Negative timeoutMs also triggers the WARNING + RPC_TIMEOUT fallback branch.
    ApiDeadline::Instance().Reset();
    {
        ApiDeadlineGuard guard(-1);
        EXPECT_TRUE(ApiDeadline::Instance().IsInitialized());
        EXPECT_GT(ApiDeadline::Instance().ApiRemainingUs(), 0);
    }
    EXPECT_FALSE(ApiDeadline::Instance().IsInitialized());
    ApiDeadline::Instance().Reset();
}

TEST(GetRemainingUsForMetaTest, ReturnsApiDeadlineRemainingWhenInitialized)
{
    ApiDeadline::Instance().Reset();
    GetRequestContext()->reqTimeoutDuration.Reset();
    ApiDeadline::Instance().InitUs(5 * 1000 * 1000);  // 5s, not expired
    int64_t remaining = GetRemainingUsForMeta();
    EXPECT_GT(remaining, 0);
    EXPECT_LE(remaining, 5 * 1000 * 1000);
    ApiDeadline::Instance().Reset();
    GetRequestContext()->reqTimeoutDuration.Reset();
}

TEST(GetRemainingUsForMetaTest, ReturnsOneWhenApiDeadlineExpired)
{
    ApiDeadline::Instance().Reset();
    GetRequestContext()->reqTimeoutDuration.Reset();
    ApiDeadline::Instance().InitUs(0);  // deadline is now -> expired
    int64_t remaining = GetRemainingUsForMeta();
    EXPECT_EQ(remaining, 1);
    ApiDeadline::Instance().Reset();
    GetRequestContext()->reqTimeoutDuration.Reset();
}

TEST(GetRemainingUsForMetaTest, ReturnsReqTimeoutDurationRemainingWhenApiDeadlineUninitialized)
{
    ApiDeadline::Instance().Reset();
    GetRequestContext()->reqTimeoutDuration.Reset();
    GetRequestContext()->reqTimeoutDuration.InitUs(5 * 1000 * 1000);  // 5s
    int64_t remaining = GetRemainingUsForMeta();
    EXPECT_GT(remaining, 0);
    EXPECT_LE(remaining, 5 * 1000 * 1000);
    ApiDeadline::Instance().Reset();
    GetRequestContext()->reqTimeoutDuration.Reset();
}

TEST(GetRemainingUsForMetaTest, ReturnsRpcTimeoutWhenBothUninitialized)
{
    ApiDeadline::Instance().Reset();
    GetRequestContext()->reqTimeoutDuration.Reset();
    int64_t remaining = GetRemainingUsForMeta();
    EXPECT_EQ(remaining, static_cast<int64_t>(RPC_TIMEOUT) * ONE_THOUSAND);
    ApiDeadline::Instance().Reset();
    GetRequestContext()->reqTimeoutDuration.Reset();
}

TEST(InitTimeoutsFromDispatchTest, PushPopRestoresOuterDeadlineAfterReinit)
{
    GetRequestContext()->reqTimeoutDuration.Reset();
    ApiDeadline::Instance().Reset();
    ApiDeadline::Instance().InitUs(5 * 1000 * 1000);
    int64_t outerRemainingUs = ApiDeadline::Instance().ApiRemainingUs();
    EXPECT_GT(outerRemainingUs, 0);

    {
        ApiDeadline::Instance().Push();
        Raii deadlineRaii([]() { ApiDeadline::Instance().Pop(); });
        auto dispatchTime = std::chrono::steady_clock::now();
        auto rc = InitTimeoutsFromDispatch(10000, dispatchTime);
        EXPECT_EQ(rc, Status::OK());
        int64_t subTaskRemainingUs = ApiDeadline::Instance().ApiRemainingUs();
        EXPECT_GT(subTaskRemainingUs, 0);
        EXPECT_LT(subTaskRemainingUs, outerRemainingUs);
    }

    int64_t restoredRemainingUs = ApiDeadline::Instance().ApiRemainingUs();
    EXPECT_GT(restoredRemainingUs, 10000);
    EXPECT_NEAR(restoredRemainingUs, outerRemainingUs, 100000);
    GetRequestContext()->reqTimeoutDuration.Reset();
    ApiDeadline::Instance().Reset();
}

}  // namespace datasystem
