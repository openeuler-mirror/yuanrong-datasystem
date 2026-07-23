/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Unit tests for brpc Controller error → datasystem Status mapping.
 *
 * Verifies that transport-layer brpc failures (connection refused, timeout,
 * peer down, ...) are mapped to K_RPC_UNAVAILABLE / K_RPC_DEADLINE_EXCEEDED
 * instead of being masked as K_RPC_CANCELLED, and that the DS_ERR sentinel
 * path still recovers the server application error code.
 */

#include <errno.h>
#include <string>

#include <gtest/gtest.h>

#include "datasystem/common/rpc/brpc_status_util.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace test {

using StatusCode = datasystem::StatusCode;

// DS_ERR sentinel in error text takes priority over brpc errno mapping:
// the server ran application code and embedded its real error code.
namespace {
// Build a DS_ERR sentinel string. Use char(N) instead of \xNN literals because
// C++ hex escapes are greedy: "\x01DS_ERR" parses as \x01D (D is a hex digit)
// followed by S_ERR, corrupting the sentinel. char(1)/char(2) avoid this.
std::string DsErrSentinel(const std::string &code)
{
    return std::string(1, '\x01') + "DS_ERR:" + code + std::string(1, '\x02');
}
}  // namespace

// DS_ERR sentinel in error text takes priority over brpc errno mapping:
// the server ran application code and embedded its real error code.
TEST(BrpcStatusUtilTest, DsErrSentinelRecoverServerCode)
{
    std::string errText = "GetObj failed: " + DsErrSentinel("5");
    auto st = TryExtractStatusFromControllerError(errText, ECONNREFUSED);
    ASSERT_TRUE(st.IsError());
    EXPECT_EQ(st.GetCode(), static_cast<StatusCode>(5));
    // The original brpc ErrorText must be preserved so on-call keeps the
    // server-side application error context. The DS_ERR sentinel bytes are
    // left in the text (they are non-printing SOH/STX, harmless in logs);
    // what matters is the readable server-side message survives.
    EXPECT_NE(st.ToString().find("GetObj failed"), std::string::npos);
}

TEST(BrpcStatusUtilTest, DsErrSentinelUsesLastOccurrence)
{
    // TryExtractStatusFromControllerError uses rfind to take the LAST DS_ERR
    // sentinel in the error text. When multiple sentinels are present (a
    // server wraps a nested error), the last one wins.
    std::string errText = "outer " + DsErrSentinel("7") + " inner " + DsErrSentinel("5");
    auto st = TryExtractStatusFromControllerError(errText, ETIMEDOUT);
    EXPECT_EQ(st.GetCode(), static_cast<StatusCode>(5));
}

TEST(BrpcStatusUtilTest, CorruptSentinelFallsBackToErrnoMapping)
{
    // Non-numeric sentinel body → fall through to errno mapping.
    std::string errText = "fail " + DsErrSentinel("abc");
    auto st = TryExtractStatusFromControllerError(errText, ECONNREFUSED);
    EXPECT_EQ(st.GetCode(), StatusCode::K_RPC_UNAVAILABLE);
}

// Timeout mappings → K_RPC_DEADLINE_EXCEEDED
TEST(BrpcStatusUtilTest, BrpcErpcTimedOutMapsToDeadlineExceeded)
{
    auto st = TryExtractStatusFromControllerError("timed out", kBrpcErpcTimedOut);
    EXPECT_EQ(st.GetCode(), StatusCode::K_RPC_DEADLINE_EXCEEDED);
}

TEST(BrpcStatusUtilTest, SystemETimedoutMapsToDeadlineExceeded)
{
    auto st = TryExtractStatusFromControllerError("timed out", ETIMEDOUT);
    EXPECT_EQ(st.GetCode(), StatusCode::K_RPC_DEADLINE_EXCEEDED);
}

// Peer-unreachable mappings → K_RPC_UNAVAILABLE
TEST(BrpcStatusUtilTest, ConnectionRefusedWithoutAttemptDiagnosticRemainsAmbiguous)
{
    auto st = TryExtractStatusFromControllerError("refused", ECONNREFUSED);
    EXPECT_EQ(st.GetCode(), StatusCode::K_RPC_UNAVAILABLE);
    EXPECT_FALSE(IsBrpcRequestDefinitelyNotSent(st));
}

TEST(BrpcStatusUtilTest, ConnectionResetMapsToUnavailable)
{
    auto st = TryExtractStatusFromControllerError("reset", ECONNRESET);
    EXPECT_EQ(st.GetCode(), StatusCode::K_RPC_UNAVAILABLE);
    EXPECT_FALSE(IsBrpcRequestDefinitelyNotSent(st));
}

TEST(BrpcStatusUtilTest, HostUnreachableWithoutAttemptDiagnosticRemainsAmbiguous)
{
    auto st = TryExtractStatusFromControllerError("no route", EHOSTUNREACH);
    EXPECT_EQ(st.GetCode(), StatusCode::K_RPC_UNAVAILABLE);
    EXPECT_FALSE(IsBrpcRequestDefinitelyNotSent(st));
}

TEST(BrpcStatusUtilTest, AllConnectionEstablishmentRetriesAreDefinitelyNotSent)
{
    auto st = TryExtractStatusFromControllerError(
        "[E112]Not connected [R1][E111]Connection refused [R2][E113]No route", ECONNREFUSED);
    EXPECT_TRUE(IsBrpcRequestDefinitelyNotSent(st));
}

TEST(BrpcStatusUtilTest, AmbiguousEarlierRetryIsNotDefinitelyNotSent)
{
    auto st = TryExtractStatusFromControllerError(
        "[E104]Connection reset [R1][E111]Connection refused", ECONNREFUSED);
    EXPECT_FALSE(IsBrpcRequestDefinitelyNotSent(st));
}

TEST(BrpcStatusUtilTest, MalformedRetryDiagnosticIsNotDefinitelyNotSent)
{
    auto st = TryExtractStatusFromControllerError(
        "[E111]Connection refused [R1][Ebad]Malformed retry", ECONNREFUSED);
    EXPECT_FALSE(IsBrpcRequestDefinitelyNotSent(st));
}

TEST(BrpcStatusUtilTest, BrpcFailedSocketMapsToUnavailable)
{
    auto st = TryExtractStatusFromControllerError("socket dead", kBrpcFailedSocket);
    EXPECT_EQ(st.GetCode(), StatusCode::K_RPC_UNAVAILABLE);
}

TEST(BrpcStatusUtilTest, BrpcLogoffMapsToUnavailable)
{
    auto st = TryExtractStatusFromControllerError("server stop", kBrpcLogoff);
    EXPECT_EQ(st.GetCode(), StatusCode::K_RPC_UNAVAILABLE);
}

// Genuine cancellation stays K_RPC_CANCELLED
TEST(BrpcStatusUtilTest, EcanceledMapsToCancelled)
{
    auto st = TryExtractStatusFromControllerError("cancelled", ECANCELED);
    EXPECT_EQ(st.GetCode(), StatusCode::K_RPC_CANCELLED);
}

// Legacy callers that do not pass ErrorCode (default 0) keep prior behavior:
// K_RPC_CANCELLED fallback, so existing call sites compile and behave as before.
TEST(BrpcStatusUtilTest, ZeroErrorCodeFallsBackToCancelled)
{
    auto st = TryExtractStatusFromControllerError("unknown failure");
    EXPECT_EQ(st.GetCode(), StatusCode::K_RPC_CANCELLED);
}

// Unmapped errno → K_RPC_CANCELLED fallback, but message is no longer opaque.
TEST(BrpcStatusUtilTest, UnmappedErrnoFallsBackWithDiagnostic)
{
    auto st = TryExtractStatusFromControllerError("weird", /*errorCode=*/99999);
    EXPECT_EQ(st.GetCode(), StatusCode::K_RPC_CANCELLED);
    EXPECT_NE(st.ToString().find("99999"), std::string::npos);
}

// Mapped status carries the raw errno in the message for diagnosis.
TEST(BrpcStatusUtilTest, MappedStatusIncludesErrnoInMessage)
{
    auto st = TryExtractStatusFromControllerError("refused", ECONNREFUSED);
    EXPECT_EQ(st.GetCode(), StatusCode::K_RPC_UNAVAILABLE);
    EXPECT_NE(st.ToString().find(std::to_string(ECONNREFUSED)), std::string::npos);
}

}  // namespace test
}  // namespace datasystem
