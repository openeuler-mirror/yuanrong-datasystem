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

/** Description: Unit tests for local UB provider admission state. */

#include <gtest/gtest.h>

#include "datasystem/common/object_cache/peer_ub_admission.h"

namespace datasystem {
namespace {

const HostPort PEER("127.0.0.1", 31501);

TEST(PeerUbAdmissionTest, ExplicitError4BlocksProviderReadSource)
{
    PeerUbAdmission admission;
    UbOpOutcome outcome(PEER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                        Status(K_URMA_ERROR, "provider write failed"));
    outcome.cqeStatus = 4;

    admission.ReportOutcome(outcome);

    EXPECT_EQ(admission.CheckReadSource(PEER).GetCode(), K_URMA_DATA_WORKER_UNAVAILABLE);
    auto state = admission.GetState(PEER);
    ASSERT_TRUE(state.has_value());
    EXPECT_EQ(state->state, UbAdmissionState::UNAVAILABLE);
    EXPECT_EQ(state->lastFailureClass, UbFailureClass::PORT_UNAVAILABLE_ERROR4);
}

TEST(PeerUbAdmissionTest, RpcTimeoutIsSuspectAndDoesNotHardBlock)
{
    PeerUbAdmission admission;
    UbOpOutcome outcome(PEER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                        Status(K_RPC_DEADLINE_EXCEEDED, "remote get timed out"));

    admission.ReportOutcome(outcome);

    EXPECT_TRUE(admission.CheckReadSource(PEER).IsOk());
    auto state = admission.GetState(PEER);
    ASSERT_TRUE(state.has_value());
    EXPECT_EQ(state->state, UbAdmissionState::SUSPECT);
}

TEST(PeerUbAdmissionTest, LegacyUrmaErrorWithoutRawEvidenceDoesNotQuarantine)
{
    PeerUbAdmission admission;
    UbOpOutcome outcome(PEER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                        Status(K_URMA_ERROR, "legacy remote error"));

    admission.ReportOutcome(outcome);

    EXPECT_TRUE(admission.CheckReadSource(PEER).IsOk());
    EXPECT_FALSE(admission.GetState(PEER).has_value());
}

TEST(PeerUbAdmissionTest, ResourcePressureDoesNotQuarantine)
{
    PeerUbAdmission admission;
    UbOpOutcome outcome(PEER, UbOperationKind::CLIENT_GET_WRITEBACK,
                        Status(K_URMA_TRY_AGAIN, "send lane exhausted"));

    admission.ReportOutcome(outcome);

    EXPECT_TRUE(admission.CheckReadSource(PEER).IsOk());
    EXPECT_FALSE(admission.GetState(PEER).has_value());
}

}  // namespace
}  // namespace datasystem
