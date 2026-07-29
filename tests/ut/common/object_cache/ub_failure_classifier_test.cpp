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

#include <gtest/gtest.h>

#include "datasystem/common/object_cache/ub_failure_classifier.h"

namespace datasystem {
namespace ut {
namespace {
const HostPort PEER("127.0.0.1", 18480);
}

TEST(UbFailureClassifierTest, ClassifiesExplicitError4AsPortUnavailable)
{
    UbFailureClassifier classifier;
    UbOpOutcome outcome{ PEER, UbOperationKind::CLIENT_PUT, Status(K_URMA_ERROR, "post send failed") };
    outcome.providerStatus = 4;
    outcome.learnedFrom = "local_completion";

    EXPECT_EQ(classifier.Classify(outcome), UbFailureClass::PORT_UNAVAILABLE_ERROR4);
}

TEST(UbFailureClassifierTest, ClassifiesWaitTimeoutAsSuspectOnly)
{
    UbFailureClassifier classifier;
    UbOpOutcome outcome{ PEER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                         Status(K_URMA_WAIT_TIMEOUT, "wait jfc timeout") };
    outcome.providerStatus = 9;
    outcome.learnedFrom = "local_completion";

    EXPECT_EQ(classifier.Classify(outcome), UbFailureClass::TIMEOUT_SUSPECT);
}

TEST(UbFailureClassifierTest, ClassifiesTryAgainAsLocalResourcePressure)
{
    UbFailureClassifier classifier;
    UbOpOutcome outcome{ PEER, UbOperationKind::CLIENT_PUT, Status(K_TRY_AGAIN, "send lane busy") };
    outcome.learnedFrom = "send_lane_pool";

    EXPECT_EQ(classifier.Classify(outcome), UbFailureClass::LOCAL_RESOURCE_PRESSURE);
}

TEST(UbFailureClassifierTest, RpcTimeoutIsSuspectWithoutSynthesizingError4)
{
    UbOpOutcome outcome(HostPort("127.0.0.1", 31501), UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                        Status(K_RPC_DEADLINE_EXCEEDED, "remote get timed out"));

    EXPECT_EQ(UbFailureClassifier().Classify(outcome), UbFailureClass::TIMEOUT_SUSPECT);
}

TEST(UbFailureClassifierTest, LegacyUrmaErrorWithoutRawEvidenceIsNotHardFailure)
{
    UbOpOutcome outcome(HostPort("127.0.0.1", 31501), UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                        Status(K_URMA_ERROR, "legacy provider error"));

    EXPECT_EQ(UbFailureClassifier().Classify(outcome), UbFailureClass::NON_UB_FAILURE);
}
}  // namespace ut
}  // namespace datasystem
