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
 * Description: Verify AccessTransportTracker without USE_URMA / hardware.
 */
#include "datasystem/common/log/access_recorder.h"

#include <atomic>
#include <string>
#include <thread>

#include "ut/common.h"

namespace datasystem {
namespace ut {
class AccessTransportTrackerTest : public CommonTest {};

TEST_F(AccessTransportTrackerTest, DefaultIsShmAfterReset)
{
    AccessTransportTracker::Reset();
    EXPECT_EQ(AccessTransportTracker::ToString(), "SHM");
}

TEST_F(AccessTransportTrackerTest, RecordUbThenTcpSimulatesFallback)
{
    AccessTransportTracker::Reset();
    AccessTransportTracker::Record(AccessTransportKind::UB);
    EXPECT_EQ(AccessTransportTracker::ToString(), "UB");

    // UB send failed and Buffer recorded TCP fallback before Publish.
    AccessTransportTracker::Record(AccessTransportKind::TCP);
    EXPECT_EQ(AccessTransportTracker::ToString(), "TCP");
}

TEST_F(AccessTransportTrackerTest, ResetClearsPreviousTransport)
{
    AccessTransportTracker::Record(AccessTransportKind::TCP);
    AccessTransportTracker::Reset();
    EXPECT_EQ(AccessTransportTracker::ToString(), "SHM");
}

TEST_F(AccessTransportTrackerTest, ThreadLocalIsolation)
{
    AccessTransportTracker::Reset();

    std::atomic<bool> childReady{ false };
    std::string childTransport;
    std::thread child([&childReady, &childTransport]() {
        AccessTransportTracker::Reset();
        AccessTransportTracker::Record(AccessTransportKind::TCP);
        childTransport = AccessTransportTracker::ToString();
        childReady.store(true);
    });
    child.join();

    ASSERT_TRUE(childReady.load());
    EXPECT_EQ(childTransport, "TCP");
    EXPECT_EQ(AccessTransportTracker::ToString(), "SHM");
}
}  // namespace ut
}  // namespace datasystem
