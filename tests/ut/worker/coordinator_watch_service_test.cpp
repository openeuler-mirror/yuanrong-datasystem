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
 * Description: Unit tests for CoordinatorWatchService brpc adapter.
 */
#include <gtest/gtest.h>

#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/coordinator.brpc.pb.h"
#include "datasystem/protos/coordinator.service.rpc.pb.h"
#include "datasystem/worker/coordinator/coordinator_watch_service_impl.h"

namespace datasystem {
namespace ut {
namespace {

TEST(CoordinatorWatchServiceBrpcTest, AdapterConstruction)
{
    // Verify the brpc adapter is constructible and wraps the impl correctly.
    coordinator::CoordinatorWatchServiceImpl svc(HostPort("127.0.0.1", 0));
    coordinator::CoordinatorWatchServiceBrpcAdapter adapter(svc);

    // The adapter inherits google::protobuf::Service and can be passed to AddBrpcService().
    google::protobuf::Service *service = &adapter;
    ASSERT_NE(service, nullptr);
    ASSERT_NE(service->GetDescriptor(), nullptr);
}

TEST(CoordinatorWatchServiceBrpcTest, HandleEventReturnsNotReadyWithoutClusterManager)
{
    coordinator::CoordinatorWatchServiceImpl svc(HostPort("127.0.0.1", 0));
    coordinator::CoordinatorWatchServiceBrpcAdapter adapter(svc);

    // Dispatch HandleEvent through DirectCallMethod (bypasses brpc callback mechanism).
    const auto *method = adapter.GetDescriptor()->FindMethodByName("HandleEvent");
    ASSERT_NE(method, nullptr);

    coordinator::EventReqPb req;
    coordinator::EventRspPb rsp;
    ::datasystem::Status st = adapter.DirectCallMethod(method, nullptr, &req, &rsp, nullptr);
    ASSERT_TRUE(st.IsError());
    ASSERT_EQ(st.GetCode(), StatusCode::K_NOT_READY);
}

}  // namespace
}  // namespace ut
}  // namespace datasystem
