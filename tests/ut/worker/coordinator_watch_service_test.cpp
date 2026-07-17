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

#include <string>
#include <vector>

#include "datasystem/common/coordinator/watch_event.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/coordinator.brpc.pb.h"
#include "datasystem/protos/coordinator.service.rpc.pb.h"
#include "datasystem/worker/coordinator/coordinator_watch_service_impl.h"
#include "ut/common.h"

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
    req.set_coordinator_id(std::string(16, 'a'));
    req.set_watch_id(1);
    req.add_events()->set_type(coordinator::EventPb::RESET);
    coordinator::EventRspPb rsp;
    ::datasystem::Status st = adapter.DirectCallMethod(method, nullptr, &req, &rsp, nullptr);
    ASSERT_TRUE(st.IsError());
    ASSERT_EQ(st.GetCode(), StatusCode::K_NOT_READY);
}

TEST(CoordinatorWatchServiceBrpcTest, ValidatesWholeBatchBeforeDelivering)
{
    std::vector<cluster::CoordinationEvent> delivered;
    coordinator::CoordinatorWatchServiceImpl svc(
        HostPort("127.0.0.1", 0),
        [&delivered](const std::string &, int64_t, cluster::CoordinationEvent &&event) {
            delivered.emplace_back(std::move(event));
            return Status::OK();
        });
    coordinator::EventReqPb req;
    req.set_coordinator_id(std::string(16, 'a'));
    req.set_watch_id(7);
    auto *valid = req.add_events();
    valid->set_type(coordinator::EventPb::PUT);
    valid->mutable_kv()->set_key("/datasystem/c/topology");
    req.add_events()->set_type(coordinator::EventPb::EVENT_TYPE_UNSPECIFIED);
    coordinator::EventRspPb rsp;

    EXPECT_EQ(svc.HandleEvent(req, rsp).GetCode(), K_INVALID);
    EXPECT_TRUE(delivered.empty());
}

TEST(CoordinatorWatchServiceBrpcTest, DeliversIdentityBoundResetWithoutKey)
{
    std::string observedId;
    int64_t observedWatchId = 0;
    cluster::CoordinationEventType observedType = cluster::CoordinationEventType::UNSPECIFIED;
    coordinator::CoordinatorWatchServiceImpl svc(
        HostPort("127.0.0.1", 0),
        [&](const std::string &coordinatorId, int64_t watchId, cluster::CoordinationEvent &&event) {
            observedId = coordinatorId;
            observedWatchId = watchId;
            observedType = event.type;
            return Status::OK();
        });
    coordinator::EventReqPb req;
    req.set_coordinator_id(std::string(16, 'b'));
    req.set_watch_id(9);
    req.add_events()->set_type(coordinator::EventPb::RESET);
    coordinator::EventRspPb rsp;

    DS_ASSERT_OK(svc.HandleEvent(req, rsp));
    EXPECT_EQ(observedId, std::string(16, 'b'));
    EXPECT_EQ(observedWatchId, 9);
    EXPECT_EQ(observedType, cluster::CoordinationEventType::RESET);
}

TEST(CoordinatorWatchServiceBrpcTest, DeliversEveryValidatedBatchEventInOrder)
{
    std::vector<cluster::CoordinationEvent> delivered;
    coordinator::CoordinatorWatchServiceImpl svc(
        HostPort("127.0.0.1", 0),
        [&delivered](const std::string &, int64_t, cluster::CoordinationEvent &&event) {
            delivered.emplace_back(std::move(event));
            return Status::OK();
        });
    coordinator::EventReqPb req;
    req.set_coordinator_id(std::string(16, 'c'));
    req.set_watch_id(11);
    auto *put = req.add_events();
    put->set_type(coordinator::EventPb::PUT);
    put->mutable_kv()->set_key("/datasystem/c/topology");
    auto *deleted = req.add_events();
    deleted->set_type(coordinator::EventPb::DELETE);
    deleted->mutable_kv()->set_key("/datasystem/c/cluster/127.0.0.1:12001");
    req.add_events()->set_type(coordinator::EventPb::RESET);
    coordinator::EventRspPb rsp;

    DS_ASSERT_OK(svc.HandleEvent(req, rsp));
    ASSERT_EQ(delivered.size(), 3);
    EXPECT_EQ(delivered[0].type, cluster::CoordinationEventType::PUT);
    EXPECT_EQ(delivered[1].type, cluster::CoordinationEventType::DELETE);
    EXPECT_EQ(delivered[2].type, cluster::CoordinationEventType::RESET);
}

TEST(CoordinatorWatchServiceBrpcTest, RejectsOversizedBatchBeforeDelivery)
{
    size_t delivered = 0;
    coordinator::CoordinatorWatchServiceImpl svc(
        HostPort("127.0.0.1", 0),
        [&delivered](const std::string &, int64_t, cluster::CoordinationEvent &&) {
            ++delivered;
            return Status::OK();
        });
    coordinator::EventReqPb req;
    req.set_coordinator_id(std::string(16, 'd'));
    req.set_watch_id(13);
    for (size_t i = 0; i <= MAX_WATCH_EVENTS_PER_BATCH; ++i) {
        req.add_events()->set_type(coordinator::EventPb::RESET);
    }
    coordinator::EventRspPb rsp;

    EXPECT_EQ(svc.HandleEvent(req, rsp).GetCode(), K_INVALID);
    EXPECT_EQ(delivered, 0UL);
    req.clear_events();
    auto *event = req.add_events();
    event->set_type(coordinator::EventPb::PUT);
    event->mutable_kv()->set_key("/datasystem/c/topology");
    event->mutable_kv()->set_value(std::string(MAX_WATCH_EVENT_BATCH_BYTES, 'x'));
    EXPECT_EQ(svc.HandleEvent(req, rsp).GetCode(), K_INVALID);
    EXPECT_EQ(delivered, 0UL);
}

TEST(CoordinatorWatchServiceBrpcTest, PropagatesTemporaryRouteRejection)
{
    coordinator::CoordinatorWatchServiceImpl svc(
        HostPort("127.0.0.1", 0),
        [](const std::string &, int64_t, cluster::CoordinationEvent &&) {
            return Status(K_NOT_READY, "watch registration is in progress");
        });
    coordinator::EventReqPb req;
    req.set_coordinator_id(std::string(16, 'e'));
    req.set_watch_id(15);
    req.add_events()->set_type(coordinator::EventPb::RESET);
    coordinator::EventRspPb rsp;

    EXPECT_EQ(svc.HandleEvent(req, rsp).GetCode(), K_NOT_READY);
}

}  // namespace
}  // namespace ut
}  // namespace datasystem
