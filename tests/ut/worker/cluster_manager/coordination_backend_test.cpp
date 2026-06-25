#include "datasystem/topology/coordination_backend/etcd_coordination_backend.h"

#include <gtest/gtest.h>

#include "etcd/api/mvccpb/kv.pb.h"

namespace datasystem {
namespace {
TEST(CoordinationBackendTest, ConvertsEtcdPutEventToNeutralEvent)
{
    mvccpb::Event etcdEvent;
    etcdEvent.set_type(mvccpb::Event_EventType_PUT);
    etcdEvent.mutable_kv()->set_key("/datasystem/cluster/127.0.0.1:31501");
    etcdEvent.mutable_kv()->set_value("100;start");

    auto event = topology::EtcdCoordinationBackend::FromEtcdEvent(etcdEvent);

    EXPECT_EQ(event.type, topology::CoordinationEventType::PUT);
    EXPECT_EQ(event.key, "/datasystem/cluster/127.0.0.1:31501");
    EXPECT_EQ(event.value, "100;start");
}

TEST(CoordinationBackendTest, ConvertsEtcdDeleteEventToNeutralEvent)
{
    mvccpb::Event etcdEvent;
    etcdEvent.set_type(mvccpb::Event_EventType_DELETE);
    etcdEvent.mutable_kv()->set_key("/datasystem/cluster/127.0.0.1:31501");

    auto event = topology::EtcdCoordinationBackend::FromEtcdEvent(etcdEvent);

    EXPECT_EQ(event.type, topology::CoordinationEventType::DELETE);
    EXPECT_EQ(event.key, "/datasystem/cluster/127.0.0.1:31501");
    EXPECT_TRUE(event.value.empty());
}
}  // namespace
}  // namespace datasystem
