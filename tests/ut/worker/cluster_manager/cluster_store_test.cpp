#include "datasystem/worker/cluster_manager/cluster_store.h"

#include <gtest/gtest.h>

#include "etcd/api/mvccpb/kv.pb.h"

namespace datasystem {
namespace {
TEST(ClusterStoreTest, ConvertsEtcdPutEventToNeutralEvent)
{
    mvccpb::Event etcdEvent;
    etcdEvent.set_type(mvccpb::Event_EventType_PUT);
    etcdEvent.mutable_kv()->set_key("/datasystem/cluster/127.0.0.1:31501");
    etcdEvent.mutable_kv()->set_value("100;start");

    auto event = EtcdClusterStore::FromEtcdEvent(etcdEvent);

    EXPECT_EQ(event.type, ClusterStoreEventType::PUT);
    EXPECT_EQ(event.key, "/datasystem/cluster/127.0.0.1:31501");
    EXPECT_EQ(event.value, "100;start");
}

TEST(ClusterStoreTest, ConvertsEtcdDeleteEventToNeutralEvent)
{
    mvccpb::Event etcdEvent;
    etcdEvent.set_type(mvccpb::Event_EventType_DELETE);
    etcdEvent.mutable_kv()->set_key("/datasystem/cluster/127.0.0.1:31501");

    auto event = EtcdClusterStore::FromEtcdEvent(etcdEvent);

    EXPECT_EQ(event.type, ClusterStoreEventType::DELETE);
    EXPECT_EQ(event.key, "/datasystem/cluster/127.0.0.1:31501");
    EXPECT_TRUE(event.value.empty());
}
}  // namespace
}  // namespace datasystem
