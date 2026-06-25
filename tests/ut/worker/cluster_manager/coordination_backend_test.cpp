#include "datasystem/topology/coordination_backend/etcd_coordination_backend.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "etcd/api/mvccpb/kv.pb.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "tests/ut/common.h"

namespace datasystem {
namespace {
constexpr char TEST_ETCD_ENDPOINT[] = "localhost:1";
constexpr char TEST_WORKER_KEY[] = "/datasystem/cluster/test-worker:31501";

class StubEtcdStore final : public EtcdStore {
public:
    StubEtcdStore() : EtcdStore(TEST_ETCD_ENDPOINT)
    {
    }

    Status Get(const std::string &, const std::string &, std::string &value) override
    {
        value = "value";
        return Status::OK();
    }

    Status GetAll(const std::string &, std::vector<std::pair<std::string, std::string>> &outKeyValues) override
    {
        outKeyValues = { { "key", "value" } };
        return Status::OK();
    }

    Status Delete(const std::string &, const std::string &) override
    {
        return Status::OK();
    }
};

TEST(CoordinationBackendTest, ConvertsEtcdPutEventToNeutralEvent)
{
    mvccpb::Event etcdEvent;
    etcdEvent.set_type(mvccpb::Event_EventType_PUT);
    etcdEvent.mutable_kv()->set_key(TEST_WORKER_KEY);
    etcdEvent.mutable_kv()->set_value("100;start");

    auto event = topology::EtcdCoordinationBackend::FromEtcdEvent(etcdEvent);

    EXPECT_EQ(event.type, topology::CoordinationEventType::PUT);
    EXPECT_EQ(event.key, TEST_WORKER_KEY);
    EXPECT_EQ(event.value, "100;start");
}

TEST(CoordinationBackendTest, ConvertsEtcdDeleteEventToNeutralEvent)
{
    mvccpb::Event etcdEvent;
    etcdEvent.set_type(mvccpb::Event_EventType_DELETE);
    etcdEvent.mutable_kv()->set_key(TEST_WORKER_KEY);

    auto event = topology::EtcdCoordinationBackend::FromEtcdEvent(etcdEvent);

    EXPECT_EQ(event.type, topology::CoordinationEventType::DELETE);
    EXPECT_EQ(event.key, TEST_WORKER_KEY);
    EXPECT_TRUE(event.value.empty());
}

TEST(CoordinationBackendTest, EventToStringIncludesOperationAndRevision)
{
    topology::CoordinationEvent event;
    event.type = topology::CoordinationEventType::PUT;
    event.key = "/datasystem/ring";
    event.value = "ring-bytes";
    event.version = 3;
    event.revision = 7;

    auto message = event.ToString();
    EXPECT_NE(message.find("PUT"), std::string::npos);
    EXPECT_NE(message.find("/datasystem/ring"), std::string::npos);
    EXPECT_NE(message.find("revision: 7"), std::string::npos);

    event.type = topology::CoordinationEventType::DELETE;
    EXPECT_NE(event.ToString().find("DELETE"), std::string::npos);
}

TEST(CoordinationBackendTest, NullEtcdStoreReturnsRuntimeError)
{
    topology::EtcdCoordinationBackend backend(nullptr);
    std::vector<std::pair<std::string, std::string>> keyValues;
    std::string value;
    RangeSearchResult rangeResult;
    topology::ICoordinationBackend::ProcessFunction process = [](const std::string &, std::unique_ptr<std::string> &,
                                                                 bool &) { return Status::OK(); };
    std::vector<topology::WatchKey> watchKeys = { { ETCD_RING_PREFIX, "", 0 } };
    HostPort workerAddr;

    EXPECT_EQ(backend.GetAll(ETCD_RING_PREFIX, keyValues).GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(backend.Get(ETCD_RING_PREFIX, "", value).GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(backend.Get(ETCD_RING_PREFIX, "", rangeResult).GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(backend.CAS(ETCD_RING_PREFIX, "", process, rangeResult).GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(backend.CAS(ETCD_RING_PREFIX, "", process).GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(backend.CAS(ETCD_RING_PREFIX, "", "old", "new").GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(backend.Delete(ETCD_RING_PREFIX, "").GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(backend.WatchEvents(watchKeys).GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(backend.InitKeepAlive(ETCD_RING_PREFIX, "", false, true).GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(backend.UpdateNodeState("READY").GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(backend.GetStorePrefix(ETCD_RING_PREFIX, value).GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(backend.InformReconciliationDone(workerAddr).GetCode(), K_RUNTIME_ERROR);
    EXPECT_FALSE(backend.IsKeepAliveTimeout());
    EXPECT_FALSE(backend.IsCreateFirstLease());

    bool called = false;
    backend.SetEventHandler([&called](topology::CoordinationEvent &&) { called = true; });
    backend.SetCheckStoreStateWhenNetworkFailedHandler([] { return true; });
    EXPECT_FALSE(called);
}

TEST(CoordinationBackendTest, ForwardsVirtualKvStoreCalls)
{
    StubEtcdStore store;
    topology::EtcdCoordinationBackend backend(&store);

    std::vector<std::pair<std::string, std::string>> keyValues;
    DS_ASSERT_OK(backend.GetAll(ETCD_RING_PREFIX, keyValues));
    ASSERT_EQ(keyValues.size(), 1ul);
    EXPECT_EQ(keyValues[0].first, "key");

    std::string value;
    DS_ASSERT_OK(backend.Get(ETCD_RING_PREFIX, "", value));
    EXPECT_EQ(value, "value");
    DS_ASSERT_OK(backend.Delete(ETCD_RING_PREFIX, ""));

    backend.SetEventHandler([](topology::CoordinationEvent &&) {});
    backend.SetCheckStoreStateWhenNetworkFailedHandler([] { return true; });
}
}  // namespace
}  // namespace datasystem
