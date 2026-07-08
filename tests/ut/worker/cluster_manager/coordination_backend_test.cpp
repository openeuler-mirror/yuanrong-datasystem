#include "datasystem/topology/coordination_backend/coordination_backend.h"
#include "datasystem/topology/coordination_backend/etcd_coordination_backend.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "etcd/api/mvccpb/kv.pb.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/util/compatibility_manager.h"
#include "datasystem/protos/coordinator.pb.h"
#include "datasystem/topology/membership/worker_node_info.h"
#include "datasystem/worker/cluster_manager/cluster_constants.h"
#include "tests/ut/common.h"

namespace datasystem {
namespace {
constexpr char TEST_ETCD_ENDPOINT[] = "localhost:1";
constexpr char TEST_WORKER_KEY[] = "/datasystem/cluster/test-worker:31501";
using topology::WorkerServiceInfo;
using topology::WorkerServiceState;

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

class StubCoordinatorProxy final : public ICoordinatorServiceProxy {
public:
    Status Put(const std::string &key, const std::string &value, int64_t ttlMs, int64_t, int64_t &version,
               int64_t &revision, int32_t) override
    {
        values_[key] = value;
        ttls_[key] = ttlMs;
        version = ++version_;
        revision = ++revision_;
        return Status::OK();
    }

    Status Range(const std::string &key, const std::string &, std::vector<KeyValueEntry> &kvs, int64_t &revision,
                 int32_t) override
    {
        kvs.clear();
        auto iter = values_.find(key);
        if (iter != values_.end()) {
            kvs.emplace_back(KeyValueEntry{ iter->first, iter->second, version_, revision_ });
        }
        revision = revision_;
        return Status::OK();
    }

    Status DeleteRange(const std::string &, const std::string &, int64_t &deleted, int64_t &revision, int32_t) override
    {
        deleted = 0;
        revision = revision_;
        return Status::OK();
    }

    Status WatchRange(const std::string &key, const std::string &, const std::string &, int64_t &watchId,
                      std::vector<KeyValueEntry> &initialKvs, int32_t) override
    {
        ++watchRangeCallCount_;
        if (failWatchRangeAfter_ > 0 && watchRangeCallCount_ >= failWatchRangeAfter_) {
            RETURN_STATUS(K_RUNTIME_ERROR, "injected watch failure");
        }
        watchId = nextWatchId_++;
        initialKvs.clear();
        auto iter = watchInitialKvs_.find(key);
        if (iter != watchInitialKvs_.end()) {
            initialKvs = iter->second;
        }
        return Status::OK();
    }

    Status CancelWatch(const std::string &, const std::vector<int64_t> &watchIds, int32_t) override
    {
        canceledWatchIds_.insert(canceledWatchIds_.end(), watchIds.begin(), watchIds.end());
        return Status::OK();
    }

    Status KeepAlive(const std::string &, int64_t &, int64_t &remainingTtlMs, int32_t) override
    {
        remainingTtlMs = 1;
        return Status::OK();
    }

    Status CAS(const std::string &, const CasProcessFunc &, int64_t &, int64_t &) override
    {
        RETURN_STATUS(K_RUNTIME_ERROR, "CAS is not used by this test");
    }

    void FailWatchRangeAfter(int callCount)
    {
        failWatchRangeAfter_ = callCount;
    }

    void AddWatchInitialKv(const std::string &key, const KeyValueEntry &entry)
    {
        watchInitialKvs_[key].emplace_back(entry);
    }

    const std::vector<int64_t> &CanceledWatchIds() const
    {
        return canceledWatchIds_;
    }

private:
    std::unordered_map<std::string, std::string> values_;
    std::unordered_map<std::string, int64_t> ttls_;
    std::unordered_map<std::string, std::vector<KeyValueEntry>> watchInitialKvs_;
    std::vector<int64_t> canceledWatchIds_;
    int64_t version_ = 0;
    int64_t revision_ = 0;
    int64_t nextWatchId_ = 1;
    int watchRangeCallCount_ = 0;
    int failWatchRangeAfter_ = 0;
};

TEST(WorkerServiceInfoTest, ToStringAndFromStringRoundTrip)
{
    WorkerServiceInfo in{ 12345, WorkerServiceState::READY, "host-a", "v1" };
    std::string str = in.ToString();
    EXPECT_EQ(str, "12345;ready;host-a;v1");

    WorkerServiceInfo out;
    DS_ASSERT_OK(WorkerServiceInfo::FromString(str, out));
    EXPECT_EQ(out.timestamp, 12345);
    EXPECT_EQ(out.state, WorkerServiceState::READY);
    EXPECT_EQ(out.hostId, "host-a");
    EXPECT_EQ(out.compatibilityVersion, "v1");
}

TEST(WorkerServiceInfoTest, PlaceholderTimestampConvertsToZero)
{
    WorkerServiceInfo parsed;
    DS_ASSERT_OK(WorkerServiceInfo::FromString("_;start", parsed));
    EXPECT_EQ(parsed.timestamp, 0);
    EXPECT_EQ(parsed.state, WorkerServiceState::START);
    EXPECT_EQ(parsed.hostId, "");
}

TEST(WorkerServiceInfoTest, ToStringReportsUnspecifiedStateAsInvalid)
{
    WorkerServiceInfo value{ 12345, WorkerServiceState::UNSPECIFIED, "", "" };

    auto str = value.ToString();

    EXPECT_TRUE(str.empty());
    WorkerServiceInfo parsed;
    auto rc = WorkerServiceInfo::FromString(str, parsed);
    DS_EXPECT_NOT_OK(rc);
    EXPECT_EQ(rc.GetCode(), K_INVALID);
}

TEST(WorkerServiceInfoTest, ProtoRoundTrip)
{
    WorkerServiceInfo in{ 12345, WorkerServiceState::EXITING, "host-a", "v1" };
    std::string bytes = in.ToProto();

    WorkerServiceInfo out;
    DS_ASSERT_OK(WorkerServiceInfo::FromProto(bytes, out));
    EXPECT_EQ(out.timestamp, 12345);
    EXPECT_EQ(out.state, WorkerServiceState::EXITING);
    EXPECT_EQ(out.hostId, "host-a");
    EXPECT_EQ(out.compatibilityVersion, "v1");
}

TEST(WorkerServiceInfoTest, FromStringRejectsInvalidTimestamp)
{
    WorkerServiceInfo parsed{ 1, WorkerServiceState::READY, "host", "v1" };
    auto rc = WorkerServiceInfo::FromString("bad;ready;host", parsed);
    DS_EXPECT_NOT_OK(rc);
    EXPECT_EQ(rc.GetCode(), K_INVALID);
    EXPECT_EQ(parsed.timestamp, 0);
    EXPECT_EQ(parsed.state, WorkerServiceState::UNSPECIFIED);
    EXPECT_EQ(parsed.hostId, "");
    EXPECT_EQ(parsed.compatibilityVersion, "");
}

TEST(CoordinationBackendTest, StoresWorkerServiceInfoProto)
{
    StubCoordinatorProxy proxy;
    topology::CoordinationBackend backend(&proxy, "127.0.0.1:0");
    DS_ASSERT_OK(backend.InitKeepAlive(CLUSTER_TABLE, "127.0.0.1:31501", false, true));
    DS_ASSERT_OK(backend.UpdateNodeState(topology::MemberLifecycleState::READY));

    std::vector<KeyValueEntry> kvs;
    int64_t revision = 0;
    DS_ASSERT_OK(
        proxy.Range("/datasystem/cluster/127.0.0.1:31501", "", kvs, revision, DEFAULT_COORDINATOR_RPC_TIMEOUT_MS));
    ASSERT_EQ(kvs.size(), 1ul);

    coordinator::WorkerServiceInfoPb info;
    ASSERT_TRUE(info.ParseFromString(kvs.front().value));
    EXPECT_GT(info.timestamp(), 0);
    EXPECT_EQ(info.state(), coordinator::WorkerServiceInfoPb::READY);
    EXPECT_EQ(info.compatibility_version(),
              CompatibilityManager::Instance().GetCurrentCompatibilityVersion().ToString());
}

TEST(CoordinationBackendTest, RollsBackRegisteredWatchesWhenLaterWatchFails)
{
    StubCoordinatorProxy proxy;
    proxy.FailWatchRangeAfter(2);
    proxy.AddWatchInitialKv(ETCD_RING_PREFIX, KeyValueEntry{ ETCD_RING_PREFIX, "ring-bytes", 1, 1 });
    topology::CoordinationBackend backend(&proxy, "127.0.0.1:0");
    int eventCount = 0;
    backend.SetEventHandler([&eventCount](topology::CoordinationEvent &&) { ++eventCount; });

    auto rc = backend.WatchEvents({ { ETCD_RING_PREFIX, "", 0 }, { CLUSTER_TABLE, "", 0 } });

    DS_EXPECT_NOT_OK(rc);
    EXPECT_EQ(rc.GetCode(), K_RUNTIME_ERROR);
    ASSERT_EQ(proxy.CanceledWatchIds().size(), 1ul);
    EXPECT_EQ(proxy.CanceledWatchIds()[0], 1);
    EXPECT_EQ(eventCount, 0);
}

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
    EXPECT_EQ(backend.UpdateNodeState(topology::MemberLifecycleState::READY).GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(backend.GetStorePrefix(ETCD_RING_PREFIX, value).GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(backend.InformReconciliationDone(workerAddr).GetCode(), K_RUNTIME_ERROR);
    EXPECT_FALSE(backend.IsKeepAliveTimeout());
    EXPECT_FALSE(backend.IsFirstKeepAliveSent());

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
