/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Master-compatible cluster coordination backend contract tests.
 */
#include "datasystem/cluster/coordination_backend/coordination_backend.h"
#include "datasystem/cluster/coordination_backend/ds_coordination_backend.h"
#include "datasystem/cluster/coordination_backend/etcd_coordination_backend.h"

#include <type_traits>

#include "etcd/api/mvccpb/kv.pb.h"
#include "gtest/gtest.h"

namespace datasystem::cluster {
namespace {

class FakeCoordinatorServiceProxy final : public ICoordinatorServiceProxy {
public:
    struct WatchCall {
        std::string key;
        std::string rangeEnd;
        std::string watcherAddr;
        int64_t watchId{ 0 };
    };

    ~FakeCoordinatorServiceProxy() override = default;

    Status Put(const std::string &, const std::string &, int64_t, int64_t, int64_t &, int64_t &, int32_t) override
    {
        return Status(K_RUNTIME_ERROR, "unused fake Put");
    }

    Status Range(const std::string &, const std::string &, std::vector<KeyValueEntry> &, int64_t &, int32_t) override
    {
        return Status(K_RUNTIME_ERROR, "unused fake Range");
    }

    Status DeleteRange(const std::string &, const std::string &, int64_t &, int64_t &, int32_t) override
    {
        return Status(K_RUNTIME_ERROR, "unused fake DeleteRange");
    }

    Status WatchRange(const std::string &key, const std::string &rangeEnd, const std::string &watcherAddr,
                      int64_t &watchId, std::vector<KeyValueEntry> &initialKvs, int32_t) override
    {
        watchId = nextWatchId_++;
        initialKvs = initialKvs_;
        watchCalls_.push_back({ key, rangeEnd, watcherAddr, watchId });
        return Status::OK();
    }

    Status CancelWatch(const std::string &watcherAddr, const std::vector<int64_t> &watchIds, int32_t) override
    {
        lastCancelWatcherAddr_ = watcherAddr;
        cancelledWatchIds_.insert(cancelledWatchIds_.end(), watchIds.begin(), watchIds.end());
        return Status::OK();
    }

    Status KeepAlive(const std::string &, int64_t &, int64_t &, int32_t) override
    {
        return Status(K_RUNTIME_ERROR, "unused fake KeepAlive");
    }

    Status CAS(const std::string &, const CasProcessFunc &, int64_t &, int64_t &) override
    {
        return Status(K_RUNTIME_ERROR, "unused fake CAS");
    }

    std::vector<KeyValueEntry> initialKvs_;
    std::vector<WatchCall> watchCalls_;
    std::string lastCancelWatcherAddr_;
    std::vector<int64_t> cancelledWatchIds_;

private:
    int64_t nextWatchId_{ 1 };
};

static_assert(std::is_abstract_v<ICoordinationBackend>);
static_assert(std::has_virtual_destructor_v<ICoordinationBackend>);
static_assert(
    std::is_same_v<decltype(&ICoordinationBackend::ShutdownEventSources), Status (ICoordinationBackend::*)()>);
static_assert(std::is_same_v<decltype(&ICoordinationBackend::Shutdown), Status (ICoordinationBackend::*)()>);

TEST(CoordinationBackendContractTest, PreservesExactAndPrefixWatchDescriptor)
{
    WatchKey exact{ "/datasystem/c/notify", "127.0.0.1:1", 17 };
    WatchKey prefix{ "/datasystem/c/cluster", "", 19 };

    EXPECT_EQ(exact.tableName, "/datasystem/c/notify");
    EXPECT_EQ(exact.key, "127.0.0.1:1");
    EXPECT_EQ(exact.startRevision, 17);
    EXPECT_EQ(prefix.tableName, "/datasystem/c/cluster");
    EXPECT_TRUE(prefix.key.empty());
    EXPECT_EQ(prefix.startRevision, 19);
}

TEST(CoordinationBackendContractTest, ConvertsEtcdEventsWithoutPublishingPolicy)
{
    mvccpb::Event input;
    input.set_type(mvccpb::Event_EventType_PUT);
    input.mutable_kv()->set_key("/datasystem/c/topology");
    input.mutable_kv()->set_value("topology-bytes");
    input.mutable_kv()->set_version(3);
    input.mutable_kv()->set_mod_revision(29);

    auto event = EtcdCoordinationBackend::FromEtcdEvent(input);

    EXPECT_EQ(event.type, CoordinationEventType::PUT);
    EXPECT_EQ(event.key, "/datasystem/c/topology");
    EXPECT_EQ(event.value, "topology-bytes");
    EXPECT_EQ(event.version, 3);
    EXPECT_EQ(event.revision, 29);
}

TEST(CoordinationBackendContractTest, NullStoreFailsOperationsAndShutsDownIdempotently)
{
    EtcdCoordinationBackend backend(nullptr);
    std::string value;
    std::vector<std::pair<std::string, std::string>> values;

    EXPECT_EQ(backend.Get("topology", "", value).GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(backend.GetAll("cluster", values).GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(backend.WatchEvents({ { "topology", "", 0 } }).GetCode(), K_RUNTIME_ERROR);
    EXPECT_TRUE(backend.ShutdownEventSources().IsOk());
    EXPECT_TRUE(backend.Shutdown().IsOk());
}

TEST(CoordinationBackendContractTest, DsBackendPreservesLegacyMembershipTable)
{
    DsCoordinationBackend backend(nullptr, "127.0.0.1:1");
    std::string prefix;

    EXPECT_TRUE(backend.GetStorePrefix("/datasystem/cluster-a/cluster", prefix).IsOk());
    EXPECT_EQ(prefix, "/datasystem/cluster");
    EXPECT_TRUE(backend.GetStorePrefix("/datasystem/cluster-a/topology", prefix).IsOk());
    EXPECT_EQ(prefix, "/datasystem/cluster-a/topology");
}

TEST(CoordinationBackendContractTest, DsBackendPrefixWatchAcceptsCoordinatorChildEvents)
{
    FakeCoordinatorServiceProxy proxy;
    DsCoordinationBackend backend(&proxy, "127.0.0.1:1");
    int eventCount = 0;
    std::string acceptedKey;
    backend.SetEventHandler([&eventCount, &acceptedKey](CoordinationEvent &&event) {
        ++eventCount;
        acceptedKey = event.key;
    });

    ASSERT_TRUE(backend.WatchEvents({ { "/datasystem/c/tasks/migrate", "", 0 } }).IsOk());
    ASSERT_EQ(proxy.watchCalls_.size(), 1UL);
    EXPECT_EQ(proxy.watchCalls_[0].key, "/datasystem/c/tasks/migrate/");
    EXPECT_EQ(proxy.watchCalls_[0].rangeEnd, "/datasystem/c/tasks/migrate0");

    CoordinationEvent accepted{ CoordinationEventType::PUT, "/datasystem/c/tasks/migrate/task-1", "value", 1, 2 };
    backend.HandleWatchEvent(std::move(accepted));
    EXPECT_EQ(eventCount, 1);
    EXPECT_EQ(acceptedKey, "/datasystem/c/tasks/migrate/task-1");

    CoordinationEvent rejected{ CoordinationEventType::PUT, "/datasystem/c/tasks/delete/task-1", "value", 1, 3 };
    backend.HandleWatchEvent(std::move(rejected));
    EXPECT_EQ(eventCount, 1);
}

TEST(CoordinationBackendContractTest, DsBackendExactWatchRejectsCoordinatorChildEvents)
{
    FakeCoordinatorServiceProxy proxy;
    DsCoordinationBackend backend(&proxy, "127.0.0.1:1");
    int eventCount = 0;
    backend.SetEventHandler([&eventCount](CoordinationEvent &&) {
        ++eventCount;
    });

    ASSERT_TRUE(backend.WatchEvents({ { "/datasystem/c/notify", "127.0.0.1:2", 0 } }).IsOk());
    ASSERT_EQ(proxy.watchCalls_.size(), 1UL);
    EXPECT_EQ(proxy.watchCalls_[0].key, "/datasystem/c/notify/127.0.0.1:2");
    EXPECT_TRUE(proxy.watchCalls_[0].rangeEnd.empty());

    CoordinationEvent accepted{ CoordinationEventType::PUT, "/datasystem/c/notify/127.0.0.1:2", "value", 1, 2 };
    backend.HandleWatchEvent(std::move(accepted));
    EXPECT_EQ(eventCount, 1);

    CoordinationEvent rejected{ CoordinationEventType::PUT, "/datasystem/c/notify/127.0.0.1:2/child", "value", 1, 3 };
    backend.HandleWatchEvent(std::move(rejected));
    EXPECT_EQ(eventCount, 1);
}

TEST(CoordinationBackendContractTest, DiagnosticEventOmitsPayload)
{
    CoordinationEvent event{ CoordinationEventType::DELETE, "/datasystem/c/notify/member", "secret", 2, 31 };
    const auto diagnostic = event.ToString();

    EXPECT_NE(diagnostic.find("DELETE"), std::string::npos);
    EXPECT_NE(diagnostic.find("revision: 31"), std::string::npos);
    EXPECT_EQ(diagnostic.find("secret"), std::string::npos);
}

}  // namespace
}  // namespace datasystem::cluster
