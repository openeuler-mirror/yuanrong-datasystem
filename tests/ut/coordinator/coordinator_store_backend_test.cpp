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
 * Description: Unit tests for the process-local Coordinator topology Store adapter.
 */
#include "datasystem/coordinator/coordinator_store_backend.h"

#include <chrono>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

#include "datasystem/common/coordinator/coordinator_store.h"
#include "datasystem/common/coordinator/memory_kv_store.h"
#include "datasystem/common/coordinator/steady_clock.h"
#include "datasystem/common/coordinator/ttl_manager.h"
#include "datasystem/common/coordinator/watch_dispatcher.h"
#include "datasystem/common/coordinator/watch_registry.h"
#include "datasystem/common/util/status_helper.h"
#include "ut/common.h"

namespace datasystem::coordinator {
namespace {
constexpr char BLUE_TABLE[] = "/datasystem/blue/tasks/migrate";
constexpr char GREEN_TABLE[] = "/datasystem/green/tasks/migrate";
constexpr char TEST_KEY[] = "m-e1-0123456789abcdef0123456789abcdef";
constexpr size_t MAX_CAS_ATTEMPTS = 16;

class NoopWatchDispatcher final : public WatchDispatcher {
public:
    explicit NoopWatchDispatcher(WatchRegistry *registry) : WatchDispatcher(registry)
    {
    }

    ~NoopWatchDispatcher() override = default;

    Status DoNotify(int64_t, const std::string &, std::vector<std::shared_ptr<WatchEvent>> &) override
    {
        return Status::OK();
    }
};

class CoordinatorStoreBackendTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        memoryStore_ = std::make_shared<MemoryKvStore>();
        registry_ = std::make_shared<WatchRegistry>();
        dispatcher_ = std::make_shared<NoopWatchDispatcher>(registry_.get());
        clock_ = std::make_shared<SteadyClockMock>();
        ttlManager_ = std::make_shared<TtlManager>(clock_);
        store_ = std::make_unique<CoordinatorStore>(memoryStore_, registry_, dispatcher_, ttlManager_);
        backend_ = std::make_unique<CoordinatorStoreBackend>(*store_);
    }

    void TearDown() override
    {
        backend_.reset();
        store_.reset();
    }

    Status PutPhysical(const std::string &key, const std::string &value)
    {
        int64_t version = 0;
        int64_t revision = 0;
        return store_->Put(key, value, 0, COORDINATOR_NO_VERSION_CHECK, version, revision);
    }

    Status ReadPhysical(const std::string &key, KeyValueEntry &entry)
    {
        std::vector<KeyValueEntry> entries;
        int64_t revision = 0;
        RETURN_IF_NOT_OK(store_->Range(key, "", entries, revision));
        if (entries.empty()) {
            return Status(K_NOT_FOUND, "missing test key");
        }
        entry = std::move(entries.front());
        return Status::OK();
    }

    std::shared_ptr<MemoryKvStore> memoryStore_;
    std::shared_ptr<WatchRegistry> registry_;
    std::shared_ptr<NoopWatchDispatcher> dispatcher_;
    std::shared_ptr<SteadyClockMock> clock_;
    std::shared_ptr<TtlManager> ttlManager_;
    std::unique_ptr<CoordinatorStore> store_;
    std::unique_ptr<CoordinatorStoreBackend> backend_;
};

TEST_F(CoordinatorStoreBackendTest, ExactReadUsesTrailingSlashAndPreservesMissingOutput)
{
    DS_ASSERT_OK(backend_->CAS(BLUE_TABLE, "", "", "topology"));
    std::string value;
    DS_ASSERT_OK(backend_->Get(BLUE_TABLE, "", value));
    EXPECT_EQ(value, "topology");

    cluster::CoordinationStoreResult result;
    DS_ASSERT_OK(backend_->Get(BLUE_TABLE, "", result));
    EXPECT_EQ(result.key, std::string(BLUE_TABLE) + "/");
    EXPECT_EQ(result.value, "topology");
    EXPECT_GT(result.version, 0);

    value = "unchanged";
    EXPECT_EQ(backend_->Get(BLUE_TABLE, "missing", value).GetCode(), K_NOT_FOUND);
    EXPECT_EQ(value, "unchanged");
}

TEST_F(CoordinatorStoreBackendTest, RevisionFencedCasRejectsConcurrentStoreMutation)
{
    DS_ASSERT_OK(backend_->CAS(BLUE_TABLE, "", "", "topology-v1"));
    const int64_t snapshotRevision = memoryStore_->CurrentRevision();
    cluster::CoordinationStoreResult result;
    const auto commitV2 = [](const std::string &, std::unique_ptr<std::string> &next, bool &retry) {
        retry = false;
        next = std::make_unique<std::string>("topology-v2");
        return Status::OK();
    };
    DS_ASSERT_OK(backend_->CASAtRevision(BLUE_TABLE, "", commitV2, snapshotRevision, result));

    const int64_t staleRevision = memoryStore_->CurrentRevision();
    DS_ASSERT_OK(PutPhysical("/datasystem/blue/cluster/127.0.0.1:12001", "new-membership"));
    const auto commitV3 = [](const std::string &, std::unique_ptr<std::string> &next, bool &retry) {
        retry = false;
        next = std::make_unique<std::string>("topology-v3");
        return Status::OK();
    };

    EXPECT_EQ(backend_->CASAtRevision(BLUE_TABLE, "", commitV3, staleRevision, result).GetCode(), K_TRY_AGAIN);
    std::string value;
    DS_ASSERT_OK(backend_->Get(BLUE_TABLE, "", value));
    EXPECT_EQ(value, "topology-v2");
}

TEST_F(CoordinatorStoreBackendTest, RangeReadsOnlyExactTablePrefixAndReturnsRelativeKeys)
{
    DS_ASSERT_OK(PutPhysical(std::string(BLUE_TABLE) + "/" + TEST_KEY, "blue"));
    DS_ASSERT_OK(PutPhysical(std::string(BLUE_TABLE) + "-extra/" + TEST_KEY, "sibling"));
    DS_ASSERT_OK(PutPhysical(std::string(GREEN_TABLE) + "/" + TEST_KEY, "green"));

    std::vector<std::pair<std::string, std::string>> values{ { "existing", "value" } };
    DS_ASSERT_OK(backend_->GetAll(BLUE_TABLE, values));
    ASSERT_EQ(values.size(), 2);
    EXPECT_EQ(values[0], std::make_pair(std::string("existing"), std::string("value")));
    EXPECT_EQ(values[1], std::make_pair(std::string(TEST_KEY), std::string("blue")));
}

TEST_F(CoordinatorStoreBackendTest, CallbackCasCreatesAndNoChangeDoesNotAdvanceRevision)
{
    cluster::CoordinationStoreResult created;
    cluster::ICoordinationBackend::ProcessFunction create =
        [](const std::string &current, std::unique_ptr<std::string> &next, bool &retry) {
            EXPECT_TRUE(current.empty());
            retry = false;
            next = std::make_unique<std::string>("created");
            return Status::OK();
        };
    DS_ASSERT_OK(backend_->CAS(BLUE_TABLE, TEST_KEY, create, created));

    cluster::CoordinationStoreResult observed;
    cluster::ICoordinationBackend::ProcessFunction noChange =
        [](const std::string &, std::unique_ptr<std::string> &, bool &) { return Status::OK(); };
    DS_ASSERT_OK(backend_->CAS(BLUE_TABLE, TEST_KEY, noChange, observed));
    EXPECT_EQ(observed.value, "created");
    EXPECT_EQ(observed.version, created.version);
    EXPECT_EQ(observed.modRevision, created.modRevision);
}

TEST_F(CoordinatorStoreBackendTest, CallbackErrorHonorsRetryFlagAndPreservesOutput)
{
    size_t attempts = 0;
    cluster::ICoordinationBackend::ProcessFunction noRetry =
        [&attempts](const std::string &, std::unique_ptr<std::string> &, bool &retry) {
            ++attempts;
            retry = false;
            return Status(K_RUNTIME_ERROR, "do not retry");
        };
    cluster::CoordinationStoreResult result;
    result.value = "unchanged";
    auto rc = backend_->CAS(BLUE_TABLE, TEST_KEY, noRetry, result);
    EXPECT_EQ(rc.GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(attempts, 1);
    EXPECT_EQ(result.value, "unchanged");

    attempts = 0;
    cluster::ICoordinationBackend::ProcessFunction retry =
        [&attempts](const std::string &, std::unique_ptr<std::string> &, bool &retryByCaller) {
            ++attempts;
            retryByCaller = true;
            return Status(K_RUNTIME_ERROR, "retry requested");
        };
    rc = backend_->CAS(BLUE_TABLE, TEST_KEY, retry, result);
    EXPECT_EQ(rc.GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(attempts, MAX_CAS_ATTEMPTS);
    EXPECT_EQ(result.value, "unchanged");
}

TEST_F(CoordinatorStoreBackendTest, VersionConflictRereadsAndThenCommits)
{
    DS_ASSERT_OK(PutPhysical(std::string(BLUE_TABLE) + "/" + TEST_KEY, "initial"));
    size_t attempts = 0;
    cluster::ICoordinationBackend::ProcessFunction process =
        [this, &attempts](const std::string &current, std::unique_ptr<std::string> &next, bool &) {
            ++attempts;
            if (attempts == 1) {
                RETURN_IF_NOT_OK(PutPhysical(std::string(BLUE_TABLE) + "/" + TEST_KEY, "concurrent"));
                EXPECT_EQ(current, "initial");
            } else {
                EXPECT_EQ(current, "concurrent");
            }
            next = std::make_unique<std::string>("committed");
            return Status::OK();
        };
    cluster::CoordinationStoreResult result;
    DS_ASSERT_OK(backend_->CAS(BLUE_TABLE, TEST_KEY, process, result));
    EXPECT_EQ(attempts, 2);
    EXPECT_EQ(result.value, "committed");
}

TEST_F(CoordinatorStoreBackendTest, VersionConflictExhaustionIsBoundedWithoutChangingOutput)
{
    DS_ASSERT_OK(PutPhysical(std::string(BLUE_TABLE) + "/" + TEST_KEY, "initial"));
    size_t attempts = 0;
    cluster::ICoordinationBackend::ProcessFunction process =
        [this, &attempts](const std::string &, std::unique_ptr<std::string> &next, bool &) {
            ++attempts;
            RETURN_IF_NOT_OK(PutPhysical(std::string(BLUE_TABLE) + "/" + TEST_KEY, std::to_string(attempts)));
            next = std::make_unique<std::string>("never-committed");
            return Status::OK();
        };
    cluster::CoordinationStoreResult result;
    result.value = "unchanged";
    const auto begin = std::chrono::steady_clock::now();
    const auto rc = backend_->CAS(BLUE_TABLE, TEST_KEY, process, result);
    const auto elapsed = std::chrono::steady_clock::now() - begin;
    EXPECT_EQ(rc.GetCode(), K_INVALID);
    EXPECT_EQ(attempts, MAX_CAS_ATTEMPTS);
    EXPECT_EQ(result.value, "unchanged");
    EXPECT_LT(elapsed, std::chrono::seconds(1));
}

TEST_F(CoordinatorStoreBackendTest, RawCasDeleteAndShutdownPreserveBorrowedStore)
{
    DS_ASSERT_OK(backend_->CAS(BLUE_TABLE, TEST_KEY, "ignored-when-absent", "first"));
    EXPECT_EQ(backend_->CAS(BLUE_TABLE, TEST_KEY, "wrong", "second").GetCode(), K_TRY_AGAIN);
    DS_ASSERT_OK(backend_->CAS(BLUE_TABLE, TEST_KEY, "first", "second"));
    DS_ASSERT_OK(backend_->Delete(BLUE_TABLE, TEST_KEY, 1));
    DS_ASSERT_OK(backend_->Delete(BLUE_TABLE, TEST_KEY));

    DS_ASSERT_OK(backend_->ShutdownEventSources());
    DS_ASSERT_OK(backend_->ShutdownEventSources());
    DS_ASSERT_OK(backend_->Shutdown());
    DS_ASSERT_OK(backend_->Shutdown());
    DS_ASSERT_OK(PutPhysical(std::string(BLUE_TABLE) + "/" + TEST_KEY, "store-alive"));
    std::string value;
    DS_ASSERT_OK(backend_->Get(BLUE_TABLE, TEST_KEY, value));
    EXPECT_EQ(value, "store-alive");
}

TEST_F(CoordinatorStoreBackendTest, ControllerOnlyMethodsRejectWorkerLifecycle)
{
    EXPECT_EQ(backend_->WatchEvents({}).GetCode(), K_INVALID);
    EXPECT_EQ(backend_->PutWithKeepAliveLease(BLUE_TABLE, TEST_KEY, "health").GetCode(), K_INVALID);
    EXPECT_EQ(backend_->InitKeepAlive(BLUE_TABLE, TEST_KEY, false, true).GetCode(), K_INVALID);
    EXPECT_EQ(backend_->UpdateNodeState(cluster::MemberLifecycleState::READY).GetCode(), K_INVALID);
    HostPort worker;
    EXPECT_EQ(backend_->InformReconciliationDone(worker).GetCode(), K_INVALID);
    EXPECT_FALSE(backend_->IsKeepAliveTimeout());
    EXPECT_FALSE(backend_->IsFirstKeepAliveSent());
    backend_->SetEventHandler([](cluster::CoordinationEvent &&) {});
    backend_->SetCheckStoreStateWhenNetworkFailedHandler([] { return true; });

    std::string prefix = "unchanged";
    DS_ASSERT_OK(backend_->GetStorePrefix(BLUE_TABLE, prefix));
    EXPECT_EQ(prefix, BLUE_TABLE);
    prefix = "unchanged";
    EXPECT_EQ(backend_->GetStorePrefix("", prefix).GetCode(), K_INVALID);
    EXPECT_EQ(prefix, "unchanged");
}

}  // namespace
}  // namespace datasystem::coordinator
