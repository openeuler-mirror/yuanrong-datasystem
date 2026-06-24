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
 * Description: Placement runtime read path unit tests.
 */
#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/gflag/flags.h"
#include "datasystem/worker/topology/membership/worker_directory.h"
#include "datasystem/worker/topology/routing/redirect_policy.h"
#include "datasystem/worker/topology/routing/routing_cache.h"
#include "datasystem/worker/topology/routing/routing_view.h"
#include "datasystem/worker/topology/routing/worker_locator.h"
#include "datasystem/worker/topology/runtime/placement_facade.h"

DS_DECLARE_string(master_address);

namespace datasystem {
namespace topology {
namespace {

constexpr int64_t VERSION_1 = 1;
constexpr int64_t VERSION_2 = 2;
const std::string WORKER_A = "worker-a";
const std::string WORKER_B = "worker-b";

HostPort MakeAddr(int port)
{
    return HostPort("127.0.0.1", port);
}

std::shared_ptr<RoutingSnapshot> MakeSnapshot()
{
    std::vector<RoutingOwnerEntry> owners{
        { PlacementUnit{ 0, 100 }, WORKER_A },
        { PlacementUnit{ 100, 200 }, WORKER_B },
    };
    return std::make_shared<RoutingSnapshot>(VERSION_1, std::move(owners));
}

std::shared_ptr<RoutingSnapshot> MakeSingleOwnerSnapshot(uint32_t hash, const std::string &workerId)
{
    std::vector<RoutingOwnerEntry> owners{
        { PlacementUnit{ hash - 1, hash }, workerId },
    };
    return std::make_shared<RoutingSnapshot>(VERSION_1, std::move(owners));
}

std::shared_ptr<WorkerDirectorySnapshot> MakeDirectory(
    WorkerAvailability workerBAvailability = WorkerAvailability::READY)
{
    auto snapshot = std::make_shared<WorkerDirectorySnapshot>();
    snapshot->version = VERSION_1;
    snapshot->localWorkerId = WORKER_A;
    snapshot->localAddress = MakeAddr(1111);
    snapshot->workers.emplace(WORKER_A, WorkerEndpoint{ WORKER_A, MakeAddr(1111), WorkerAvailability::READY });
    snapshot->workers.emplace(WORKER_B, WorkerEndpoint{ WORKER_B, MakeAddr(2222), workerBAvailability });
    return snapshot;
}

PlacementFacade MakeScopeOnlyFacade()
{
    return PlacementFacade(nullptr, nullptr);
}

TEST(PlacementRuntimeTest, RoutingSnapshotLocateNormalRange)
{
    auto snapshot = MakeSnapshot();
    RoutingOwnerEntry entry;
    auto status = snapshot->Locate(50, entry);
    EXPECT_TRUE(status.IsOk());
    EXPECT_EQ(entry.ownerWorkerId, WORKER_A);

    RoutingOwnerEntry boundaryEntry;
    status = snapshot->Locate(100, boundaryEntry);
    EXPECT_TRUE(status.IsOk());
    EXPECT_EQ(boundaryEntry.ownerWorkerId, WORKER_A);
}

TEST(PlacementRuntimeTest, RoutingSnapshotLocateWrappedRange)
{
    std::vector<RoutingOwnerEntry> owners{
        { PlacementUnit{ 300, 100 }, WORKER_A },
        { PlacementUnit{ 100, 300 }, WORKER_B },
    };
    RoutingSnapshot snapshot(VERSION_1, std::move(owners));

    RoutingOwnerEntry lowEntry;
    EXPECT_TRUE(snapshot.Locate(50, lowEntry).IsOk());
    EXPECT_EQ(lowEntry.ownerWorkerId, WORKER_A);

    RoutingOwnerEntry highEntry;
    EXPECT_TRUE(snapshot.Locate(350, highEntry).IsOk());
    EXPECT_EQ(highEntry.ownerWorkerId, WORKER_A);
}

TEST(PlacementRuntimeTest, RoutingSnapshotLocateEmpty)
{
    RoutingSnapshot snapshot(VERSION_1, {});
    EXPECT_TRUE(snapshot.Empty());

    RoutingOwnerEntry entry;
    auto status = snapshot.Locate(50, entry);
    EXPECT_EQ(status.GetCode(), K_NOT_READY);
}

TEST(PlacementRuntimeTest, RoutingViewNotReady)
{
    RoutingView view;
    std::shared_ptr<const RoutingSnapshot> snapshot;
    auto status = view.GetSnapshot(snapshot);
    EXPECT_EQ(status.GetCode(), K_NOT_READY);
}

TEST(PlacementRuntimeTest, WorkerDirectoryResolveMissing)
{
    WorkerDirectory directory;
    directory.Publish(MakeDirectory());

    WorkerEndpoint endpoint;
    auto status = directory.ResolveWorker("missing-worker", endpoint);
    EXPECT_EQ(status.GetCode(), K_NOT_FOUND);
}

TEST(PlacementRuntimeTest, WorkerDirectoryNotReadyThenLocal)
{
    WorkerDirectory directory;

    WorkerEndpoint resolved;
    EXPECT_EQ(directory.ResolveWorker(WORKER_A, resolved).GetCode(), K_NOT_READY);

    WorkerEndpoint local;
    EXPECT_EQ(directory.GetLocalWorker(local).GetCode(), K_NOT_READY);

    directory.Publish(MakeDirectory());
    EXPECT_TRUE(directory.GetLocalWorker(local).IsOk());
    EXPECT_EQ(local.workerId, WORKER_A);
    EXPECT_EQ(local.availability, WorkerAvailability::READY);
}

TEST(PlacementRuntimeTest, RoutingCacheVersionMismatchMiss)
{
    RoutingCache cache;
    RouteDecision decision;
    decision.routingVersion = VERSION_1;
    decision.ownerWorkerId = WORKER_A;
    cache.Store(RouteCacheKey{ VERSION_1, 10 }, decision);

    RouteDecision cached;
    EXPECT_FALSE(cache.Lookup(RouteCacheKey{ VERSION_2, 10 }, cached));
    EXPECT_TRUE(cache.Lookup(RouteCacheKey{ VERSION_1, 10 }, cached));
    EXPECT_EQ(cached.ownerWorkerId, WORKER_A);
}

TEST(PlacementRuntimeTest, LocateMetaOwnerCentralizedMaster)
{
    const std::string masterAddr = "127.0.0.1:9999";
    FLAGS_master_address = masterAddr;
    auto routingView = std::make_shared<RoutingView>();
    auto directory = std::make_shared<WorkerDirectory>();
    WorkerLocator locator(routingView, directory);

    RouteDecision decision;
    RouteOptions options;
    options.centralizedMode = true;
    EXPECT_TRUE(locator.LocateMetaOwner("any-key", options, nullptr, decision).IsOk());
    EXPECT_FALSE(decision.ownerEndpoint.address.Empty());
    EXPECT_EQ(decision.ownerEndpoint.address.ToString(), masterAddr);
    EXPECT_EQ(decision.ownerEndpoint.availability, WorkerAvailability::READY);

    BatchRouteDecision batch;
    std::vector<std::string> keys{ "key-1", "key-2", "key-3" };
    EXPECT_TRUE(locator.LocateMetaOwnersBatch(keys, options, batch).IsOk());
    EXPECT_EQ(batch.groupsByEndpoint.size(), 1ul);
    EXPECT_EQ(batch.perKeyDecision.size(), keys.size());
}

TEST(PlacementRuntimeTest, LocateMetaOwnerRequireAvailableTarget)
{
    const std::string key = "key-for-unavailable-worker";
    const uint32_t hash = MurmurHash3_32(key);
    auto routingView = std::make_shared<RoutingView>();
    routingView->Publish(MakeSingleOwnerSnapshot(hash, WORKER_B));
    auto directory = std::make_shared<WorkerDirectory>();
    directory->Publish(MakeDirectory(WorkerAvailability::UNAVAILABLE));
    WorkerLocator locator(routingView, directory);

    RouteDecision decision;
    RouteOptions options;
    options.requireAvailableTarget = true;
    auto status = locator.LocateMetaOwner(key, options, nullptr, decision);
    EXPECT_EQ(status.GetCode(), K_RPC_UNAVAILABLE);
}

TEST(PlacementRuntimeTest, LocateMetaOwnersBatchPartialFailure)
{
    const std::string key = "key-for-missing-worker";
    const uint32_t hash = MurmurHash3_32(key);
    auto routingView = std::make_shared<RoutingView>();
    routingView->Publish(MakeSingleOwnerSnapshot(hash, WORKER_B));
    auto directory = std::make_shared<WorkerDirectory>();
    auto dirSnapshot = MakeDirectory();
    dirSnapshot->workers.erase(WORKER_B);
    directory->Publish(dirSnapshot);
    WorkerLocator locator(routingView, directory);

    BatchRouteDecision decision;
    RouteOptions options;
    std::vector<std::string> keys{ key };
    EXPECT_TRUE(locator.LocateMetaOwnersBatch(keys, options, decision).IsOk());
    EXPECT_EQ(decision.routingVersion, VERSION_1);
    EXPECT_TRUE(decision.hasPartialFailure);
    EXPECT_EQ(decision.perKeyFailure.size(), 1ul);
}

TEST(PlacementRuntimeTest, LocateMetaOwnersBatchGroupsByEndpoint)
{
    const std::string keyA = "batch-group-key-a";
    const std::string keyB = "batch-group-key-b";
    const uint32_t hashA = MurmurHash3_32(keyA);
    const uint32_t hashB = MurmurHash3_32(keyB);
    // Build a two-owner ring that deterministically covers both hashes: the lower hash goes to
    // WORKER_A and the higher hash to WORKER_B. Using each key's real hash avoids hard-coded ranges.
    const uint32_t lowerHash = std::min(hashA, hashB);
    const uint32_t higherHash = std::max(hashA, hashB);
    std::vector<RoutingOwnerEntry> owners{
        { PlacementUnit{ lowerHash - 1, lowerHash }, WORKER_A },
        { PlacementUnit{ lowerHash, higherHash }, WORKER_B },
    };
    const std::string &expectedOwnerA = hashA < hashB ? WORKER_A : WORKER_B;
    const std::string &expectedOwnerB = hashA < hashB ? WORKER_B : WORKER_A;
    auto routingView = std::make_shared<RoutingView>();
    routingView->Publish(std::make_shared<RoutingSnapshot>(VERSION_1, std::move(owners)));
    auto directory = std::make_shared<WorkerDirectory>();
    directory->Publish(MakeDirectory());
    WorkerLocator locator(routingView, directory);

    BatchRouteDecision decision;
    RouteOptions options;
    std::vector<std::string> keys{ keyA, keyB };
    EXPECT_TRUE(locator.LocateMetaOwnersBatch(keys, options, decision).IsOk());
    EXPECT_EQ(decision.routingVersion, VERSION_1);
    EXPECT_FALSE(decision.hasPartialFailure);
    // Two distinct owners form two endpoint groups, one key each.
    EXPECT_EQ(decision.groupsByEndpoint.size(), 2ul);
    EXPECT_EQ(decision.perKeyDecision.size(), keys.size());
    EXPECT_EQ(decision.perKeyDecision.at(keyA).ownerWorkerId, expectedOwnerA);
    EXPECT_EQ(decision.perKeyDecision.at(keyB).ownerWorkerId, expectedOwnerB);
}

TEST(PlacementRuntimeTest, IsInRangeMatchesAndMisses)
{
    auto facade = MakeScopeOnlyFacade();

    // Normal non-wrapped range: a key hashing into (begin, end] matches.
    const std::string key = "is-in-range-key";
    const uint32_t hash = MurmurHash3_32(key);
    EXPECT_TRUE(facade.IsInRange(key, { { hash - 1, hash } }));
    // A gap before the key's hash does not match.
    EXPECT_FALSE(facade.IsInRange(key, { { hash + 1, hash + 2 } }));
    // Wrapped range across the 0 boundary still matches a low hash.
    EXPECT_TRUE(facade.IsInRange(key, { { UINT32_MAX - 1, hash } }));
    // Empty range set never matches.
    EXPECT_FALSE(facade.IsInRange(key, {}));
}

TEST(PlacementRuntimeTest, RedirectServeLocal)
{
    const std::string key = "redirect-local-key";
    const uint32_t hash = MurmurHash3_32(key);
    auto routingView = std::make_shared<RoutingView>();
    routingView->Publish(MakeSingleOwnerSnapshot(hash, WORKER_A));
    auto directory = std::make_shared<WorkerDirectory>();
    directory->Publish(MakeDirectory());
    auto locator = std::make_shared<WorkerLocator>(routingView, directory);
    RedirectPolicy policy(routingView, directory, locator);

    RedirectDecision decision;
    RouteOptions options;
    EXPECT_TRUE(policy.Evaluate(key, options, decision).IsOk());
    EXPECT_EQ(decision.action, RedirectAction::SERVE_LOCAL);
}

TEST(PlacementRuntimeTest, RedirectTargetUnavailableStillRedirects)
{
    const std::string key = "redirect-unavailable-key";
    const uint32_t hash = MurmurHash3_32(key);
    auto routingView = std::make_shared<RoutingView>();
    routingView->Publish(MakeSingleOwnerSnapshot(hash, WORKER_B));
    auto directory = std::make_shared<WorkerDirectory>();
    directory->Publish(MakeDirectory(WorkerAvailability::UNAVAILABLE));
    auto locator = std::make_shared<WorkerLocator>(routingView, directory);
    RedirectPolicy policy(routingView, directory, locator);

    RedirectDecision decision;
    RouteOptions options;
    EXPECT_TRUE(policy.Evaluate(key, options, decision).IsOk());
    EXPECT_EQ(decision.action, RedirectAction::REDIRECT);
    EXPECT_EQ(decision.targetEndpoint.workerId, WORKER_B);
}

TEST(PlacementRuntimeTest, RedirectScaleUpInFlightRange)
{
    const std::string key = "redirect-in-flight-key";
    const uint32_t hash = MurmurHash3_32(key);
    std::vector<RoutingOwnerEntry> owners{
        { PlacementUnit{ hash - 1, hash }, WORKER_A },
    };
    std::vector<RoutingRedirectHint> hints{
        { PlacementUnit{ hash - 1, hash }, WORKER_B, MakeAddr(2222) },
    };
    auto routingView = std::make_shared<RoutingView>();
    routingView->Publish(std::make_shared<RoutingSnapshot>(VERSION_1, std::move(owners), std::move(hints)));
    auto directory = std::make_shared<WorkerDirectory>();
    directory->Publish(MakeDirectory());
    auto locator = std::make_shared<WorkerLocator>(routingView, directory);
    auto redirectPolicy = std::make_shared<RedirectPolicy>(routingView, directory, locator);
    PlacementFacade facade(locator, redirectPolicy);

    RouteDecision route;
    RouteOptions options;
    EXPECT_TRUE(facade.LocateMetaOwner(key, options, route).IsOk());
    EXPECT_EQ(route.ownerWorkerId, WORKER_A);

    RedirectDecision decision;
    EXPECT_TRUE(facade.EvaluateRedirect(key, options, decision).IsOk());
    EXPECT_EQ(decision.action, RedirectAction::REDIRECT);
    EXPECT_EQ(decision.targetEndpoint.workerId, WORKER_B);
}

TEST(PlacementRuntimeTest, LocateMetaOwnerDoesNotUseRedirectHint)
{
    const std::string key = "locate-with-redirect-hint-key";
    const uint32_t hash = MurmurHash3_32(key);
    std::vector<RoutingOwnerEntry> owners{
        { PlacementUnit{ hash - 1, hash }, WORKER_A },
    };
    std::vector<RoutingRedirectHint> hints{
        { PlacementUnit{ hash - 1, hash }, WORKER_B, MakeAddr(2222) },
    };
    auto routingView = std::make_shared<RoutingView>();
    routingView->Publish(std::make_shared<RoutingSnapshot>(VERSION_1, std::move(owners), std::move(hints)));
    auto directory = std::make_shared<WorkerDirectory>();
    directory->Publish(MakeDirectory());
    WorkerLocator locator(routingView, directory);

    RouteDecision route;
    RouteOptions options;
    EXPECT_TRUE(locator.LocateMetaOwner(key, options, nullptr, route).IsOk());
    EXPECT_EQ(route.ownerWorkerId, WORKER_A);
    EXPECT_EQ(route.ownerEndpoint.workerId, WORKER_A);
}

TEST(PlacementRuntimeTest, RedirectScaleUpFinished)
{
    const std::string key = "redirect-finished-key";
    const uint32_t hash = MurmurHash3_32(key);
    auto routingView = std::make_shared<RoutingView>();
    routingView->Publish(MakeSingleOwnerSnapshot(hash, WORKER_B));
    auto directory = std::make_shared<WorkerDirectory>();
    directory->Publish(MakeDirectory());
    auto locator = std::make_shared<WorkerLocator>(routingView, directory);
    auto redirectPolicy = std::make_shared<RedirectPolicy>(routingView, directory, locator);
    PlacementFacade facade(locator, redirectPolicy);

    RouteOptions options;

    RedirectDecision decision;
    EXPECT_TRUE(facade.EvaluateRedirect(key, options, decision).IsOk());
    EXPECT_EQ(decision.action, RedirectAction::REDIRECT);
    EXPECT_EQ(decision.targetEndpoint.workerId, WORKER_B);
}

TEST(PlacementRuntimeTest, RedirectEvaluateCentralized)
{
    const std::string masterAddr = "127.0.0.1:9999";
    FLAGS_master_address = masterAddr;
    // The centralized master differs from the local worker, so the policy redirects to it.
    auto routingView = std::make_shared<RoutingView>();
    auto directory = std::make_shared<WorkerDirectory>();
    directory->Publish(MakeDirectory());
    auto locator = std::make_shared<WorkerLocator>(routingView, directory);
    RedirectPolicy policy(routingView, directory, locator);

    RedirectDecision decision;
    RouteOptions options;
    options.centralizedMode = true;
    EXPECT_TRUE(policy.Evaluate("any-key", options, decision).IsOk());
    EXPECT_EQ(decision.action, RedirectAction::REDIRECT);
    EXPECT_EQ(decision.targetEndpoint.address.ToString(), masterAddr);
}

TEST(PlacementRuntimeTest, PlacementFacadeQueryLocalPlacement)
{
    const std::string key = "facade-local-placement-key";
    const uint32_t hash = MurmurHash3_32(key);
    auto routingView = std::make_shared<RoutingView>();
    routingView->Publish(MakeSingleOwnerSnapshot(hash, WORKER_A));
    auto directory = std::make_shared<WorkerDirectory>();
    directory->Publish(MakeDirectory());
    auto locator = std::make_shared<WorkerLocator>(routingView, directory);
    auto redirectPolicy = std::make_shared<RedirectPolicy>(routingView, directory, locator);
    PlacementFacade facade(locator, redirectPolicy);

    LocalPlacementQuery query;
    query.objectKey = key;
    LocalPlacementDecision decision;
    EXPECT_TRUE(facade.QueryLocalPlacement(query, decision).IsOk());
    EXPECT_EQ(decision.objectKeyHash, hash);
    EXPECT_EQ(decision.routingVersion, VERSION_1);
    EXPECT_EQ(decision.placementUnit.rangeBegin, hash - 1);
    EXPECT_EQ(decision.placementUnit.rangeEnd, hash);
}

// Hint workerId is absent from the directory: the policy must fall back to the hint's raw address
// with NOT_READY availability instead of failing.
TEST(PlacementRuntimeTest, RedirectHintWorkerMissingFallsBackToRawAddress)
{
    const std::string key = "redirect-hint-unknown-worker-key";
    const uint32_t hash = MurmurHash3_32(key);
    const std::string unknownWorker = "worker-unknown";
    const HostPort hintAddr = MakeAddr(3333);
    std::vector<RoutingOwnerEntry> owners{ { PlacementUnit{ hash - 1, hash }, WORKER_A } };
    std::vector<RoutingRedirectHint> hints{ { PlacementUnit{ hash - 1, hash }, unknownWorker, hintAddr } };
    auto routingView = std::make_shared<RoutingView>();
    routingView->Publish(std::make_shared<RoutingSnapshot>(VERSION_1, std::move(owners), std::move(hints)));
    auto directory = std::make_shared<WorkerDirectory>();
    directory->Publish(MakeDirectory());
    auto locator = std::make_shared<WorkerLocator>(routingView, directory);
    RedirectPolicy policy(routingView, directory, locator);

    RedirectDecision decision;
    RouteOptions options;
    EXPECT_TRUE(policy.Evaluate(key, options, decision).IsOk());
    EXPECT_EQ(decision.action, RedirectAction::REDIRECT);
    EXPECT_EQ(decision.targetEndpoint.workerId, unknownWorker);
    EXPECT_EQ(decision.targetEndpoint.address, hintAddr);
    EXPECT_EQ(decision.targetEndpoint.availability, WorkerAvailability::NOT_READY);
}

// Directory has a local identity/address but no entry for it in the workers map: GetLocalWorker must
// synthesize a NOT_READY endpoint from the local address rather than fail.
TEST(PlacementRuntimeTest, WorkerDirectoryLocalWorkerFallsBackToAddress)
{
    const std::string localWorker = "worker-local-only";
    const HostPort localAddr = MakeAddr(4321);
    auto snapshot = std::make_shared<WorkerDirectorySnapshot>();
    snapshot->version = VERSION_1;
    snapshot->localWorkerId = localWorker;
    snapshot->localAddress = localAddr;
    // workers map intentionally omits localWorker so the fallback branch is exercised.
    snapshot->workers.emplace(WORKER_B, WorkerEndpoint{ WORKER_B, MakeAddr(2222), WorkerAvailability::READY });

    WorkerDirectory directory;
    directory.Publish(snapshot);

    WorkerEndpoint endpoint;
    EXPECT_TRUE(directory.GetLocalWorker(endpoint).IsOk());
    EXPECT_EQ(endpoint.workerId, localWorker);
    EXPECT_EQ(endpoint.address, localAddr);
    EXPECT_EQ(endpoint.availability, WorkerAvailability::NOT_READY);
}

// Locate returns K_NOT_FOUND when no owner range covers the hash (a genuine gap in the ring).
TEST(PlacementRuntimeTest, RoutingSnapshotLocateGapReturnsNotFound)
{
    std::vector<RoutingOwnerEntry> owners{
        { PlacementUnit{ 10, 100 }, WORKER_A },
        { PlacementUnit{ 200, 300 }, WORKER_B },
    };
    RoutingSnapshot snapshot(VERSION_1, std::move(owners));

    RoutingOwnerEntry entry;
    auto status = snapshot.Locate(150, entry);  // 150 sits between 100 and 200.
    EXPECT_EQ(status.GetCode(), K_NOT_FOUND);
}

// A wrapped redirect hint range must match hashes that span the 0 boundary.
TEST(PlacementRuntimeTest, FindRedirectHintWrappedRangeMatches)
{
    std::vector<RoutingOwnerEntry> owners{ { PlacementUnit{ 0, 100 }, WORKER_A } };
    std::vector<RoutingRedirectHint> hints{ { PlacementUnit{ 300, 100 }, WORKER_B, MakeAddr(2222) } };
    RoutingSnapshot snapshot(VERSION_1, std::move(owners), std::move(hints));

    RoutingRedirectHint hint;
    EXPECT_TRUE(snapshot.FindRedirectHint(50, hint));   // 50 is in (300, 100] across the wrap.
    EXPECT_EQ(hint.targetWorkerId, WORKER_B);
    EXPECT_FALSE(snapshot.FindRedirectHint(200, hint)); // 200 is outside the wrapped hint.
}

}  // namespace
}  // namespace topology
}  // namespace datasystem
