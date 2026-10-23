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

// Orphan sweep bookkeeping tests need no hardware; handshake and send lifetime cases use the URMA mock.

#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/rdma/urma_manager.h"
#include "datasystem/common/rdma/urma_resource.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/timer.h"

namespace datasystem {
class UrmaConnectionTestAccess {
public:
    UrmaConnectionTestAccess() = delete;
    ~UrmaConnectionTestAccess() = delete;

    static bool Has(const std::string &key)
    {
        TbbUrmaConnectionMap::const_accessor accessor;
        auto &map = UrmaManager::Instance().urmaConnectionMap_;
        return map.find(accessor, key) && accessor->second != nullptr;
    }

    static void Put(const std::string &key, std::shared_ptr<UrmaConnection> connection)
    {
        TbbUrmaConnectionMap::accessor accessor;
        UrmaManager::Instance().urmaConnectionMap_.insert(accessor, key);
        accessor->second = std::move(connection);
    }

    static void MapClientEntity(const std::string &entityId, const std::string &key)
    {
        auto &manager = UrmaManager::Instance();
        std::shared_lock<std::shared_timed_mutex> lock(manager.remoteMapMutex_);
        manager.RegisterClientMapping(entityId, key);
    }

    static void ResetMaps()
    {
        auto &manager = UrmaManager::Instance();
        manager.urmaConnectionMap_.clear();
        std::lock_guard<std::mutex> lock(manager.clientIdMutex_);
        manager.clientIdMapping_.clear();
        manager.clientConnectionOwners_.clear();
    }

    static void Sweep(uint64_t nowMs)
    {
        UrmaManager::Instance().SweepOrphanClientConnections(nowMs);
    }

    static void MapClientEntityLocked(const std::string &entityId, const std::string &key)
    {
        UrmaManager::Instance().RegisterClientMapping(entityId, key);
    }

    static std::unique_lock<std::mutex> HoldClientMappings()
    {
        return std::unique_lock<std::mutex>(UrmaManager::Instance().clientIdMutex_);
    }

    static size_t OwnerCount(const std::string &key)
    {
        auto &manager = UrmaManager::Instance();
        std::lock_guard<std::mutex> lock(manager.clientIdMutex_);
        const auto it = manager.clientConnectionOwners_.find(key);
        return it == manager.clientConnectionOwners_.end() ? 0 : it->second;
    }

    static std::shared_lock<std::shared_timed_mutex> HoldMap()
    {
        return std::shared_lock<std::shared_timed_mutex>(UrmaManager::Instance().remoteMapMutex_);
    }

    static void StartSweepThread()
    {
        auto &manager = UrmaManager::Instance();
        manager.serverStop_ = false;
        manager.StartOrphanSweepThread();
    }

    static void StopSweepThread()
    {
        auto &manager = UrmaManager::Instance();
        {
            std::lock_guard<std::mutex> lock(manager.orphanSweepMutex_);
            manager.serverStop_ = true;
        }
        manager.orphanSweepCv_.notify_all();
        if (manager.orphanSweepThread_) {
            manager.orphanSweepThread_->join();
            manager.orphanSweepThread_.reset();
        }
    }

    static void SetClientMode(bool enabled)
    {
        UrmaManager::clientMode_.store(enabled);
    }

    static bool ClientMode()
    {
        return UrmaManager::clientMode_.load();
    }

    static bool HasSweepThread()
    {
        return UrmaManager::Instance().orphanSweepThread_ != nullptr;
    }

    static Status Handshake(const UrmaHandshakeReqPb &req)
    {
        return UrmaManager::Instance().ProcessHandshakePeer(req, nullptr);
    }

    static void RemoveLocalSegment(uint64_t address)
    {
        auto &manager = UrmaManager::Instance();
        std::unique_lock<std::shared_timed_mutex> lock(manager.localMapMutex_);
        manager.localSegmentMap_->erase(address);
    }

    static bool CanLockConnectionAndMapping()
    {
        auto &manager = UrmaManager::Instance();
        std::unique_lock<std::shared_timed_mutex> mapLock(manager.remoteMapMutex_, std::try_to_lock);
        std::unique_lock<std::mutex> clientLock(manager.clientIdMutex_, std::try_to_lock);
        return mapLock.owns_lock() && clientLock.owns_lock();
    }

    static std::shared_ptr<UrmaConnection> Find(const std::string &key)
    {
        TbbUrmaConnectionMap::const_accessor accessor;
        auto &map = UrmaManager::Instance().urmaConnectionMap_;
        return map.find(accessor, key) ? accessor->second : nullptr;
    }

    static Status Notify(const UrmaWriteTrace &trace)
    {
        return UrmaManager::Instance().CheckAndNotify(trace);
    }

    static void SetLastActive(UrmaConnection &connection, uint64_t ms)
    {
        connection.lastActiveMs_.store(ms, std::memory_order_relaxed);
    }
};

namespace {
constexpr uint64_t SWEEP_IDLE_THRESHOLD_S = 600;
constexpr uint64_t SWEEP_MS_PER_SECOND = 1000;

std::shared_ptr<UrmaConnection> MakeConnection(const std::string &clientId)
{
    UrmaJfrInfo info;
    info.clientId = clientId;
    info.uniqueInstanceId = "instance-" + clientId;
    return std::make_shared<UrmaConnection>(nullptr, info);
}

std::shared_ptr<UrmaConnection> PutIdleConnection(const std::string &key, const std::string &clientId, uint64_t nowMs)
{
    auto connection = MakeConnection(clientId);
    UrmaConnectionTestAccess::SetLastActive(*connection, nowMs - (SWEEP_IDLE_THRESHOLD_S + 1) * SWEEP_MS_PER_SECOND);
    UrmaConnectionTestAccess::Put(key, connection);
    return connection;
}

#ifdef USE_URMA_MOCK
UrmaHandshakeReqPb MakeHandshake(const std::string &clientId, const std::string &entityId,
                               const std::string &instanceId, uint32_t port)
{
    UrmaHandshakeReqPb req;
    req.set_eid("0123456789abcdef");
    req.set_uasid(1);
    req.add_jfr_ids(1);
    req.mutable_address()->set_host("127.0.0.1");
    req.mutable_address()->set_port(port);
    req.set_urma_instance_id(instanceId);
    req.set_client_id(clientId);
    req.set_client_entity_id(entityId);
    return req;
}
#endif

bool WaitForHook(const std::string &hook)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (inject::GetExecuteCount(hook) == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return inject::GetExecuteCount(hook) != 0;
}

class UrmaOrphanSweepTest : public testing::Test {
protected:
    void SetUp() override
    {
        clientMode_ = UrmaConnectionTestAccess::ClientMode();
        UrmaConnectionTestAccess::SetClientMode(false);
        ttl_ = FLAGS_urma_orphan_conn_ttl_s;
        batch_ = FLAGS_urma_orphan_sweep_batch;
        interval_ = FLAGS_urma_orphan_sweep_interval_s;
        FLAGS_urma_orphan_conn_ttl_s = SWEEP_IDLE_THRESHOLD_S;
        FLAGS_urma_orphan_sweep_batch = 100;
        UrmaConnectionTestAccess::ResetMaps();
    }

    void TearDown() override
    {
        UrmaConnectionTestAccess::StopSweepThread();
        UrmaConnectionTestAccess::ResetMaps();
        UrmaConnectionTestAccess::SetClientMode(clientMode_);
        FLAGS_urma_orphan_conn_ttl_s = ttl_;
        FLAGS_urma_orphan_sweep_batch = batch_;
        FLAGS_urma_orphan_sweep_interval_s = interval_;
    }

    bool clientMode_;
    uint32_t ttl_;
    uint32_t batch_;
    uint32_t interval_;
};

TEST_F(UrmaOrphanSweepTest, SweepRemovesIdleOrphanClientConnection)
{
    const auto nowMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    auto connection = PutIdleConnection("client-orphan", "client-orphan", nowMs);

    UrmaConnectionTestAccess::Sweep(nowMs);

    EXPECT_FALSE(UrmaConnectionTestAccess::Has("client-orphan"));
}

TEST_F(UrmaOrphanSweepTest, SweepKeepsRegisteredClientConnection)
{
    const auto nowMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    auto connection = PutIdleConnection("client-registered", "client-registered", nowMs);
    UrmaConnectionTestAccess::MapClientEntity("entity-registered", "client-registered");

    UrmaConnectionTestAccess::Sweep(nowMs);

    EXPECT_TRUE(UrmaConnectionTestAccess::Has("client-registered"));
}

TEST_F(UrmaOrphanSweepTest, SweepKeepsWorkerKeyedConnection)
{
    const auto nowMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    auto connection = PutIdleConnection("10.0.0.1:6666", "", nowMs);

    UrmaConnectionTestAccess::Sweep(nowMs);

    EXPECT_TRUE(UrmaConnectionTestAccess::Has("10.0.0.1:6666"));
}

TEST_F(UrmaOrphanSweepTest, SweepKeepsActiveOrphanConnection)
{
    const auto nowMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    auto fresh = MakeConnection("client-fresh");
    UrmaConnectionTestAccess::Put("client-fresh", fresh);
    auto racingTouch = MakeConnection("client-racing-touch");
    // A send racing the sweep moves lastActiveMs_ past the caller's snapshot; must count as active.
    UrmaConnectionTestAccess::SetLastActive(*racingTouch, nowMs + 1);
    UrmaConnectionTestAccess::Put("client-racing-touch", racingTouch);

    UrmaConnectionTestAccess::Sweep(nowMs);

    EXPECT_TRUE(UrmaConnectionTestAccess::Has("client-fresh"));
    EXPECT_TRUE(UrmaConnectionTestAccess::Has("client-racing-touch"));
}

TEST_F(UrmaOrphanSweepTest, CheckAndNotifyDoesNotSweep)
{
    const auto nowMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    auto connection = PutIdleConnection("client-wiring", "client-wiring", nowMs);

    ASSERT_TRUE(UrmaConnectionTestAccess::Notify(UrmaWriteTrace{}).IsOk());

    EXPECT_TRUE(UrmaConnectionTestAccess::Has("client-wiring"));
}

TEST_F(UrmaOrphanSweepTest, EmptyEntityDoesNotOwnConnection)
{
    const auto nowMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    auto connection = PutIdleConnection("data-plane", "data-plane", nowMs);
    UrmaConnectionTestAccess::MapClientEntity("", "data-plane");
    EXPECT_EQ(UrmaConnectionTestAccess::OwnerCount("data-plane"), 0u);
    UrmaConnectionTestAccess::Sweep(nowMs);
    EXPECT_FALSE(UrmaConnectionTestAccess::Has("data-plane"));
}

TEST_F(UrmaOrphanSweepTest, FormalRemovalDestroysConnectionOutsideLocks)
{
    bool destroyed = false;
    bool locksAvailable = false;
    UrmaJfrInfo info;
    info.clientId = "destruction-test";
    auto connection = std::shared_ptr<UrmaConnection>(new UrmaConnection(nullptr, info), [&](UrmaConnection *value) {
        auto check = std::async(std::launch::async, [] {
            return UrmaConnectionTestAccess::CanLockConnectionAndMapping();
        });
        locksAvailable = check.get();
        delete value;
        destroyed = true;
    });
    UrmaConnectionTestAccess::Put(info.clientId, connection);
    UrmaConnectionTestAccess::MapClientEntity("destruction-entity", info.clientId);
    connection.reset();
    EXPECT_TRUE(UrmaManager::Instance().RemoveRemoteClient(ClientKey::Intern("destruction-entity")).IsOk());
    EXPECT_TRUE(destroyed);
    EXPECT_TRUE(locksAvailable);
}

TEST_F(UrmaOrphanSweepTest, MappingReplacementAndSharedOwnersStayConsistent)
{
    UrmaConnectionTestAccess::MapClientEntity("entity-a", "old");
    UrmaConnectionTestAccess::MapClientEntity("entity-a", "old");
    EXPECT_EQ(UrmaConnectionTestAccess::OwnerCount("old"), 1u);
    UrmaConnectionTestAccess::MapClientEntity("entity-b", "old");
    UrmaConnectionTestAccess::MapClientEntity("entity-a", "new");
    EXPECT_EQ(UrmaConnectionTestAccess::OwnerCount("old"), 1u);
    EXPECT_EQ(UrmaConnectionTestAccess::OwnerCount("new"), 1u);
    UrmaConnectionTestAccess::Put("old", MakeConnection("old"));
    UrmaConnectionTestAccess::MapClientEntity("entity-c", "old");
    EXPECT_TRUE(UrmaManager::Instance().RemoveRemoteClient(ClientKey::Intern("entity-b")).IsOk());
    EXPECT_TRUE(UrmaConnectionTestAccess::Has("old"));
    EXPECT_TRUE(UrmaManager::Instance().RemoveRemoteClient(ClientKey::Intern("entity-c")).IsOk());
    EXPECT_FALSE(UrmaConnectionTestAccess::Has("old"));
    EXPECT_EQ(UrmaConnectionTestAccess::OwnerCount("old"), 0u);
    UrmaConnectionTestAccess::MapClientEntity("entity-a", "third");
    EXPECT_EQ(UrmaConnectionTestAccess::OwnerCount("new"), 0u);
}

TEST_F(UrmaOrphanSweepTest, BatchCountsOnlyOrphansAndEventuallyDrainsThem)
{
    FLAGS_urma_orphan_sweep_batch = 2;
    const auto nowMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    for (size_t i = 0; i < 20; ++i) {
        const auto key = "registered-" + std::to_string(i);
        auto connection = PutIdleConnection(key, key, nowMs);
        UrmaConnectionTestAccess::MapClientEntity("entity-" + key, key);
    }
    for (size_t i = 0; i < 5; ++i) {
        const auto key = "orphan-" + std::to_string(i);
        auto connection = PutIdleConnection(key, key, nowMs);
    }
    for (size_t remaining : {3u, 1u, 0u}) {
        UrmaConnectionTestAccess::Sweep(nowMs);
        size_t count = 0;
        for (size_t i = 0; i < 5; ++i) {
            count += UrmaConnectionTestAccess::Has("orphan-" + std::to_string(i));
        }
        EXPECT_EQ(count, remaining);
        for (size_t i = 0; i < 20; ++i) {
            EXPECT_TRUE(UrmaConnectionTestAccess::Has("registered-" + std::to_string(i)));
        }
    }
}

TEST_F(UrmaOrphanSweepTest, SweepKeepsInflightAndHonorsDisable)
{
    const auto nowMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    auto connection = PutIdleConnection("inflight", "inflight", nowMs);
    ASSERT_TRUE(connection->AcquireInflightSlot(1000000).IsOk());
    UrmaConnectionTestAccess::Sweep(nowMs);
    EXPECT_TRUE(UrmaConnectionTestAccess::Has("inflight"));
    connection->ReleaseInflightSlot();
    FLAGS_urma_orphan_conn_ttl_s = 0;
    UrmaConnectionTestAccess::Sweep(nowMs);
    EXPECT_TRUE(UrmaConnectionTestAccess::Has("inflight"));
    FLAGS_urma_orphan_conn_ttl_s = SWEEP_IDLE_THRESHOLD_S;
    UrmaConnectionTestAccess::Sweep(nowMs);
    EXPECT_FALSE(UrmaConnectionTestAccess::Has("inflight"));
}

TEST_F(UrmaOrphanSweepTest, SweepWaitsForMappingRegistration)
{
    const auto nowMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    auto connection = PutIdleConnection("registering", "registering", nowMs);
    auto lock = UrmaConnectionTestAccess::HoldMap();
    std::promise<void> started;
    auto ready = started.get_future();
    auto sweep = std::async(std::launch::async, [&] {
        started.set_value();
        UrmaConnectionTestAccess::Sweep(nowMs);
    });
    ready.wait();
    EXPECT_EQ(sweep.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout);
    UrmaConnectionTestAccess::MapClientEntityLocked("entity-registering", "registering");
    lock.unlock();
    sweep.get();
    EXPECT_TRUE(UrmaConnectionTestAccess::Has("registering"));
}

TEST_F(UrmaOrphanSweepTest, SweepThreadStopsDuringLongInterval)
{
    const std::string hook = "UrmaManager.OrphanSweepBeforeWait";
    ASSERT_TRUE(inject::Set(hook, "return()").IsOk());
    FLAGS_urma_orphan_sweep_interval_s = 3600;
    UrmaConnectionTestAccess::StartSweepThread();
    EXPECT_TRUE(WaitForHook(hook));
    // Stop takes the wait mutex, so it cannot notify before wait_for releases that mutex.
    UrmaConnectionTestAccess::StopSweepThread();
    EXPECT_TRUE(inject::Clear(hook).IsOk());
}

TEST_F(UrmaOrphanSweepTest, ClientDoesNotStartSweepThread)
{
    UrmaConnectionTestAccess::SetClientMode(true);
    UrmaConnectionTestAccess::StartSweepThread();
    EXPECT_FALSE(UrmaConnectionTestAccess::HasSweepThread());
}

TEST_F(UrmaOrphanSweepTest, BackgroundTickSweepsAndStopsWhileMapIsBusy)
{
    const std::string hook = "UrmaManager.OrphanSweepWaitingForMap";
    ASSERT_TRUE(inject::Set(hook, "return()").IsOk());
    FLAGS_urma_orphan_sweep_interval_s = 1;
    const auto nowMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    auto connection = PutIdleConnection("background", "background", nowMs);
    auto lock = UrmaConnectionTestAccess::HoldMap();
    UrmaConnectionTestAccess::StartSweepThread();
    EXPECT_TRUE(WaitForHook(hook));
    // Keep the map lock held throughout Stop: no dependency on foreground completion is allowed.
    UrmaConnectionTestAccess::StopSweepThread();
    lock.unlock();
    EXPECT_TRUE(UrmaConnectionTestAccess::Has("background"));
    EXPECT_TRUE(inject::Clear(hook).IsOk());
}

TEST_F(UrmaOrphanSweepTest, BackgroundTickRemovesOrphanAndPreservesHeldReference)
{
    FLAGS_urma_orphan_sweep_interval_s = 1;
    const auto nowMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    auto connection = PutIdleConnection("held", "held", nowMs);
    UrmaConnectionTestAccess::StartSweepThread();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (UrmaConnectionTestAccess::Has("held") && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    UrmaConnectionTestAccess::StopSweepThread();
    EXPECT_FALSE(UrmaConnectionTestAccess::Has("held"));
    EXPECT_EQ(connection->GetUrmaJfrInfo().clientId, "held");
}

#ifdef USE_URMA_MOCK
TEST_F(UrmaOrphanSweepTest, FormalRemovalWaitsUntilHandshakeRegistersOwnership)
{
    auto &manager = UrmaManager::Instance();
    ASSERT_TRUE(manager.Init(HostPort("127.0.0.1", 0)).IsOk());
    auto req = MakeHandshake("shared-client", "old-entity", "handshake-removal-test", 29301);
    ASSERT_TRUE(UrmaConnectionTestAccess::Handshake(req).IsOk());

    const std::string hook = "UrmaManager.HandshakeBeforeClientMapping";
    ASSERT_TRUE(inject::Set(hook, "return()").IsOk());
    auto mappingLock = UrmaConnectionTestAccess::HoldClientMappings();
    req.set_client_entity_id("new-entity");
    auto handshake = std::async(std::launch::async, [&] { return UrmaConnectionTestAccess::Handshake(req); });
    EXPECT_TRUE(WaitForHook(hook));
    std::promise<void> removing;
    auto entered = removing.get_future();
    const std::string removalHook = "UrmaManager.RemoveClientMapLocked";
    EXPECT_TRUE(inject::Set(removalHook, "return()").IsOk());
    auto removal = std::async(std::launch::async, [&] {
        removing.set_value();
        return manager.RemoveRemoteClient(ClientKey::Intern("old-entity"));
    });
    entered.wait();
    EXPECT_EQ(removal.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
    // With a shared map lock, removal reaches this hook even though the mapping mutex is occupied.
    EXPECT_EQ(inject::GetExecuteCount(removalHook), 0u);
    mappingLock.unlock();
    EXPECT_TRUE(handshake.get().IsOk());
    EXPECT_TRUE(removal.get().IsOk());
    EXPECT_EQ(inject::GetExecuteCount(removalHook), 1u);
    EXPECT_TRUE(inject::Clear(removalHook).IsOk());
    EXPECT_TRUE(UrmaConnectionTestAccess::Has("shared-client"));
    EXPECT_EQ(UrmaConnectionTestAccess::OwnerCount("shared-client"), 1u);
    EXPECT_TRUE(manager.RemoveRemoteClient(ClientKey::Intern("new-entity")).IsOk());
    EXPECT_TRUE(UrmaConnectionTestAccess::Handshake(req).IsOk());
    EXPECT_TRUE(UrmaConnectionTestAccess::Has("shared-client"));
    EXPECT_TRUE(inject::Clear(hook).IsOk());
    UrmaConnectionTestAccess::StopSweepThread();
}

TEST_F(UrmaOrphanSweepTest, PostedWriteRetainsImportedResourcesAfterFormalRemoval)
{
    auto &manager = UrmaManager::Instance();
    ASSERT_TRUE(manager.Init(HostPort("127.0.0.1", 0)).IsOk());
    std::vector<uint8_t> payload(4096, 0x5a);
    const auto address = reinterpret_cast<uint64_t>(payload.data());
    auto req = MakeHandshake("sending-client", "sending-entity", "send-lifetime-test", 29302);
    ASSERT_TRUE(manager.RegisterSegment(address, payload.size()).IsOk());
    ASSERT_TRUE(manager.GetSegmentInfo(address, *req.add_seg_infos()).IsOk());
    ASSERT_TRUE(UrmaConnectionTestAccess::Handshake(req).IsOk());
    auto connection = UrmaConnectionTestAccess::Find(req.client_id());
    ASSERT_NE(connection, nullptr);
    ASSERT_NE(connection->GetTargetJetty(), nullptr);
    UrmaRemoteSegmentMap::const_accessor remoteSegment;
    ASSERT_TRUE(connection->GetRemoteSeg(address, remoteSegment).IsOk());
    ASSERT_NE(remoteSegment->second->Raw(), nullptr);
    remoteSegment.release();
    std::weak_ptr<UrmaConnection> lifetime = connection;
    connection.reset();

    const std::string postHook = "UrmaManager.UrmaWriteAfterPost";
    ASSERT_TRUE(inject::Set(postHook, "1*pause()").IsOk());
    UrmaRemoteAddrPb remote;
    remote.mutable_request_address()->CopyFrom(req.address());
    remote.set_client_id(req.client_id());
    remote.set_seg_va(address);
    std::vector<uint64_t> eventKeys;
    auto writer = std::async(std::launch::async, [&] {
        return manager.UrmaWritePayload(remote, address, payload.size(), address, 0, 64, 0,
                                        INVALID_CHIP_ID, INVALID_CHIP_ID, false, eventKeys);
    });
    const bool posted = WaitForHook(postHook);
    auto removal = std::async(std::launch::async, [&] {
        return manager.RemoveRemoteClient(ClientKey::Intern(req.client_entity_id()));
    });
    const auto removedWhileSending = removal.wait_for(std::chrono::seconds(2));
    EXPECT_EQ(removedWhileSending, std::future_status::ready);
    if (removedWhileSending == std::future_status::ready) {
        EXPECT_FALSE(UrmaConnectionTestAccess::Has(req.client_id()));
        auto held = lifetime.lock();
        EXPECT_NE(held, nullptr);
        if (held != nullptr) {
            EXPECT_NE(held->GetTargetJetty(), nullptr);
            UrmaRemoteSegmentMap::const_accessor segment;
            EXPECT_TRUE(held->GetRemoteSeg(address, segment).IsOk());
        }
    }
    EXPECT_TRUE(inject::Clear(postHook).IsOk());
    const auto writeStatus = writer.get();
    EXPECT_TRUE(removal.get().IsOk());
    EXPECT_TRUE(posted);
    EXPECT_TRUE(writeStatus.IsOk()) << writeStatus.ToString();
    EXPECT_FALSE(eventKeys.empty());
    for (const auto key : eventKeys) {
        EXPECT_TRUE(manager.WaitToFinish(key, 2000).IsOk());
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!lifetime.expired() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_TRUE(lifetime.expired());
    UrmaConnectionTestAccess::RemoveLocalSegment(address);
}

#endif

} // namespace
} // namespace datasystem
