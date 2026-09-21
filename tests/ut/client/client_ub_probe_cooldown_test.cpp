/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include <atomic>
#include <limits>
#include <thread>
#include <unordered_set>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/client/object_cache/transport/data_plane/client_ub_probe_cooldown.h"

namespace datasystem::client {
namespace {
const HostPort PEER("127.0.0.1", 19101);
const HostPort OTHER("127.0.0.1", 19102);

TEST(ClientUbProbeCooldownTest, FirstErrorStartsOneSecondCooldown)
{
    ClientUbProbeCooldown cooldown;

    EXPECT_TRUE(cooldown.TryAcquire(PEER, ClientUbProbeScope::REMOTE_WORKER, 1, 10'000));
    EXPECT_FALSE(cooldown.TryAcquire(PEER, ClientUbProbeScope::REMOTE_WORKER, 1, 10'999));
    EXPECT_TRUE(cooldown.TryAcquire(PEER, ClientUbProbeScope::REMOTE_WORKER, 1, 11'000));
}

TEST(ClientUbProbeCooldownTest, ScopeDestinationAndGenerationAreIndependent)
{
    ClientUbProbeCooldown cooldown;

    EXPECT_TRUE(cooldown.TryAcquire(PEER, ClientUbProbeScope::REMOTE_WORKER, 1, 100));
    EXPECT_TRUE(cooldown.TryAcquire(PEER, ClientUbProbeScope::LOCAL_NODE, 1, 100));
    EXPECT_TRUE(cooldown.TryAcquire(OTHER, ClientUbProbeScope::REMOTE_WORKER, 1, 100));
    EXPECT_TRUE(cooldown.TryAcquire(PEER, ClientUbProbeScope::REMOTE_WORKER, 2, 100));
    EXPECT_FALSE(cooldown.TryAcquire(PEER, ClientUbProbeScope::REMOTE_WORKER, 1, 1'100));
    EXPECT_FALSE(cooldown.TryAcquire(PEER, ClientUbProbeScope::REMOTE_WORKER, 2, 1'099));
}

TEST(ClientUbProbeCooldownTest, ConcurrentErrorsAdmitOnlyOneProbe)
{
    ClientUbProbeCooldown cooldown;
    std::atomic<size_t> admitted{ 0 };
    std::vector<std::thread> threads;
    for (size_t index = 0; index < 16; ++index) {
        threads.emplace_back([&] {
            admitted.fetch_add(cooldown.TryAcquire(PEER, ClientUbProbeScope::REMOTE_WORKER, 1, 5'000));
        });
    }
    for (auto &thread : threads) {
        thread.join();
    }
    EXPECT_EQ(admitted.load(), 1u);
}

TEST(ClientUbProbeCooldownTest, OutOfOrderObservationDoesNotBypassCooldown)
{
    ClientUbProbeCooldown cooldown;

    EXPECT_TRUE(cooldown.TryAcquire(PEER, ClientUbProbeScope::REMOTE_WORKER, 1, 5'001));
    EXPECT_FALSE(cooldown.TryAcquire(PEER, ClientUbProbeScope::REMOTE_WORKER, 1, 5'000));
}

TEST(ClientUbProbeCooldownTest, ReconcileDropsDepartedDestination)
{
    ClientUbProbeCooldown cooldown;
    ASSERT_TRUE(cooldown.TryAcquire(PEER, ClientUbProbeScope::LOCAL_NODE, 1, 1'000));
    ASSERT_TRUE(cooldown.TryAcquire(PEER, ClientUbProbeScope::REMOTE_WORKER, 1, 1'000));

    cooldown.Reconcile(std::unordered_set<HostPort>{ OTHER });

    EXPECT_TRUE(cooldown.TryAcquire(PEER, ClientUbProbeScope::LOCAL_NODE, 1, 1'001));
    EXPECT_TRUE(cooldown.TryAcquire(PEER, ClientUbProbeScope::REMOTE_WORKER, 1, 1'001));
}

TEST(ClientUbProbeCooldownTest, ElapsedTimeSurvivesTimestampWrapping)
{
    ClientUbProbeCooldown cooldown;
    constexpr uint64_t NEAR_MAX = std::numeric_limits<uint64_t>::max() - 10;

    EXPECT_TRUE(cooldown.TryAcquire(PEER, ClientUbProbeScope::REMOTE_WORKER, 1, NEAR_MAX));
    EXPECT_FALSE(cooldown.TryAcquire(PEER, ClientUbProbeScope::REMOTE_WORKER, 1,
                                     std::numeric_limits<uint64_t>::max() - 1));
    EXPECT_FALSE(cooldown.TryAcquire(PEER, ClientUbProbeScope::REMOTE_WORKER, 1, 988));
    EXPECT_TRUE(cooldown.TryAcquire(PEER, ClientUbProbeScope::REMOTE_WORKER, 1, 989));
}
}  // namespace
}  // namespace datasystem::client
