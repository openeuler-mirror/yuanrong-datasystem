/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Cluster topology Runtime composition contract tests.
 */
#include "datasystem/cluster/coordination_backend/coordination_backend.h"
#include "datasystem/cluster/runtime/topology_engine.h"

#include <type_traits>
#include <utility>

#include "gtest/gtest.h"

namespace datasystem {
class EtcdStore;
}

namespace datasystem::cluster {
namespace {

template <typename T, typename = void>
struct HasPublicEventSubmit : std::false_type {};

template <typename T>
struct HasPublicEventSubmit<
    T, std::void_t<decltype(std::declval<T &>().SubmitCoordinationEvent(std::declval<CoordinationEvent>()))>>
    : std::true_type {};

template <typename T, typename = void>
struct HasPublicReadyMutation : std::false_type {};

template <typename T>
struct HasPublicReadyMutation<
    T, std::void_t<decltype(std::declval<T &>().SetReady()), decltype(std::declval<T &>().RequestScaleIn())>>
    : std::true_type {};

template <typename T, typename = void>
struct HasEtcdStoreSelection : std::false_type {};

template <typename T>
struct HasEtcdStoreSelection<T, std::void_t<decltype(std::declval<T &>().UseEtcd(std::declval<EtcdStore &>()))>>
    : std::true_type {};

template <typename T, typename = void>
struct HasUnifiedCoordinationBackendSelection : std::false_type {};

template <typename T>
struct HasUnifiedCoordinationBackendSelection<
    T,
    std::void_t<decltype(std::declval<T &>().UseUnifiedCoordinationBackends(
        std::declval<std::unique_ptr<ICoordinationBackend>>(), std::declval<std::unique_ptr<ICoordinationBackend>>()))>>
    : std::true_type {};

static_assert(!HasEtcdStoreSelection<TopologyEngine::Builder>::value,
              "Cluster topology composition must not expose concrete ETCD Store selection");
static_assert(HasUnifiedCoordinationBackendSelection<TopologyEngine::Builder>::value,
              "Unified topology composition must accept ICoordinationBackend instances");

TEST(TopologyRuntimeCompositionTest, KeepsBackendEventsAndMembershipMutationsInsideOwners)
{
    EXPECT_FALSE(HasPublicEventSubmit<TopologyEngine>::value);
    EXPECT_FALSE(HasPublicReadyMutation<TopologyEngine>::value);
    EXPECT_FALSE(std::is_copy_constructible_v<TopologyEngine>);
    EXPECT_FALSE(std::is_copy_assignable_v<TopologyEngine>);
}

}  // namespace
}  // namespace datasystem::cluster
