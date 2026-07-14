/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Cluster topology Observer-backed Router client.
 */
#include "datasystem/router_client.h"

#include <algorithm>
#include <chrono>
#include <mutex>

#include "datasystem/cluster/coordination_backend/etcd_coordination_backend.h"
#include "datasystem/cluster/repository/topology_repository.h"
#include "datasystem/cluster/runtime/topology_observer.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/uuid_generator.h"

namespace datasystem {
namespace {
constexpr size_t BATCH_MEMBER_LIMIT = 100;
constexpr auto ROUTER_SHUTDOWN_BUDGET = std::chrono::seconds(5);

void RotateRandomly(std::vector<std::string> &addresses, RandomData &random)
{
    if (addresses.size() > 1) {
        auto offset = random.GetRandomIndex(addresses.size());
        std::rotate(addresses.begin(), addresses.begin() + offset, addresses.end());
    }
}
}  // namespace

struct RouterClient::Impl {
    std::shared_ptr<EtcdStore> store;
    std::unique_ptr<cluster::TopologyKeyHelper> keys;
    std::unique_ptr<cluster::EtcdCoordinationBackend> backend;
    std::unique_ptr<cluster::TopologyRepository> repository;
    std::unique_ptr<cluster::TopologyReader> reader;
    std::unique_ptr<cluster::TopologyObserver> observer;
    RandomData random;
    // Protects random while candidate groups are shuffled concurrently.
    std::mutex randomMutex;
};

RouterClient::RouterClient(const std::string &azName, const std::string &etcdAddress, const SensitiveValue &etcdCa,
                           const SensitiveValue &etcdCert, const SensitiveValue &etcdKey,
                           const std::string &etcdDNSName)
    : azName_(azName),
      etcdAddress_(etcdAddress),
      etcdCa_(etcdCa),
      etcdCert_(etcdCert),
      etcdKey_(etcdKey),
      etcdDNSName_(etcdDNSName)
{
}

RouterClient::~RouterClient()
{
    std::lock_guard<std::mutex> lock(lifecycleMutex_);
    if (impl_ == nullptr) {
        return;
    }
    if (impl_->observer != nullptr) {
        auto rc = impl_->observer->Shutdown(std::chrono::steady_clock::now() + ROUTER_SHUTDOWN_BUDGET);
        if (rc.IsError()) {
            LOG(ERROR) << "RouterClient cannot safely destroy the cluster topology Observer: " << rc.ToString();
            (void)impl_.release();
            return;
        }
    }
    if (impl_->backend != nullptr) {
        (void)impl_->backend->ShutdownEventSources();
        (void)impl_->backend->Shutdown();
    }
}

Status RouterClient::Init()
{
    std::lock_guard<std::mutex> lock(lifecycleMutex_);
    if (impl_ != nullptr) {
        return Status::OK();
    }
    auto runtime = std::make_unique<Impl>();
    RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::Create(azName_, runtime->keys));
    runtime->store = std::make_shared<EtcdStore>(etcdAddress_, etcdCa_.GetData(), etcdCert_, etcdKey_, etcdDNSName_);
    RETURN_IF_NOT_OK(runtime->store->Init());
    RETURN_IF_NOT_OK(
        runtime->store->CreateTableWithExactPrefix(runtime->keys->TopologyTable(), runtime->keys->TopologyTable()));
    runtime->backend = std::make_unique<cluster::EtcdCoordinationBackend>(runtime->store.get());
    runtime->repository = std::make_unique<cluster::TopologyRepository>(*runtime->backend, *runtime->keys);
    runtime->reader = std::make_unique<cluster::TopologyReader>(*runtime->repository);
    runtime->observer =
        std::make_unique<cluster::TopologyObserver>(*runtime->backend, *runtime->reader, *runtime->keys);
    RETURN_IF_NOT_OK(runtime->observer->Start());
    impl_ = std::move(runtime);
    return Status::OK();
}

Status RouterClient::GetWorkerCandidates(const std::string &targetHost, std::vector<std::string> &addresses) const
{
    addresses.clear();
    CHECK_FAIL_RETURN_STATUS(impl_ != nullptr && impl_->observer != nullptr, K_NOT_READY,
                             "RouterClient cluster topology Observer is not initialized");
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    RETURN_IF_NOT_OK(impl_->observer->GetSnapshot(snapshot));
    std::vector<std::string> preferred;
    std::vector<std::string> remaining;
    for (const auto &member : snapshot->Members()) {
        if (member.state != cluster::MemberState::ACTIVE) {
            continue;
        }
        HostPort endpoint;
        RETURN_IF_NOT_OK(endpoint.ParseString(member.identity.address));
        (endpoint.Host() == targetHost ? preferred : remaining).emplace_back(member.identity.address);
    }
    CHECK_FAIL_RETURN_STATUS(!preferred.empty() || !remaining.empty(), K_NOT_FOUND,
                             "cluster topology has no ACTIVE Router candidate");
    {
        std::lock_guard<std::mutex> lock(impl_->randomMutex);
        RotateRandomly(preferred, impl_->random);
        RotateRandomly(remaining, impl_->random);
    }
    addresses.reserve(preferred.size() + remaining.size());
    addresses.insert(addresses.end(), preferred.begin(), preferred.end());
    addresses.insert(addresses.end(), remaining.begin(), remaining.end());
    return Status::OK();
}

Status RouterClient::GetWorkerAddrByWorkerId(const std::vector<std::string> &memberIds,
                                             std::vector<std::string> &memberAddresses) const
{
    CHECK_FAIL_RETURN_STATUS(!memberIds.empty() && memberIds.size() <= BATCH_MEMBER_LIMIT, K_INVALID,
                             "RouterClient member id batch is invalid");
    CHECK_FAIL_RETURN_STATUS(std::all_of(memberIds.begin(), memberIds.end(), [](const auto &id) {
                                 return id.size() == UUID_SIZE;
                             }),
                             K_INVALID, "RouterClient member id must be 16 binary bytes");
    CHECK_FAIL_RETURN_STATUS(impl_ != nullptr && impl_->observer != nullptr, K_NOT_READY,
                             "RouterClient cluster topology Observer is not initialized");
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    RETURN_IF_NOT_OK(impl_->observer->GetSnapshot(snapshot));
    bool found = false;
    std::vector<std::string> resolved;
    resolved.reserve(memberIds.size());
    for (const auto &id : memberIds) {
        const cluster::Member *member = nullptr;
        if (snapshot->FindMemberById(id, member).IsOk()) {
            resolved.emplace_back(member->identity.address);
            found = true;
        } else {
            resolved.emplace_back();
        }
    }
    CHECK_FAIL_RETURN_STATUS(found, K_NOT_FOUND, "RouterClient member ids are absent from cluster topology");
    memberAddresses = std::move(resolved);
    return Status::OK();
}

}  // namespace datasystem
