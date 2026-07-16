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
 * @brief In-process URMA mock backend implementation.
 * The backend provides shared-memory transfers, eventfd wakeups, and completion queues so Datasystem exercises the
 * normal UrmaManager register, send, poll, and async-event paths without RDMA hardware.
 */
#include "datasystem/common/urma_mock/urma_mock_backend.h"

#ifdef USE_URMA_MOCK

#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <csignal>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <string>
#include <vector>

#include "datasystem/common/log/log.h"
#include "datasystem/common/urma_mock/registry/import_endpoint_registry.h"
#include "datasystem/common/urma_mock/segment/memfd_resolver.h"
#include "datasystem/common/urma_mock/transport/uds_endpoint_service.h"
#include "datasystem/common/urma_mock/transport/uds_transport.h"
#include "datasystem/common/urma_mock/inject/fault_inject.h"
#include "datasystem/common/urma_mock/mock_registry.h"
#include "datasystem/common/urma_mock/objects/mock_context.h"
#include "datasystem/common/urma_mock/objects/mock_device.h"
#include "datasystem/common/urma_mock/objects/mock_jfc.h"
#include "datasystem/common/urma_mock/objects/mock_jetty.h"
#include "datasystem/common/urma_mock/objects/mock_seg.h"
#include "datasystem/common/urma_mock/objects/mock_tjetty.h"
#include "datasystem/common/urma_mock/post_send/mock_thread_pool.h"
#include "datasystem/common/util/format.h"

namespace datasystem {
namespace urma_mock {

namespace {
MockThreadPool *&MockPoolPtr()
{
    static MockThreadPool *pool = nullptr;
    return pool;
}

std::mutex &MockPoolMutex()
{
    static std::mutex *mu = new std::mutex();
    return *mu;
}

}  // namespace

MockUrmaBackend::MockUrmaBackend() = default;

MockUrmaBackend &MockUrmaBackend::Instance()
{
    static MockUrmaBackend *inst = new MockUrmaBackend();
    return *inst;
}

bool MockUrmaBackend::IsInitialized() const
{
    return initialized_.load();
}

uint64_t MockUrmaBackend::NextId()
{
    return nextId_.fetch_add(1, std::memory_order_relaxed);
}

MockThreadPool &MockUrmaBackend::Pool()
{
    std::lock_guard<std::mutex> lk(MockPoolMutex());
    auto *&pool = MockPoolPtr();
    if (pool == nullptr || pool->IsStopped()) {
        delete pool;
        pool = new MockThreadPool();
    }
    return *pool;
}

std::mutex &MockUrmaBackend::Mu()
{
    return Instance().mu_;
}

bool MockUrmaBackend::Init()
{
    auto currentPid = getpid();
    std::lock_guard<std::mutex> lk(mu_);
    if (initialized_.load()) {
        if (ownerPid_ == currentPid) {
            return true;
        }
        LOG(INFO) << "[MockUrma] Backend detected fork from pid=" << ownerPid_ << " to pid=" << currentPid
                  << ", resetting inherited state";
        ResetStateLocked();
    }

    initialized_.store(true);
    ownerPid_ = currentPid;
    // Create one mock device using the bonding prefix expected by the existing device selection path.
    auto dev = std::make_shared<MockDevice>(NextId(), "bonding_mock0");
    auto *devPtr = dev.get();
    devices_[devPtr->GetId()] = dev;
    auto *rawDev = new urma_device_t{};
    std::strncpy(rawDev->name, "bonding_mock0", sizeof(rawDev->name) - 1);
    rawDev->type = 1;
    rawDev->eid_cnt = 1;
    devPtr->SetPrivRawDev(rawDev);
    {
        auto &t = Tables();
        std::lock_guard<std::mutex> tlk(t.mu);
        t.dev[rawDev] = std::move(dev);
    }
    LOG(INFO) << "[MockUrma] Backend initialized with device: bonding_mock0";
    return true;
}

void MockUrmaBackend::ResetStateLocked()
{
    {
        auto &t = Tables();
        std::lock_guard<std::mutex> tlk(t.mu);
        for (auto &[k, v] : t.ctx) {
            delete k;
        }
        for (auto &[k, v] : t.jfce) {
            delete k;
        }
        for (auto &[k, v] : t.jfc) {
            delete k;
        }
        for (auto &[k, v] : t.jfr) {
            delete k;
        }
        for (auto &[k, v] : t.jetty) {
            delete k;
        }
        for (auto &[k, v] : t.tjetty) {
            delete k;
        }
        for (auto &[k, v] : t.tseg) {
            delete k;
        }
        for (auto &[k, v] : t.seg) { /* segs owned by tseg */
            (void)v;
        }
        for (auto &[k, v] : t.dev) {
            delete k;
        }
        t.ctx.clear();
        t.jfce.clear();
        t.jfc.clear();
        t.jfr.clear();
        t.jetty.clear();
        t.tjetty.clear();
        t.tseg.clear();
        t.seg.clear();
        t.dev.clear();
    }
    devices_.clear();
    ImportEndpointRegistry::Instance().Clear();
    ResetMockInject();
    nextId_.store(1, std::memory_order_relaxed);
    initialized_.store(false);
    ownerPid_ = 0;
}

void MockUrmaBackend::Cleanup()
{
    if (!initialized_.load()) {
        return;
    }
    // Tear down the endpoint listener if it was spawned during this process's
    // lifetime. Tests that only call ds_urma_mock_uninit otherwise leave the
    // accept thread alive at exit.
    UdsEndpointService::Instance().ShutdownListener();
    {
        std::lock_guard<std::mutex> plk(MockPoolMutex());
        auto *&pool = MockPoolPtr();
        if (pool != nullptr) {
            pool->Drain();
            pool->Shutdown();
            delete pool;
            pool = nullptr;
        }
    }
    std::lock_guard<std::mutex> lk(mu_);
    ResetStateLocked();
    LOG(INFO) << "[MockUrma] Backend cleaned up";
}

}  // namespace urma_mock
}  // namespace datasystem

#endif  // USE_URMA_MOCK
