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

#include "datasystem/common/urma_mock/urma_mock_backend.h"

#ifdef USE_URMA_MOCK

#include <sys/eventfd.h>
#include <unistd.h>

#include <charconv>
#include <chrono>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/urma_mock/inject/fault_inject.h"
#include "datasystem/common/urma_mock/objects/mock_context.h"
#include "datasystem/common/urma_mock/objects/mock_device.h"
#include "datasystem/common/urma_mock/objects/mock_jfc.h"
#include "datasystem/common/urma_mock/objects/mock_jetty.h"
#include "datasystem/common/urma_mock/objects/mock_seg.h"
#include "datasystem/common/urma_mock/objects/mock_tjetty.h"
#include "datasystem/common/urma_mock/mock_registry.h"
#include "datasystem/common/urma_mock/post_send/post_send_wr.h"
#include "datasystem/common/urma_mock/segment/segment_memory.h"
#include "datasystem/common/util/raii.h"

#include <unistd.h>

namespace datasystem {
namespace urma_mock {

namespace {
constexpr uint32_t K_DEFAULT_JFC_DEPTH = 1024;
constexpr bool K_NON_OWNER_SEG = false;
constexpr uint32_t K_CONTEXT_ID_BITS = 12;
constexpr uint32_t K_CONTEXT_ID_MASK = (1U << K_CONTEXT_ID_BITS) - 1;
const std::string K_REMOTE_PLACEHOLDER_SHM_NAME = "/local-only:remote-placeholder";

uint64_t TokenFromKey(const std::string &key)
{
    uint64_t token = 0;
    auto [ptr, ec] = std::from_chars(key.data(), key.data() + key.size(), token);
    return (ec == std::errc() && ptr == key.data() + key.size()) ? token : 0;
}

uint32_t WireUasid(uint64_t contextId)
{
    // Context ids are process-local in the mock backend. Mix in the process id
    // so cross-process segment endpoint keys remain distinct under parallel ST.
    uint32_t pidPart = static_cast<uint32_t>(::getpid()) & ((1U << (32 - K_CONTEXT_ID_BITS)) - 1);
    return (pidPart << K_CONTEXT_ID_BITS) | (static_cast<uint32_t>(contextId) & K_CONTEXT_ID_MASK);
}

void FillRawSeg(urma_target_seg_t *raw, uint64_t size, void *ptr, const std::string &key, uint64_t wireVa = 0)
{
    raw->seg.len = size;
    raw->seg.priv = ptr;
    raw->seg.ubva.va = wireVa == 0 ? reinterpret_cast<uint64_t>(ptr) : wireVa;
    raw->seg.token_id = TokenFromKey(key);
}

void FillRawSegContext(urma_target_seg_t *raw, MockContext *ctx)
{
    if (raw == nullptr || ctx == nullptr) {
        return;
    }
    raw->seg.ubva.eid = StableMockDeviceEid();
    raw->seg.ubva.uasid = WireUasid(ctx->GetId());
}

MockSeg *RegisterImportedSeg(MockUrmaBackend &backend, MockContext *ctx, const std::string &key,
                             SegmentBacking &backing, const ImportEndpoint *ep, urma_target_seg_t **rawOut)
{
    std::lock_guard<std::mutex> lk(MockUrmaBackend::Mu());
    auto seg =
        std::make_shared<MockSeg>(backend.NextId(), ctx, backing.GetSize(), key, backing.GetShmName(),
                                  backing.GetShmFd(), backing.GetPtr(), K_NON_OWNER_SEG, backing.GetMappingOwner());
    backing.ReleaseOwnership();
    if (backing.GetMemfdFd() >= 0) {
        seg->SetMemfdFd(backing.GetMemfdFd());
    }
    seg->SetMemfdOffset(backing.GetMemfdOffset());
    seg->SetIsRemote(true);
    seg->SetRemoteVa(backing.GetWireVa());
    if (ep != nullptr) {
        seg->SetRemoteClientId(ep->clientId);
        seg->SetRemoteHost(ep->host);
        seg->SetRemotePort(ep->port);
    }
    auto *segPtr = seg.get();
    ctx->RegisterSeg(seg);
    auto *raw = new urma_target_seg_t{};
    FillRawSeg(raw, backing.GetSize(), backing.GetPtr(), key, backing.GetWireVa());
    FillRawSegContext(raw, ctx);
    raw->priv = segPtr;
    {
        auto &t = Tables();
        std::lock_guard<std::mutex> tlk(t.mu);
        t.tseg[raw] = seg;
        t.seg[&raw->seg] = seg;
    }
    if (rawOut != nullptr) {
        *rawOut = raw;
    }
    return segPtr;
}

MockSeg *CreateRemotePlaceholder(MockUrmaBackend &backend, MockContext *ctx, uint64_t size, const std::string &key,
                                 uint64_t remoteVa, const ImportEndpoint &ep, urma_target_seg_t **rawOut)
{
    std::lock_guard<std::mutex> lk(MockUrmaBackend::Mu());
    const uint64_t wireVa = remoteVa != 0 ? remoteVa : ep.va;
    auto seg = std::make_shared<MockSeg>(backend.NextId(), ctx, size, key, K_REMOTE_PLACEHOLDER_SHM_NAME, -1, nullptr,
                                         K_NON_OWNER_SEG);
    seg->SetIsRemote(true);
    seg->SetRemoteVa(wireVa);
    seg->SetRemoteClientId(ep.clientId);
    seg->SetRemoteHost(ep.host);
    seg->SetRemotePort(ep.port);
    auto *segPtr = seg.get();
    ctx->RegisterSeg(seg);
    auto *raw = new urma_target_seg_t{};
    FillRawSeg(raw, size, nullptr, key, wireVa);
    raw->priv = segPtr;
    {
        auto &t = Tables();
        std::lock_guard<std::mutex> tlk(t.mu);
        t.tseg[raw] = seg;
        t.seg[&raw->seg] = seg;
    }
    if (rawOut != nullptr) {
        *rawOut = raw;
    }
    return segPtr;
}
}  // namespace

// ----- Device accessors -----

std::vector<MockDevice *> MockUrmaBackend::GetDevices()
{
    std::lock_guard<std::mutex> lk(mu_);
    std::vector<MockDevice *> out;
    out.reserve(devices_.size());
    for (auto &[id, dev] : devices_) {
        out.push_back(dev.get());
    }
    return out;
}

MockDevice *MockUrmaBackend::GetDeviceByName(const std::string &name)
{
    std::lock_guard<std::mutex> lk(mu_);
    for (auto &[id, dev] : devices_) {
        if (dev->GetName() == name) {
            return dev.get();
        }
    }
    return nullptr;
}

// ----- Context factory -----

std::pair<urma_context_t *, MockContext *> MockUrmaBackend::CreateContextHandle(MockDevice *dev)
{
    if (dev == nullptr) {
        return { nullptr, nullptr };
    }
    std::lock_guard<std::mutex> lk(mu_);
    auto ctx = std::make_shared<MockContext>(NextId(), dev);
    auto *ctxPtr = ctx.get();
    dev->RegisterContext(ctx);
    auto *raw = new urma_context_t{};
    raw->async_fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (raw->async_fd < 0) {
        LOG(ERROR) << "[MockUrma] failed to create async event fd: " << errno;
        dev->UnregisterContext(ctxPtr->GetId());
        delete raw;
        return { nullptr, nullptr };
    }
    raw->dev_type = 1;
    raw->uasid = WireUasid(ctxPtr->GetId());
    raw->eid = StableMockDeviceEid();
    {
        auto &t = Tables();
        std::lock_guard<std::mutex> tlk(t.mu);
        t.ctx[raw] = ctx;
    }
    LOG(INFO) << "[MockUrma] create context id=" << ctxPtr->GetId();
    return { raw, ctxPtr };
}

void MockUrmaBackend::DeleteContext(MockContext *ctx)
{
    if (ctx == nullptr) {
        return;
    }
    std::lock_guard<std::mutex> lk(mu_);
    auto *dev = ctx->GetDevice();
    // (contextMap_ is private to MockDevice; we walk via the device's accessor below.)
    if (dev != nullptr) {
        auto *cur = dev->FindContext(ctx->GetId());
        if (cur == ctx) {
            dev->UnregisterContext(ctx->GetId());
        }
    }
    {
        auto &t = Tables();
        std::lock_guard<std::mutex> tlk(t.mu);
        for (auto it = t.ctx.begin(); it != t.ctx.end();) {
            if (it->second.get() == ctx) {
                if (it->first->async_fd >= 0) {
                    static_cast<void>(close(it->first->async_fd));
                }
                delete it->first;
                it = t.ctx.erase(it);
            } else {
                ++it;
            }
        }
    }
}

// ----- JFC factory -----

std::pair<urma_jfc_t *, MockJfc *> MockUrmaBackend::CreateJfcHandle(const std::shared_ptr<MockContext> &ctx)
{
    if (ctx == nullptr) {
        return { nullptr, nullptr };
    }
    std::lock_guard<std::mutex> lk(mu_);
    auto jfc = std::make_shared<MockJfc>(NextId(), ctx);
    auto *jfcPtr = jfc.get();
    ctx->RegisterJfc(jfc);
    auto *raw = new urma_jfc_t{};
    raw->jfc_id.id = jfcPtr->GetId();
    raw->depth = K_DEFAULT_JFC_DEPTH;
    {
        auto &t = Tables();
        std::lock_guard<std::mutex> tlk(t.mu);
        t.jfc[raw] = jfc;
    }
    return { raw, jfcPtr };
}

MockJfc *MockUrmaBackend::CreateJfc(const std::shared_ptr<MockContext> &ctx)
{
    return CreateJfcHandle(ctx).second;
}

void MockUrmaBackend::DeleteJfc(MockJfc *jfc)
{
    if (jfc == nullptr) {
        return;
    }
    std::lock_guard<std::mutex> lk(mu_);
    if (auto ctx = jfc->LockContext()) {
        ctx->UnregisterJfc(jfc->GetId());
    }
    {
        auto &t = Tables();
        std::lock_guard<std::mutex> tlk(t.mu);
        for (auto it = t.jfc.begin(); it != t.jfc.end();) {
            if (it->second.get() == jfc) {
                delete it->first;
                it = t.jfc.erase(it);
            } else {
                ++it;
            }
        }
    }
}

// ----- Seg factory -----

MockSeg *MockUrmaBackend::RegisterSeg(MockContext *ctx, uint64_t size, const std::string &key, uint64_t businessVa,
                                      urma_target_seg_t **rawOut)
{
    if (ctx == nullptr || size == 0) {
        return nullptr;
    }
    const auto &segmentMemory = SegmentMemoryManager::Instance();
    SegmentBacking backing;
    if (!segmentMemory.CreateLocal(size, key, businessVa, backing)) {
        return nullptr;
    }
    std::lock_guard<std::mutex> lk(mu_);
    auto seg = std::make_shared<MockSeg>(NextId(), ctx, size, key, backing.GetShmName(), backing.GetShmFd(),
                                         backing.GetPtr(), backing.IsOwner(), backing.GetMappingOwner());
    backing.ReleaseOwnership();
    if (backing.GetMemfdFd() >= 0) {
        seg->SetMemfdFd(backing.GetMemfdFd());
        seg->SetMemfdOffset(backing.GetMemfdOffset());
    }
    seg->SetRemoteVa(backing.GetWireVa());
    auto *segPtr = seg.get();
    urma_target_seg_t *raw = nullptr;
    bool ctxRegistered = false;
    datasystem::RaiiPlus registrationGuard([&]() {
        auto &t = Tables();
        std::lock_guard<std::mutex> tlk(t.mu);
        if (raw != nullptr) {
            t.seg.erase(&raw->seg);
            t.tseg.erase(raw);
            delete raw;
            raw = nullptr;
        }
        if (ctxRegistered) {
            ctx->UnregisterSeg(segPtr->GetId());
        }
    });
    ctx->RegisterSeg(seg);
    ctxRegistered = true;
    raw = new urma_target_seg_t{};
    FillRawSeg(raw, size, backing.GetPtr(), key, backing.GetWireVa());
    FillRawSegContext(raw, ctx);
    raw->priv = segPtr;
    {
        auto &t = Tables();
        std::lock_guard<std::mutex> tlk(t.mu);
        t.tseg[raw] = seg;
        t.seg[&raw->seg] = seg;
    }
    if (rawOut != nullptr) {
        *rawOut = raw;
    }
    registrationGuard.ClearAllTask();
    return segPtr;
}

void MockUrmaBackend::UnregisterSeg(MockSeg *seg)
{
    if (seg == nullptr) {
        return;
    }
    std::lock_guard<std::mutex> lk(mu_);
    if (auto *ctx = seg->GetContext()) {
        ctx->UnregisterSeg(seg->GetId());
    }
    {
        auto &t = Tables();
        std::lock_guard<std::mutex> tlk(t.mu);
        for (auto it = t.tseg.begin(); it != t.tseg.end();) {
            if (it->second.get() == seg) {
                t.seg.erase(&it->first->seg);
                delete it->first;
                it = t.tseg.erase(it);
            } else {
                ++it;
            }
        }
    }
    // Destructor of MockSeg unlinks + munmaps.
}

// ----- Jetty factory -----

std::pair<urma_jetty_t *, MockJetty *> MockUrmaBackend::CreateJettyHandle(MockContext *ctx, MockJfc *sendJfc)
{
    if (ctx == nullptr) {
        return { nullptr, nullptr };
    }
    std::lock_guard<std::mutex> lk(mu_);
    auto jetty = std::make_shared<MockJetty>(NextId(), ctx, sendJfc);
    auto *jtPtr = jetty.get();
    ctx->RegisterJetty(jetty);
    auto *raw = new urma_jetty_t{};
    raw->jetty_id.id = jtPtr->GetId();
    raw->state = URMA_JETTY_STATE_ACTIVE;
    {
        auto &t = Tables();
        std::lock_guard<std::mutex> tlk(t.mu);
        t.jetty[raw] = jetty;
    }
    return { raw, jtPtr };
}

void MockUrmaBackend::DeleteJetty(MockJetty *jetty)
{
    if (jetty == nullptr) {
        return;
    }
    std::lock_guard<std::mutex> lk(mu_);
    if (auto *ctx = jetty->GetContext()) {
        ctx->UnregisterJetty(jetty->GetId());
    }
    {
        auto &t = Tables();
        std::lock_guard<std::mutex> tlk(t.mu);
        for (auto it = t.jetty.begin(); it != t.jetty.end();) {
            if (it->second.get() == jetty) {
                delete it->first;
                it = t.jetty.erase(it);
            } else {
                ++it;
            }
        }
    }
}

// ----- Target jetty (import) factory -----

std::pair<urma_target_jetty_t *, MockTjetty *> MockUrmaBackend::ImportJettyHandle(MockContext *ctx, MockSeg *remoteSeg,
                                                                                  MockJfc *remoteRecvJfc,
                                                                                  const std::string &token)
{
    if (ctx == nullptr) {
        return { nullptr, nullptr };
    }
    std::lock_guard<std::mutex> lk(mu_);
    auto tjt = std::make_shared<MockTjetty>(NextId(), ctx, remoteSeg, remoteRecvJfc, token);
    auto *tjtPtr = tjt.get();
    ctx->RegisterTjetty(tjt);
    auto *raw = new urma_target_jetty_t{};
    raw->jetty_id.id = tjtPtr->GetId();
    {
        auto &t = Tables();
        std::lock_guard<std::mutex> tlk(t.mu);
        t.tjetty[raw] = tjt;
    }
    return { raw, tjtPtr };
}

void MockUrmaBackend::UnimportJetty(MockTjetty *tjetty)
{
    if (tjetty == nullptr) {
        return;
    }
    std::lock_guard<std::mutex> lk(mu_);
    {
        auto &t = Tables();
        std::lock_guard<std::mutex> tlk(t.mu);
        for (auto &[rawJetty, mockJetty] : t.jetty) {
            (void)rawJetty;
            if (mockJetty != nullptr && mockJetty->GetDstTjetty() == tjetty) {
                mockJetty->SetDstTjetty(nullptr);
            }
        }
        for (auto it = t.tjetty.begin(); it != t.tjetty.end();) {
            if (it->second.get() == tjetty) {
                delete it->first;
                it = t.tjetty.erase(it);
            } else {
                ++it;
            }
        }
    }
    if (auto *ctx = tjetty->GetContext()) {
        ctx->UnregisterTjetty(tjetty->GetId());
    }
}

// ----- Seg import (cross-process / UDS) -----

MockSeg *MockUrmaBackend::ImportSeg(MockContext *ctx, uint64_t size, const std::string &token, const std::string &key,
                                    uint64_t remoteVa, urma_target_seg_t **rawOut)
{
    if (ctx == nullptr || size == 0) {
        return nullptr;
    }
    SegmentImportRequest request{ size, token, remoteVa };
    auto importResult = SegmentImporter::Instance().Resolve(request);
    if (importResult.GetKind() == SegmentImportKind::PLACEHOLDER) {
        return CreateRemotePlaceholder(*this, ctx, size, key, remoteVa, importResult.GetEndpoint(), rawOut);
    }
    if (importResult.GetKind() != SegmentImportKind::BACKING) {
        return nullptr;
    }
    const ImportEndpoint *endpoint = importResult.HasEndpoint() ? &importResult.GetEndpoint() : nullptr;
    return RegisterImportedSeg(*this, ctx, key, importResult.GetBacking(), endpoint, rawOut);
}

void MockUrmaBackend::UnimportSeg(MockSeg *seg)
{
    UnregisterSeg(seg);
}

urma_status_t MockUrmaBackend::PostSendWr(MockJetty *jetty, const urma_jfs_wr_t *wr, urma_jfs_wr_t **badWr)
{
    return MockPostSendWr(jetty, wr, badWr);
}

// ----- JFC completion forwarders (thin) -----

int MockUrmaBackend::PollJfc(MockJfc *jfc, int maxCr, urma_cr_t *outRecords)
{
    if (jfc == nullptr) {
        return 0;
    }
    return jfc->Poll(maxCr, outRecords);
}

void MockUrmaBackend::AckJfc(MockJfc *jfc, uint32_t ackCnt)
{
    if (jfc != nullptr) {
        jfc->Ack(ackCnt);
    }
}

void MockUrmaBackend::RearmJfc(MockJfc *jfc, bool enableEvents)
{
    if (jfc != nullptr) {
        jfc->Rearm(enableEvents);
    }
}

// ----- Async / Perf no-op forwarders -----

urma_status_t MockUrmaBackend::GetAsyncEvent(urma_async_event_t *event)
{
    auto eventType = GetMockInjectEventType();
#ifdef WITH_TESTS
    auto handle = inject::Execute(__LINE__, __FILE__, "UrmaManager.InjectAsyncEvent",
                                  [](int64_t injectedEventType) { return injectedEventType; });
    if (handle.NeedReturn()) {
        eventType = static_cast<int>(handle.Get());
    }
#endif
    if (eventType != URMA_EVENT_NONE && event != nullptr) {
        auto delayMs = GetMockInjectEventDelayMs();
        if (delayMs != 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
        }
        event->event_type = eventType;
        event->element_type = 0;
        if (eventType == URMA_EVENT_JETTY_ERR) {
            event->element.jetty = GetMockInjectEventJetty();
        } else {
            event->element.raw = nullptr;
        }
        event->data = 0;
    }
    return URMA_SUCCESS;
}

}  // namespace urma_mock
}  // namespace datasystem

#endif  // USE_URMA_MOCK
