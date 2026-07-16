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

#include <gtest/gtest.h>
#include <sys/mman.h>
#include <unistd.h>

#include <atomic>
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "datasystem/common/urma_mock/abi/mock_abi.h"
#include "datasystem/common/urma_mock/abi/urma_abi_compat.h"
#include "datasystem/common/urma_mock/mock_registry.h"
#include "datasystem/common/urma_mock/objects/mock_context.h"
#include "datasystem/common/urma_mock/objects/mock_device.h"
#include "datasystem/common/urma_mock/objects/mock_jfc.h"
#include "datasystem/common/urma_mock/urma_mock_backend.h"

namespace datasystem {
namespace common {
namespace rdma {
namespace {

#ifdef USE_URMA_MOCK

/*
 * These tests exercise the real shim struct allocation, side-table lookup, and memcpy + CR queue path that the
 * UrmaManager code drives in mock mode. They are not stub-layer "expect null" checks; they verify that a Set/Get-style
 * scenario transfers bytes and reports completion on the recv jfc.
 */

urma_jfc_t *GetTjettyRecvJfc(urma_target_jetty_t *tjetty)
{
    auto &tables = datasystem::urma_mock::Tables();
    std::lock_guard<std::mutex> lock(tables.mu);
    auto it = tables.tjetty.find(tjetty);
    if (it == tables.tjetty.end() || it->second == nullptr) {
        return nullptr;
    }
    auto *recvJfc = it->second->GetRemoteRecvJfc();
    if (recvJfc == nullptr) {
        return nullptr;
    }
    for (auto &[raw, mockJfc] : tables.jfc) {
        if (mockJfc.get() == recvJfc) {
            return raw;
        }
    }
    return nullptr;
}

class UrmaMockBackendTest : public testing::Test {
protected:
    void SetUp() override
    {
        // Reset singleton state across tests.
        datasystem::urma_mock::MockUrmaBackend::Instance().Cleanup();
        ASSERT_TRUE(datasystem::urma_mock::MockUrmaBackend::Instance().Init());
    }
    void TearDown() override
    {
        datasystem::urma_mock::MockUrmaBackend::Instance().Cleanup();
    }
};

TEST_F(UrmaMockBackendTest, InitAndDeviceList)
{
    auto devs = datasystem::urma_mock::MockUrmaBackend::Instance().GetDevices();
    ASSERT_EQ(devs.size(), 1u);
    EXPECT_EQ(devs[0]->GetName(), "bonding_mock0");
}

TEST_F(UrmaMockBackendTest, QueryDeviceFillsReasonableCaps)
{
    auto *dev = datasystem::urma_mock::MockUrmaBackend::Instance().GetDevices()[0];
    auto *rawDev = dev->GetPrivRawDev();
    ASSERT_NE(rawDev, nullptr);
    urma_device_attr_t attr{};
    EXPECT_EQ(ds_urma_mock_query_device(rawDev, &attr), URMA_SUCCESS);
    EXPECT_GT(attr.max_jetty, 0);
    EXPECT_GT(attr.max_jfc, 0);
    EXPECT_GT(attr.dev_cap.max_jfc_depth, 0);
    EXPECT_GT(attr.max_send_wr, 0u);
    EXPECT_GT(attr.max_recv_wr, 0u);
    EXPECT_GT(attr.dev_cap.max_write_size, 0u);
    EXPECT_GT(attr.dev_cap.max_read_size, 0u);
}

TEST_F(UrmaMockBackendTest, GetEidListReturnsOneEid)
{
    auto *rawDev = datasystem::urma_mock::MockUrmaBackend::Instance().GetDevices()[0]->GetPrivRawDev();
    uint32_t count = 0;
    auto *list = ds_urma_mock_get_eid_list(rawDev, &count);
    ASSERT_NE(list, nullptr);
    EXPECT_EQ(count, 1u);
    EXPECT_EQ(sizeof(list->eid.raw), 16u);
    ds_urma_mock_free_eid_list(list);
}

TEST_F(UrmaMockBackendTest, ContextsUseStableDeviceEidAndUniqueUasid)
{
    auto *rawDev = datasystem::urma_mock::MockUrmaBackend::Instance().GetDevices()[0]->GetPrivRawDev();
    uint32_t count = 0;
    auto *list = ds_urma_mock_get_eid_list(rawDev, &count);
    ASSERT_NE(list, nullptr);
    ASSERT_EQ(count, 1u);

    urma_context_t *first = ds_urma_mock_create_context(rawDev, 0);
    ASSERT_NE(first, nullptr);
    urma_context_t *second = ds_urma_mock_create_context(rawDev, 0);
    ASSERT_NE(second, nullptr);

    EXPECT_EQ(std::memcmp(first->eid.raw, list->eid.raw, sizeof(first->eid.raw)), 0);
    EXPECT_EQ(std::memcmp(second->eid.raw, list->eid.raw, sizeof(second->eid.raw)), 0);
    EXPECT_NE(first->uasid, second->uasid);

    EXPECT_EQ(ds_urma_mock_delete_context(second), URMA_SUCCESS);
    EXPECT_EQ(ds_urma_mock_delete_context(first), URMA_SUCCESS);
    ds_urma_mock_free_eid_list(list);
}

TEST_F(UrmaMockBackendTest, CreateContextJfrJettyRoundTrip)
{
    auto *rawDev = datasystem::urma_mock::MockUrmaBackend::Instance().GetDevices()[0]->GetPrivRawDev();
    urma_context_t *ctx = ds_urma_mock_create_context(rawDev, 0);
    ASSERT_NE(ctx, nullptr);
    EXPECT_EQ(ctx->dev_type, 1);

    urma_jfr_cfg_t jfrCfg{};
    auto *jfr = ds_urma_mock_create_jfr(ctx, &jfrCfg);
    ASSERT_NE(jfr, nullptr);
    EXPECT_GT(jfr->jfr_id.id, 0u);
    EXPECT_EQ(ds_urma_mock_delete_jfr(jfr), URMA_SUCCESS);

    urma_jetty_cfg_t jettyCfg{};
    urma_jfc_t *jfc = ds_urma_mock_create_jfc(ctx, nullptr);
    ASSERT_NE(jfc, nullptr);
    jettyCfg.jfs_cfg.jfc = jfc;
    auto *jetty = ds_urma_mock_create_jetty(ctx, &jettyCfg);
    ASSERT_NE(jetty, nullptr);
    EXPECT_GT(jetty->jetty_id.id, 0u);
    EXPECT_EQ(ds_urma_mock_delete_jetty(jetty), URMA_SUCCESS);

    EXPECT_EQ(ds_urma_mock_delete_jfc(jfc), URMA_SUCCESS);
    EXPECT_EQ(ds_urma_mock_delete_context(ctx), URMA_SUCCESS);
}

TEST_F(UrmaMockBackendTest, ConcurrentNextIdAllocationsAreUnique)
{
    constexpr size_t kThreadCount = 32;
    constexpr size_t kIdsPerThread = 4096;
    std::vector<std::thread> threads;
    std::vector<std::vector<uint64_t>> ids(kThreadCount);
    std::atomic<bool> start{ false };

    for (size_t i = 0; i < kThreadCount; ++i) {
        threads.emplace_back([i, &ids, &start]() {
            while (!start.load(std::memory_order_acquire)) {
            }
            ids[i].reserve(kIdsPerThread);
            auto &backend = datasystem::urma_mock::MockUrmaBackend::Instance();
            for (size_t j = 0; j < kIdsPerThread; ++j) {
                ids[i].push_back(backend.NextId());
            }
        });
    }
    start.store(true, std::memory_order_release);
    for (auto &thread : threads) {
        thread.join();
    }

    std::vector<uint64_t> allIds;
    allIds.reserve(kThreadCount * kIdsPerThread);
    for (auto &threadIds : ids) {
        allIds.insert(allIds.end(), threadIds.begin(), threadIds.end());
    }
    std::sort(allIds.begin(), allIds.end());
    auto uniqueEnd = std::unique(allIds.begin(), allIds.end());
    EXPECT_EQ(static_cast<size_t>(uniqueEnd - allIds.begin()), kThreadCount * kIdsPerThread);
}

TEST_F(UrmaMockBackendTest, MockJfcContextReferenceExpiresWithContext)
{
    auto ctx = std::make_shared<datasystem::urma_mock::MockContext>(1, nullptr);
    datasystem::urma_mock::MockJfc jfc(2, ctx);
    ASSERT_NE(jfc.LockContext(), nullptr);

    ctx.reset();
    EXPECT_EQ(jfc.LockContext(), nullptr);
}

TEST_F(UrmaMockBackendTest, RegisterSegAllocatesRealShm)
{
    auto *rawDev = datasystem::urma_mock::MockUrmaBackend::Instance().GetDevices()[0]->GetPrivRawDev();
    urma_context_t *ctx = ds_urma_mock_create_context(rawDev, 0);
    ASSERT_NE(ctx, nullptr);

    constexpr size_t kSegSize = 4096;
    urma_seg_cfg_t segCfg{};
    segCfg.len = kSegSize;
    segCfg.va = 0x10000;
    auto *tseg = ds_urma_mock_register_seg(ctx, &segCfg);
    ASSERT_NE(tseg, nullptr);
    EXPECT_EQ(tseg->seg.len, kSegSize);
    EXPECT_NE(tseg->seg.priv, nullptr);

    // Write/read through priv ptr.
    auto *ptr = static_cast<char *>(tseg->seg.priv);
    std::strcpy(ptr, "hello mock urma");
    EXPECT_STREQ(ptr, "hello mock urma");

    EXPECT_EQ(ds_urma_mock_unregister_seg(tseg), URMA_SUCCESS);
    EXPECT_EQ(ds_urma_mock_delete_context(ctx), URMA_SUCCESS);
}

TEST_F(UrmaMockBackendTest, UnregisterBorrowedBusinessMemfdKeepsMappingAlive)
{
    auto *rawDev = datasystem::urma_mock::MockUrmaBackend::Instance().GetDevices()[0]->GetPrivRawDev();
    urma_context_t *ctx = ds_urma_mock_create_context(rawDev, 0);
    ASSERT_NE(ctx, nullptr);

    constexpr size_t kSegSize = 4096;
    constexpr uint64_t kBusinessVa = 0x7f0033000000ULL;
    int businessFd = ::memfd_create("datasystem", 0);
    ASSERT_GE(businessFd, 0);
    ASSERT_EQ(::ftruncate(businessFd, static_cast<off_t>(kSegSize)), 0);
    void *businessPtr = ::mmap(reinterpret_cast<void *>(kBusinessVa), kSegSize, PROT_READ | PROT_WRITE,
                               MAP_SHARED | MAP_FIXED, businessFd, 0);
    ASSERT_NE(businessPtr, MAP_FAILED);

    auto *businessBytes = static_cast<char *>(businessPtr);
    std::strcpy(businessBytes, "before unregister");

    urma_seg_cfg_t segCfg{};
    segCfg.len = kSegSize;
    segCfg.va = kBusinessVa;
    auto *tseg = ds_urma_mock_register_seg(ctx, &segCfg);
    ASSERT_NE(tseg, nullptr);
    auto *seg = reinterpret_cast<datasystem::urma_mock::MockSeg *>(tseg->priv);
    ASSERT_NE(seg, nullptr);
    EXPECT_FALSE(seg->OwnsMapping());

    EXPECT_EQ(ds_urma_mock_unregister_seg(tseg), URMA_SUCCESS);
    std::strcpy(businessBytes, "after unregister");
    EXPECT_STREQ(businessBytes, "after unregister");

    EXPECT_EQ(ds_urma_mock_delete_context(ctx), URMA_SUCCESS);
    EXPECT_EQ(::munmap(businessPtr, kSegSize), 0);
    EXPECT_EQ(::close(businessFd), 0);
}

TEST_F(UrmaMockBackendTest, PostSendTransfersBytesAndCompletes)
{
    // Use one ctx with separate src and dst segs.
    auto *rawDev = datasystem::urma_mock::MockUrmaBackend::Instance().GetDevices()[0]->GetPrivRawDev();
    urma_context_t *ctx = ds_urma_mock_create_context(rawDev, 0);
    ASSERT_NE(ctx, nullptr);

    // src jetty
    urma_jfc_t *jfcSrc = ds_urma_mock_create_jfc(ctx, nullptr);
    urma_jetty_cfg_t jCfg{};
    jCfg.jfs_cfg.jfc = jfcSrc;
    urma_jetty_t *jetty = ds_urma_mock_create_jetty(ctx, &jCfg);
    ASSERT_NE(jetty, nullptr);
    // src local seg with payload
    constexpr uint64_t kPayloadSize = 1024;
    urma_seg_cfg_t sCfg{};
    sCfg.len = kPayloadSize;
    sCfg.va = 0x100000;
    sCfg.token_value.value = 0x100000;
    auto *tsegSrc = ds_urma_mock_register_seg(ctx, &sCfg);
    ASSERT_NE(tsegSrc, nullptr);
    char *srcPtr = static_cast<char *>(tsegSrc->seg.priv);
    std::strcpy(srcPtr, "PAYLOAD-ABC-123");

    // dst remote seg (where src writes)
    urma_seg_cfg_t dCfg{};
    dCfg.len = kPayloadSize;
    dCfg.va = 0x200000;
    dCfg.token_value.value = 0x200000;
    auto *tsegDst = ds_urma_mock_register_seg(ctx, &dCfg);
    ASSERT_NE(tsegDst, nullptr);
    // Pre-fill dst with 0xFF so the write is detectable.
    std::memset(tsegDst->seg.priv, 0xFF, kPayloadSize);

    urma_token_t tok{};
    tok.value = 0x200000;  // matches dCfg.token_value
    urma_rjetty_t rjetty{};
    urma_target_jetty_t *tjetty = ds_urma_mock_import_jetty(ctx, &rjetty, &tok);
    ASSERT_NE(tjetty, nullptr);

    // dst recv jfc: import_jetty creates its own internal jfc on the ctx.
    // We look it up via the public helper to poll the actual jfc that
    // PostSend writes CRs to.
    urma_jfc_t *jfcDstRecv = GetTjettyRecvJfc(tjetty);
    ASSERT_NE(jfcDstRecv, nullptr);

    // Build a WR that points to srcPtr, writes to dst base
    char srcSgeBuf[64] = "PAYLOAD-ABC-123";
    urma_sge_t sge{};
    sge.addr = reinterpret_cast<uintptr_t>(srcSgeBuf);
    sge.len = sizeof(srcSgeBuf);
    urma_sge_t dstSge{};
    dstSge.addr = reinterpret_cast<uintptr_t>(tsegDst->seg.priv);
    dstSge.len = sizeof(srcSgeBuf);
    urma_jfs_wr_t wr{};
    wr.opcode = URMA_OPC_WRITE;
    wr.flag.bs.complete_enable = 1;
    wr.tjetty = tjetty;
    wr.rw.src.sge = &sge;
    wr.rw.src.num_sge = 1;
    wr.rw.dst.sge = &dstSge;
    wr.rw.dst.num_sge = 1;
    urma_jfs_wr_t *bad = nullptr;
    EXPECT_EQ(ds_urma_mock_post_jetty_send_wr(jetty, &wr, &bad), URMA_SUCCESS);
    EXPECT_EQ(bad, nullptr);

    // PostSendWr is async, so poll until the worker pushes the CR.
    urma_cr_t crs[4]{};
    int n = 0;
    for (int i = 0; i < 5000 && n == 0; ++i) {
        n = ds_urma_mock_poll_jfc(jfcSrc, 4, crs);
        if (n == 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }
    EXPECT_GE(n, 1);
    EXPECT_EQ(crs[0].status, URMA_SUCCESS);
    EXPECT_EQ(crs[0].byte_cnt, sizeof(srcSgeBuf));
    EXPECT_EQ(crs[0].opcode, URMA_OPC_WRITE);
    EXPECT_EQ(crs[0].local_id, jetty->jetty_id.id);

    // dst base should now contain the payload
    EXPECT_EQ(std::memcmp(tsegDst->seg.priv, srcSgeBuf, sizeof(srcSgeBuf)), 0);

    // Cleanup
    EXPECT_EQ(ds_urma_mock_unimport_jetty(tjetty), URMA_SUCCESS);
    EXPECT_EQ(ds_urma_mock_unregister_seg(tsegDst), URMA_SUCCESS);
    EXPECT_EQ(ds_urma_mock_unregister_seg(tsegSrc), URMA_SUCCESS);
    EXPECT_EQ(ds_urma_mock_delete_jetty(jetty), URMA_SUCCESS);
    EXPECT_EQ(ds_urma_mock_delete_jfc(jfcSrc), URMA_SUCCESS);
    EXPECT_EQ(ds_urma_mock_delete_jfc(jfcDstRecv), URMA_SUCCESS);
    EXPECT_EQ(ds_urma_mock_delete_context(ctx), URMA_SUCCESS);
}

TEST_F(UrmaMockBackendTest, PostSendOnUnboundJettyIsNoopSuccess)
{
    auto *rawDev = datasystem::urma_mock::MockUrmaBackend::Instance().GetDevices()[0]->GetPrivRawDev();
    urma_context_t *ctx = ds_urma_mock_create_context(rawDev, 0);
    urma_jfc_t *jfc = ds_urma_mock_create_jfc(ctx, nullptr);
    urma_jetty_cfg_t jCfg{};
    jCfg.jfs_cfg.jfc = jfc;
    urma_jetty_t *jetty = ds_urma_mock_create_jetty(ctx, &jCfg);
    ASSERT_NE(jetty, nullptr);
    urma_jfs_wr_t wr{};
    urma_jfs_wr_t *bad = nullptr;
    EXPECT_EQ(ds_urma_mock_post_jetty_send_wr(jetty, &wr, &bad), URMA_SUCCESS);
    EXPECT_EQ(bad, nullptr);
    EXPECT_EQ(ds_urma_mock_delete_jetty(jetty), URMA_SUCCESS);
    EXPECT_EQ(ds_urma_mock_delete_jfc(jfc), URMA_SUCCESS);
    EXPECT_EQ(ds_urma_mock_delete_context(ctx), URMA_SUCCESS);
}

TEST_F(UrmaMockBackendTest, PollAndAckJfcRoundTrip)
{
    auto *rawDev = datasystem::urma_mock::MockUrmaBackend::Instance().GetDevices()[0]->GetPrivRawDev();
    urma_context_t *ctx = ds_urma_mock_create_context(rawDev, 0);
    urma_jfc_t *jfc = ds_urma_mock_create_jfc(ctx, nullptr);
    // Empty poll returns 0.
    urma_cr_t out[1]{};
    EXPECT_EQ(ds_urma_mock_poll_jfc(jfc, 1, out), 0);
    // wait_jfc with no events and short timeout.
    urma_jfc_t *ev = nullptr;
    int n = ds_urma_mock_wait_jfc(nullptr, 1, 1, &ev);
    EXPECT_EQ(n, 0);
    EXPECT_EQ(ds_urma_mock_rearm_jfc(jfc, true), URMA_SUCCESS);
    EXPECT_EQ(ds_urma_mock_delete_jfc(jfc), URMA_SUCCESS);
    EXPECT_EQ(ds_urma_mock_delete_context(ctx), URMA_SUCCESS);
}

TEST_F(UrmaMockBackendTest, ImportSegSharesUnderlyingShm)
{
    // Cross-process mock transport design: register_seg allocates a real
    // POSIX shm under a stable name derived from (key, size). import_seg
    // re-derives the same name and attaches to the same kernel pages. So
    // after register + import, a write via the original ptr is visible
    // via the imported ptr — that's the URMA data-plane contract.
    auto *rawDev = datasystem::urma_mock::MockUrmaBackend::Instance().GetDevices()[0]->GetPrivRawDev();
    urma_context_t *ctx = ds_urma_mock_create_context(rawDev, 0);
    ASSERT_NE(ctx, nullptr);

    constexpr uint64_t kSegSize = 8192;
    // Use token_value 0xCAFE so register_seg and import_seg compute the
    // same shm name. (register_seg's key derivation falls back to va:len
    // when token is 0, so we set both token and a matching va.)
    urma_seg_cfg_t segCfg{};
    segCfg.len = kSegSize;
    segCfg.token_value.value = 0xCAFE;
    auto *tsegOrig = ds_urma_mock_register_seg(ctx, &segCfg);
    ASSERT_NE(tsegOrig, nullptr);
    auto *origPtr = reinterpret_cast<uint8_t *>(tsegOrig->seg.priv);
    ASSERT_NE(origPtr, nullptr);
    origPtr[0] = 0xAB;
    origPtr[kSegSize - 1] = 0xCD;

    urma_seg_t remoteSeg{};
    remoteSeg.len = kSegSize;
    urma_token_t tok{};
    tok.value = 0xCAFE;
    urma_import_seg_flag_t flag{};
    flag.value = URMA_IMPORT_SEG_FLAG_REMOTE_WRITE;
    auto *tseg = ds_urma_mock_import_seg(ctx, &remoteSeg, &tok, 0, flag);
    ASSERT_NE(tseg, nullptr);
    EXPECT_EQ(tseg->seg.len, kSegSize);
    auto *importPtr = reinterpret_cast<uint8_t *>(tseg->seg.priv);
    EXPECT_NE(importPtr, nullptr);
    // Imported ptr aliases the same kernel pages: reads see what orig wrote.
    EXPECT_EQ(importPtr[0], 0xAB);
    EXPECT_EQ(importPtr[kSegSize - 1], 0xCD);
    EXPECT_EQ(ds_urma_mock_unimport_seg(tseg), URMA_SUCCESS);
    EXPECT_EQ(ds_urma_mock_unregister_seg(tsegOrig), URMA_SUCCESS);
    EXPECT_EQ(ds_urma_mock_delete_context(ctx), URMA_SUCCESS);
}

TEST_F(UrmaMockBackendTest, ImportSegWithRemoteVaCreatesPlaceholderWhenEndpointIsStale)
{
    auto *rawDev = datasystem::urma_mock::MockUrmaBackend::Instance().GetDevices()[0]->GetPrivRawDev();
    urma_context_t *ctx = ds_urma_mock_create_context(rawDev, 0);
    ASSERT_NE(ctx, nullptr);

    constexpr uint64_t kSegSize = 8192;
    constexpr uint64_t kRemoteVa = 0x7f0012345000ULL;
    urma_seg_t remoteSeg{};
    remoteSeg.len = kSegSize;
    remoteSeg.ubva.va = kRemoteVa;
    urma_token_t tok{};
    tok.value = 0xCAFE;
    urma_import_seg_flag_t flag{};
    flag.value = URMA_IMPORT_SEG_FLAG_REMOTE_WRITE;

    auto *tseg = ds_urma_mock_import_seg(ctx, &remoteSeg, &tok, 0, flag);
    ASSERT_NE(tseg, nullptr);
    EXPECT_EQ(tseg->seg.len, kSegSize);
    EXPECT_EQ(tseg->seg.ubva.va, kRemoteVa);
    EXPECT_EQ(tseg->seg.priv, nullptr);

    EXPECT_EQ(ds_urma_mock_unimport_seg(tseg), URMA_SUCCESS);
    EXPECT_EQ(ds_urma_mock_delete_context(ctx), URMA_SUCCESS);
}

TEST_F(UrmaMockBackendTest, PostSendWrReportsErrorForRemotePlaceholderSegment)
{
    auto *rawDev = datasystem::urma_mock::MockUrmaBackend::Instance().GetDevices()[0]->GetPrivRawDev();
    urma_context_t *ctx = ds_urma_mock_create_context(rawDev, 0);
    ASSERT_NE(ctx, nullptr);
    auto *jfc = ds_urma_mock_create_jfc(ctx, nullptr);
    ASSERT_NE(jfc, nullptr);

    constexpr uint64_t kSegSize = 8192;
    constexpr uint64_t kRemoteVa = 0x7f0012365000ULL;
    constexpr uint64_t kToken = 0xCAFF;
    urma_seg_t remoteSeg{};
    remoteSeg.len = kSegSize;
    remoteSeg.ubva.va = kRemoteVa;
    urma_token_t tok{};
    tok.value = kToken;
    urma_import_seg_flag_t flag{};
    flag.value = URMA_IMPORT_SEG_FLAG_REMOTE_WRITE;
    auto *tseg = ds_urma_mock_import_seg(ctx, &remoteSeg, &tok, 0, flag);
    ASSERT_NE(tseg, nullptr);
    ASSERT_EQ(tseg->seg.priv, nullptr);

    urma_jetty_cfg_t jettyCfg{};
    jettyCfg.jfs_cfg.jfc = jfc;
    auto *jetty = ds_urma_mock_create_jetty(ctx, &jettyCfg);
    ASSERT_NE(jetty, nullptr);
    urma_rjetty_t rjetty{};
    auto *tjetty = ds_urma_mock_import_jetty(ctx, &rjetty, &tok);
    ASSERT_NE(tjetty, nullptr);

    char payload[16] = "placeholder";
    urma_sge_t localSge{};
    localSge.addr = reinterpret_cast<uintptr_t>(payload);
    localSge.len = sizeof(payload);
    urma_sge_t remoteSge{};
    remoteSge.addr = kRemoteVa;
    remoteSge.len = sizeof(payload);
    remoteSge.tseg = tseg;
    urma_jfs_wr_t wr{};
    wr.opcode = URMA_OPC_WRITE;
    wr.tjetty = tjetty;
    wr.rw.src.sge = &localSge;
    wr.rw.src.num_sge = 1;
    wr.rw.dst.sge = &remoteSge;
    wr.rw.dst.num_sge = 1;
    urma_jfs_wr_t *badWr = nullptr;

    EXPECT_EQ(ds_urma_mock_post_jetty_send_wr(jetty, &wr, &badWr), URMA_SUCCESS);
    ASSERT_EQ(badWr, nullptr);
    urma_cr_t crs[4]{};
    int n = 0;
    for (int i = 0; i < 5000 && n == 0; ++i) {
        n = ds_urma_mock_poll_jfc(jfc, 4, crs);
        if (n == 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }
    ASSERT_GE(n, 1);
    EXPECT_EQ(crs[0].status, URMA_CR_REM_ACCESS_ABORT_ERR);
    EXPECT_EQ(crs[0].byte_cnt, 0u);

    EXPECT_EQ(ds_urma_mock_unimport_jetty(tjetty), URMA_SUCCESS);
    EXPECT_EQ(ds_urma_mock_unimport_seg(tseg), URMA_SUCCESS);
    EXPECT_EQ(ds_urma_mock_delete_jetty(jetty), URMA_SUCCESS);
    EXPECT_EQ(ds_urma_mock_delete_jfc(jfc), URMA_SUCCESS);
    EXPECT_EQ(ds_urma_mock_delete_context(ctx), URMA_SUCCESS);
}

TEST_F(UrmaMockBackendTest, AsyncEventAndPerfNoop)
{
    urma_async_event_t ev{};
    EXPECT_EQ(ds_urma_mock_get_async_event(nullptr, &ev), URMA_SUCCESS);
    ds_urma_mock_ack_async_event(&ev);
    EXPECT_EQ(ds_urma_mock_start_perf(), URMA_SUCCESS);
    EXPECT_EQ(ds_urma_mock_stop_perf(), URMA_SUCCESS);
    char buf[8]{};
    uint32_t len = 8;
    EXPECT_EQ(ds_urma_mock_get_perf_info(buf, &len), URMA_SUCCESS);
    EXPECT_EQ(len, 0u);
}

// Concurrent import_jetty + post_send must not tear and must not read a stale
// destination tjetty pointer.
TEST_F(UrmaMockBackendTest, DstTjettyConcurrentAssign)
{
    auto *rawDev = datasystem::urma_mock::MockUrmaBackend::Instance().GetDevices()[0]->GetPrivRawDev();
    urma_context_t *ctx = ds_urma_mock_create_context(rawDev, 0);
    ASSERT_NE(ctx, nullptr);

    urma_jfc_t *jfc = ds_urma_mock_create_jfc(ctx, nullptr);
    urma_seg_cfg_t sCfg{};
    sCfg.len = 4096;
    sCfg.token_value.value = 0x300000;
    auto *tseg = ds_urma_mock_register_seg(ctx, &sCfg);
    ASSERT_NE(tseg, nullptr);
    urma_jetty_cfg_t jCfg{};
    jCfg.jfs_cfg.jfc = jfc;
    urma_jetty_t *jetty = ds_urma_mock_create_jetty(ctx, &jCfg);
    ASSERT_NE(jetty, nullptr);

    // Many concurrent import_jetty + post_send cycles; with a raw pointer
    // this races the setter and produces a stale/null read inside PostSend.
    // With atomic + shared_ptr, every post_send must observe a valid
    // dstTjetty (or null after delete) without crashing.
    constexpr int kIters = 200;
    std::atomic<int> bad{ 0 };
    std::vector<std::thread> ts;
    for (int i = 0; i < 4; ++i) {
        ts.emplace_back([&]() {
            for (int j = 0; j < kIters; ++j) {
                urma_token_t tok{};
                tok.value = 0x300000;
                urma_rjetty_t rjetty{};
                auto *tjt = ds_urma_mock_import_jetty(ctx, &rjetty, &tok);
                if (tjt == nullptr) {
                    bad.fetch_add(1);
                    continue;
                }

                urma_sge_t sge{};
                char buf[16] = "race123";
                sge.addr = reinterpret_cast<uintptr_t>(buf);
                sge.len = sizeof(buf);
                urma_sge_t dstSge{};
                dstSge.addr = reinterpret_cast<uintptr_t>(tseg->seg.priv);
                dstSge.len = sizeof(buf);
                urma_jfs_wr_t wr{};
                wr.opcode = URMA_OPC_WRITE;
                wr.rw.src.sge = &sge;
                wr.rw.src.num_sge = 1;
                wr.rw.dst.sge = &dstSge;
                wr.rw.dst.num_sge = 1;
                urma_jfs_wr_t *badWr = nullptr;
                auto rc = ds_urma_mock_post_jetty_send_wr(jetty, &wr, &badWr);
                if (rc != URMA_SUCCESS || badWr != nullptr) {
                    bad.fetch_add(1);
                }
                ds_urma_mock_unimport_jetty(tjt);
            }
        });
    }
    for (auto &t : ts) {
        t.join();
    }
    EXPECT_EQ(bad.load(), 0);

    ds_urma_mock_delete_jetty(jetty);
    ds_urma_mock_unregister_seg(tseg);
    ds_urma_mock_delete_jfc(jfc);
    ds_urma_mock_delete_context(ctx);
}

// import_jetty must not bind a jetty that is already bound; otherwise writes
// can be routed to the wrong target or context.
TEST_F(UrmaMockBackendTest, ImportJettyOnlyBindsTargetCtx)
{
    auto *rawDev = datasystem::urma_mock::MockUrmaBackend::Instance().GetDevices()[0]->GetPrivRawDev();
    urma_context_t *ctx = ds_urma_mock_create_context(rawDev, 0);
    ASSERT_NE(ctx, nullptr);

    urma_jfc_t *jfc = ds_urma_mock_create_jfc(ctx, nullptr);

    urma_seg_cfg_t sCfg{};
    sCfg.len = 4096;
    sCfg.token_value.value = 0x400000;  // matches import token for tsegA
    auto *tsegA = ds_urma_mock_register_seg(ctx, &sCfg);
    ASSERT_NE(tsegA, nullptr);
    urma_seg_cfg_t sCfgB = sCfg;
    sCfgB.token_value.value = 0x400001;
    auto *tsegB = ds_urma_mock_register_seg(ctx, &sCfgB);
    ASSERT_NE(tsegB, nullptr);

    urma_jetty_cfg_t jCfgA{};
    jCfgA.jfs_cfg.jfc = jfc;
    auto *jettyA = ds_urma_mock_create_jetty(ctx, &jCfgA);
    urma_jetty_cfg_t jCfgB = jCfgA;
    auto *jettyB = ds_urma_mock_create_jetty(ctx, &jCfgB);
    ASSERT_NE(jettyA, nullptr);
    ASSERT_NE(jettyB, nullptr);

    // First import_jetty on token-A: should bind jettyA (which is earlier
    // in the iteration order). Then post on jettyA; jettyB remains unbound.
    urma_token_t tokA{};
    tokA.value = 0x400000;  // matches first registered seg's token (default key=va:len)
    urma_rjetty_t rjetty{};
    auto *tjtA = ds_urma_mock_import_jetty(ctx, &rjetty, &tokA);
    ASSERT_NE(tjtA, nullptr);

    // Now post on jettyA: should be a real write (success, dstTjetty set).
    urma_sge_t sgeA{};
    char bufA[8] = "bind-A";
    sgeA.addr = reinterpret_cast<uintptr_t>(bufA);
    sgeA.len = sizeof(bufA);
    urma_sge_t dstSgeA{};
    dstSgeA.addr = reinterpret_cast<uintptr_t>(tsegA->seg.priv);
    dstSgeA.len = sizeof(bufA);
    urma_jfs_wr_t wrA{};
    wrA.opcode = URMA_OPC_WRITE;
    wrA.rw.src.sge = &sgeA;
    wrA.rw.src.num_sge = 1;
    wrA.rw.dst.sge = &dstSgeA;
    wrA.rw.dst.num_sge = 1;
    urma_jfs_wr_t *badWrA = nullptr;
    EXPECT_EQ(ds_urma_mock_post_jetty_send_wr(jettyA, &wrA, &badWrA), URMA_SUCCESS);

    // Poll for async CR before checking dst bytes.
    urma_cr_t crsA[2]{};
    int nA = 0;
    for (int i = 0; i < 5000 && nA == 0; ++i) {
        nA = ds_urma_mock_poll_jfc(jfc, 2, crsA);
        if (nA == 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }
    EXPECT_GE(nA, 1);
    EXPECT_EQ(std::memcmp(tsegA->seg.priv, bufA, sizeof(bufA)), 0);

    // Cleanup
    ds_urma_mock_unimport_jetty(tjtA);
    ds_urma_mock_delete_jetty(jettyB);
    ds_urma_mock_delete_jetty(jettyA);
    ds_urma_mock_unregister_seg(tsegB);
    ds_urma_mock_unregister_seg(tsegA);
    ds_urma_mock_delete_jfc(jfc);
    ds_urma_mock_delete_context(ctx);
}

TEST_F(UrmaMockBackendTest, UnimportJettyClearsSourceJettyBinding)
{
    auto *rawDev = datasystem::urma_mock::MockUrmaBackend::Instance().GetDevices()[0]->GetPrivRawDev();
    urma_context_t *ctx = ds_urma_mock_create_context(rawDev, 0);
    ASSERT_NE(ctx, nullptr);
    urma_jfc_t *jfc = ds_urma_mock_create_jfc(ctx, nullptr);
    ASSERT_NE(jfc, nullptr);

    urma_seg_cfg_t segCfg{};
    segCfg.len = 4096;
    segCfg.token_value.value = 0x410000;
    auto *seg = ds_urma_mock_register_seg(ctx, &segCfg);
    ASSERT_NE(seg, nullptr);

    urma_jetty_cfg_t jettyCfg{};
    jettyCfg.jfs_cfg.jfc = jfc;
    auto *jetty = ds_urma_mock_create_jetty(ctx, &jettyCfg);
    ASSERT_NE(jetty, nullptr);
    urma_token_t token{};
    token.value = segCfg.token_value.value;
    urma_rjetty_t rjetty{};
    auto *tjetty = ds_urma_mock_import_jetty(ctx, &rjetty, &token);
    ASSERT_NE(tjetty, nullptr);
    {
        auto &tables = datasystem::urma_mock::Tables();
        std::lock_guard<std::mutex> lock(tables.mu);
        auto mockJetty = datasystem::urma_mock::FindMockObject(tables.jetty, jetty);
        ASSERT_NE(mockJetty, nullptr);
        EXPECT_NE(mockJetty->GetDstTjetty(), nullptr);
    }

    EXPECT_EQ(ds_urma_mock_unimport_jetty(tjetty), URMA_SUCCESS);
    {
        auto &tables = datasystem::urma_mock::Tables();
        std::lock_guard<std::mutex> lock(tables.mu);
        auto mockJetty = datasystem::urma_mock::FindMockObject(tables.jetty, jetty);
        ASSERT_NE(mockJetty, nullptr);
        EXPECT_EQ(mockJetty->GetDstTjetty(), nullptr);
    }

    ds_urma_mock_delete_jetty(jetty);
    ds_urma_mock_unregister_seg(seg);
    ds_urma_mock_delete_jfc(jfc);
    ds_urma_mock_delete_context(ctx);
}

// PostSendWr must not copy out of bounds when dst SGE addr points outside the
// destination segment.
TEST_F(UrmaMockBackendTest, PostSendWrOutOfBoundsOffset)
{
    auto *rawDev = datasystem::urma_mock::MockUrmaBackend::Instance().GetDevices()[0]->GetPrivRawDev();
    urma_context_t *ctx = ds_urma_mock_create_context(rawDev, 0);
    ASSERT_NE(ctx, nullptr);

    urma_jfc_t *jfc = ds_urma_mock_create_jfc(ctx, nullptr);
    urma_seg_cfg_t sCfg{};
    sCfg.len = 256;
    sCfg.token_value.value = 0x600000;  // src seg token
    auto *tsegSrc = ds_urma_mock_register_seg(ctx, &sCfg);
    urma_seg_cfg_t dCfg{};
    dCfg.len = 256;
    dCfg.token_value.value = 0x500000;  // dst seg token (matches import token)
    auto *tsegDst = ds_urma_mock_register_seg(ctx, &dCfg);
    ASSERT_NE(tsegSrc, nullptr);
    ASSERT_NE(tsegDst, nullptr);

    urma_jetty_cfg_t jCfg{};
    jCfg.jfs_cfg.jfc = jfc;
    auto *jetty = ds_urma_mock_create_jetty(ctx, &jCfg);
    urma_token_t tok{};
    tok.value = 0x500000;
    urma_rjetty_t rjetty{};
    auto *tjt = ds_urma_mock_import_jetty(ctx, &rjetty, &tok);
    ASSERT_NE(tjt, nullptr);

    urma_jfc_t *jfcDstRecv = GetTjettyRecvJfc(tjt);
    ASSERT_NE(jfcDstRecv, nullptr);

    // Drain anything queued from the bind step.
    urma_cr_t drain[8]{};
    ds_urma_mock_poll_jfc(jfc, 8, drain);

    // Bogus dst SGE: addr is 1 MiB before dstBase. This must be skipped instead
    // of underflowing into an oversized memcpy.
    char srcBuf[16] = "should-skip-OOB";
    urma_sge_t sge{};
    sge.addr = reinterpret_cast<uintptr_t>(srcBuf);
    sge.len = sizeof(srcBuf);
    urma_sge_t dstSge{};
    dstSge.addr = reinterpret_cast<uintptr_t>(tsegDst->seg.priv) - (1ULL << 20);
    dstSge.len = sizeof(srcBuf);
    dstSge.tseg = tsegDst;
    urma_jfs_wr_t wr{};
    wr.opcode = URMA_OPC_WRITE;
    wr.rw.src.sge = &sge;
    wr.rw.src.num_sge = 1;
    wr.rw.dst.sge = &dstSge;
    wr.rw.dst.num_sge = 1;
    urma_jfs_wr_t *badWr = nullptr;
    EXPECT_EQ(ds_urma_mock_post_jetty_send_wr(jetty, &wr, &badWr), URMA_SUCCESS);
    // The OOB SGE was skipped, so dst seg should still be all zero.
    char zero[16] = { 0 };
    EXPECT_EQ(std::memcmp(tsegDst->seg.priv, zero, sizeof(zero)), 0) << "OOB dst SGE leaked bytes into dst seg";

    // Cleanup
    ds_urma_mock_unimport_jetty(tjt);
    ds_urma_mock_delete_jetty(jetty);
    ds_urma_mock_unregister_seg(tsegDst);
    ds_urma_mock_unregister_seg(tsegSrc);
    ds_urma_mock_delete_jfc(jfcDstRecv);
    ds_urma_mock_delete_jfc(jfc);
    ds_urma_mock_delete_context(ctx);
}

TEST_F(UrmaMockBackendTest, PostSendWrUsesAddressMatchedSegmentWhenWrTsegIsStale)
{
    auto *rawDev = datasystem::urma_mock::MockUrmaBackend::Instance().GetDevices()[0]->GetPrivRawDev();
    urma_context_t *ctx = ds_urma_mock_create_context(rawDev, 0);
    ASSERT_NE(ctx, nullptr);

    urma_jfc_t *jfc = ds_urma_mock_create_jfc(ctx, nullptr);
    ASSERT_NE(jfc, nullptr);
    urma_seg_cfg_t staleCfg{};
    staleCfg.len = 256;
    staleCfg.token_value.value = 0x510000;
    auto *staleTseg = ds_urma_mock_register_seg(ctx, &staleCfg);
    ASSERT_NE(staleTseg, nullptr);
    urma_seg_cfg_t dstCfg{};
    dstCfg.len = 256;
    dstCfg.token_value.value = 0x520000;
    auto *dstTseg = ds_urma_mock_register_seg(ctx, &dstCfg);
    ASSERT_NE(dstTseg, nullptr);

    urma_jetty_cfg_t jettyCfg{};
    jettyCfg.jfs_cfg.jfc = jfc;
    auto *jetty = ds_urma_mock_create_jetty(ctx, &jettyCfg);
    ASSERT_NE(jetty, nullptr);
    urma_token_t token{};
    token.value = dstCfg.token_value.value;
    urma_rjetty_t rjetty{};
    auto *tjetty = ds_urma_mock_import_jetty(ctx, &rjetty, &token);
    ASSERT_NE(tjetty, nullptr);
    auto *recvJfc = GetTjettyRecvJfc(tjetty);
    ASSERT_NE(recvJfc, nullptr);

    char payload[16] = "addr-match";
    urma_sge_t localSge{};
    localSge.addr = reinterpret_cast<uintptr_t>(payload);
    localSge.len = sizeof(payload);
    urma_sge_t remoteSge{};
    remoteSge.addr = reinterpret_cast<uintptr_t>(dstTseg->seg.priv);
    remoteSge.len = sizeof(payload);
    remoteSge.tseg = staleTseg;
    urma_jfs_wr_t wr{};
    wr.opcode = URMA_OPC_WRITE;
    wr.rw.src.sge = &localSge;
    wr.rw.src.num_sge = 1;
    wr.rw.dst.sge = &remoteSge;
    wr.rw.dst.num_sge = 1;
    urma_jfs_wr_t *badWr = nullptr;

    EXPECT_EQ(ds_urma_mock_post_jetty_send_wr(jetty, &wr, &badWr), URMA_SUCCESS);
    EXPECT_EQ(std::memcmp(dstTseg->seg.priv, payload, sizeof(payload)), 0);
    char zero[16] = { 0 };
    EXPECT_EQ(std::memcmp(staleTseg->seg.priv, zero, sizeof(zero)), 0);

    ds_urma_mock_unimport_jetty(tjetty);
    ds_urma_mock_delete_jetty(jetty);
    ds_urma_mock_unregister_seg(dstTseg);
    ds_urma_mock_unregister_seg(staleTseg);
    ds_urma_mock_delete_jfc(recvJfc);
    ds_urma_mock_delete_jfc(jfc);
    ds_urma_mock_delete_context(ctx);
}

TEST_F(UrmaMockBackendTest, PostSendWrFallsBackToBoundTjettyWhenWrTjettyIsStale)
{
    auto *rawDev = datasystem::urma_mock::MockUrmaBackend::Instance().GetDevices()[0]->GetPrivRawDev();
    urma_context_t *ctx = ds_urma_mock_create_context(rawDev, 0);
    ASSERT_NE(ctx, nullptr);

    urma_jfc_t *jfc = ds_urma_mock_create_jfc(ctx, nullptr);
    ASSERT_NE(jfc, nullptr);
    urma_seg_cfg_t segCfg{};
    segCfg.len = 256;
    segCfg.token_value.value = 0x530000;
    auto *tseg = ds_urma_mock_register_seg(ctx, &segCfg);
    ASSERT_NE(tseg, nullptr);

    urma_jetty_cfg_t jettyCfg{};
    jettyCfg.jfs_cfg.jfc = jfc;
    auto *jetty = ds_urma_mock_create_jetty(ctx, &jettyCfg);
    ASSERT_NE(jetty, nullptr);
    urma_token_t token{};
    token.value = segCfg.token_value.value;
    urma_rjetty_t rjetty{};
    auto *tjetty = ds_urma_mock_import_jetty(ctx, &rjetty, &token);
    ASSERT_NE(tjetty, nullptr);
    auto *recvJfc = GetTjettyRecvJfc(tjetty);
    ASSERT_NE(recvJfc, nullptr);

    char payload[16] = "bound-tjetty";
    urma_sge_t localSge{};
    localSge.addr = reinterpret_cast<uintptr_t>(payload);
    localSge.len = sizeof(payload);
    urma_sge_t remoteSge{};
    remoteSge.addr = reinterpret_cast<uintptr_t>(tseg->seg.priv);
    remoteSge.len = sizeof(payload);
    remoteSge.tseg = tseg;
    urma_target_jetty_t staleRawTjetty{};
    urma_jfs_wr_t wr{};
    wr.opcode = URMA_OPC_WRITE;
    wr.tjetty = &staleRawTjetty;
    wr.rw.src.sge = &localSge;
    wr.rw.src.num_sge = 1;
    wr.rw.dst.sge = &remoteSge;
    wr.rw.dst.num_sge = 1;
    urma_jfs_wr_t *badWr = nullptr;

    EXPECT_EQ(ds_urma_mock_post_jetty_send_wr(jetty, &wr, &badWr), URMA_SUCCESS);
    EXPECT_EQ(std::memcmp(tseg->seg.priv, payload, sizeof(payload)), 0);

    ds_urma_mock_unimport_jetty(tjetty);
    ds_urma_mock_delete_jetty(jetty);
    ds_urma_mock_unregister_seg(tseg);
    ds_urma_mock_delete_jfc(recvJfc);
    ds_urma_mock_delete_jfc(jfc);
    ds_urma_mock_delete_context(ctx);
}

// Concurrent DeleteJetty + PostSendWr must not use a freed jetty. The registry
// entry can outlive the in-flight write.
TEST_F(UrmaMockBackendTest, DeleteJettyInflightPostSend)
{
    auto *rawDev = datasystem::urma_mock::MockUrmaBackend::Instance().GetDevices()[0]->GetPrivRawDev();
    urma_context_t *ctx = ds_urma_mock_create_context(rawDev, 0);
    ASSERT_NE(ctx, nullptr);
    urma_jfc_t *jfc = ds_urma_mock_create_jfc(ctx, nullptr);
    urma_seg_cfg_t sCfg{};
    sCfg.len = 1024;
    sCfg.token_value.value = 0x700000;
    auto *tseg = ds_urma_mock_register_seg(ctx, &sCfg);
    ASSERT_NE(tseg, nullptr);
    urma_jetty_cfg_t jCfg{};
    jCfg.jfs_cfg.jfc = jfc;
    auto *jetty = ds_urma_mock_create_jetty(ctx, &jCfg);
    ASSERT_NE(jetty, nullptr);
    urma_token_t tok{};
    tok.value = 0x700000;
    urma_rjetty_t rjetty{};
    auto *tjt = ds_urma_mock_import_jetty(ctx, &rjetty, &tok);
    ASSERT_NE(tjt, nullptr);

    constexpr int kIters = 100;
    std::atomic<int> errors{ 0 };
    std::vector<std::thread> ts;
    for (int t = 0; t < 4; ++t) {
        ts.emplace_back([&]() {
            for (int j = 0; j < kIters; ++j) {
                // Create + bind + post + delete in a tight loop. DeleteJetty
                // drops the registry entry, while any in-flight PostSendWr keeps
                // the object alive.
                urma_jfc_t *jfcL = ds_urma_mock_create_jfc(ctx, nullptr);
                urma_jetty_cfg_t jCfgL{};
                jCfgL.jfs_cfg.jfc = jfcL;
                auto *jt = ds_urma_mock_create_jetty(ctx, &jCfgL);
                if (jt == nullptr) {
                    errors.fetch_add(1);
                    continue;
                }
                char buf[8] = "race";
                urma_sge_t sge{};
                sge.addr = reinterpret_cast<uintptr_t>(buf);
                sge.len = sizeof(buf);
                urma_sge_t dstSge{};
                dstSge.addr = reinterpret_cast<uintptr_t>(tseg->seg.priv);
                dstSge.len = sizeof(buf);
                urma_jfs_wr_t wr{};
                wr.opcode = URMA_OPC_WRITE;
                wr.rw.src.sge = &sge;
                wr.rw.src.num_sge = 1;
                wr.rw.dst.sge = &dstSge;
                wr.rw.dst.num_sge = 1;
                urma_jfs_wr_t *badWr = nullptr;
                auto rc = ds_urma_mock_post_jetty_send_wr(jt, &wr, &badWr);
                if (rc != URMA_SUCCESS) {
                    errors.fetch_add(1);
                }
                ds_urma_mock_delete_jetty(jt);
                ds_urma_mock_delete_jfc(jfcL);
            }
        });
    }
    for (auto &t : ts) {
        t.join();
    }
    EXPECT_EQ(errors.load(), 0);

    ds_urma_mock_unimport_jetty(tjt);
    ds_urma_mock_delete_jetty(jetty);
    ds_urma_mock_unregister_seg(tseg);
    ds_urma_mock_delete_jfc(jfc);
    ds_urma_mock_delete_context(ctx);
}

// Cleanup must tolerate rapid calls and leave the singleton ready for a clean
// Init.
TEST_F(UrmaMockBackendTest, CleanupWaitsForInflight)
{
    auto &b = datasystem::urma_mock::MockUrmaBackend::Instance();
    ASSERT_TRUE(b.Init());
    auto *rawDev = b.GetDevices()[0]->GetPrivRawDev();
    urma_context_t *ctx = ds_urma_mock_create_context(rawDev, 0);
    ASSERT_NE(ctx, nullptr);
    urma_jfc_t *jfc = ds_urma_mock_create_jfc(ctx, nullptr);
    urma_seg_cfg_t sCfg{};
    sCfg.len = 1024;
    sCfg.token_value.value = 0x800000;  // matches worker's import token
    auto *tseg = ds_urma_mock_register_seg(ctx, &sCfg);
    ASSERT_NE(tseg, nullptr);
    urma_jetty_cfg_t jCfg{};
    jCfg.jfs_cfg.jfc = jfc;
    auto *jetty = ds_urma_mock_create_jetty(ctx, &jCfg);
    ASSERT_NE(jetty, nullptr);

    // Exercise PostSendWr cycles before cleanup so shared objects have to drain
    // safely.
    std::atomic<bool> stop{ false };
    std::atomic<int> errors{ 0 };
    std::thread worker([&]() {
        urma_token_t tok{};
        tok.value = 0x800000;
        urma_rjetty_t rjetty{};
        auto *tjt = ds_urma_mock_import_jetty(ctx, &rjetty, &tok);
        if (tjt == nullptr) {
            errors.fetch_add(1);
            return;
        }
        (void)tjt;
        while (!stop.load()) {
            char buf[8] = "cleanup";
            urma_sge_t sge{};
            sge.addr = reinterpret_cast<uintptr_t>(buf);
            sge.len = sizeof(buf);
            urma_sge_t dstSge{};
            dstSge.addr = reinterpret_cast<uintptr_t>(tseg->seg.priv);
            dstSge.len = sizeof(buf);
            urma_jfs_wr_t wr{};
            wr.opcode = URMA_OPC_WRITE;
            wr.rw.src.sge = &sge;
            wr.rw.src.num_sge = 1;
            wr.rw.dst.sge = &dstSge;
            wr.rw.dst.num_sge = 1;
            urma_jfs_wr_t *badWr = nullptr;
            if (ds_urma_mock_post_jetty_send_wr(jetty, &wr, &badWr) != URMA_SUCCESS) {
                errors.fetch_add(1);
                break;
            }
        }
    });
    // Let the worker run a few cycles.
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    stop.store(true);
    worker.join();
    EXPECT_EQ(errors.load(), 0);

    // Note: ds_urma_mock_* teardown is best-effort here — the worker
    // thread's tjetty was created without us holding the raw pointer.
    // b.Cleanup() forcibly walks all side tables and shared_ptrs, which
    // is what we're really testing.
    b.Cleanup();

    // Re-init: must work cleanly (Init uses compare_exchange_strong).
    ASSERT_TRUE(b.Init());
    b.Cleanup();
}

// Concurrent Register* on the same MockContext must not race.
TEST_F(UrmaMockBackendTest, MockContextMapConcurrentReg)
{
    auto *rawDev = datasystem::urma_mock::MockUrmaBackend::Instance().GetDevices()[0]->GetPrivRawDev();
    urma_context_t *ctx = ds_urma_mock_create_context(rawDev, 0);
    ASSERT_NE(ctx, nullptr);

    constexpr int kPerThread = 50;
    constexpr int kThreads = 4;
    std::atomic<int> errors{ 0 };
    std::vector<std::thread> ts;
    for (int t = 0; t < kThreads; ++t) {
        ts.emplace_back([&, t]() {
            for (int j = 0; j < kPerThread; ++j) {
                // Each thread creates + deletes a jfc and a jetty on the
                // Same ctx. The lock order must keep these maps race-free.
                urma_jfc_t *jfcL = ds_urma_mock_create_jfc(ctx, nullptr);
                if (jfcL == nullptr) {
                    errors.fetch_add(1);
                    continue;
                }
                urma_jetty_cfg_t jCfgL{};
                jCfgL.jfs_cfg.jfc = jfcL;
                auto *jt = ds_urma_mock_create_jetty(ctx, &jCfgL);
                if (jt == nullptr) {
                    errors.fetch_add(1);
                    ds_urma_mock_delete_jfc(jfcL);
                    continue;
                }
                ds_urma_mock_delete_jetty(jt);
                ds_urma_mock_delete_jfc(jfcL);
            }
        });
    }
    for (auto &t : ts) {
        t.join();
    }
    EXPECT_EQ(errors.load(), 0);

    ds_urma_mock_delete_context(ctx);
}

#else  // USE_URMA_MOCK

TEST(UrmaMockBackendTest, DisabledWhenUseUrmaMockOff)
{
    GTEST_SKIP() << "URMA mock backend not compiled in this build (USE_URMA_MOCK undefined)";
}

#endif  // USE_URMA_MOCK

/*
 * Async PostSendWr tests exercise the real ds_urma_mock_post_jetty_send_wr entry point and verify that submit returns
 * immediately, latency is completed by the worker, caller-side source buffers can be released after submit, and
 * concurrent submissions do not deadlock.
 */
#ifdef USE_URMA_MOCK

// Async PostSendWr UTs share the same fixture/setup pattern as the suite above.
TEST_F(UrmaMockBackendTest, PostSendWrAsyncReturnsImmediately)
{
    // Submit must return quickly even with latency injection — the worker
    // does the sleep + memcpy, not the Submit path.
    setenv("URMA_MOCK_LATENCY_US", "10000", 1);  // 10ms — definitely visible

    auto *rawDev = datasystem::urma_mock::MockUrmaBackend::Instance().GetDevices()[0]->GetPrivRawDev();
    urma_context_t *ctx = ds_urma_mock_create_context(rawDev, 0);
    ASSERT_NE(ctx, nullptr);
    urma_jfc_t *jfc = ds_urma_mock_create_jfc(ctx, nullptr);
    urma_jetty_cfg_t jCfg{};
    jCfg.jfs_cfg.jfc = jfc;
    urma_jetty_t *jetty = ds_urma_mock_create_jetty(ctx, &jCfg);
    urma_seg_cfg_t sCfg{};
    sCfg.len = 256;
    sCfg.token_value.value = 0x900000;
    auto *tseg = ds_urma_mock_register_seg(ctx, &sCfg);
    urma_token_t tok{};
    tok.value = 0x900000;
    urma_rjetty_t rjetty{};
    auto *tjt = ds_urma_mock_import_jetty(ctx, &rjetty, &tok);
    auto *jfcRecv = GetTjettyRecvJfc(tjt);

    char srcBuf[16] = "async-immediate";
    urma_sge_t sge{};
    sge.addr = reinterpret_cast<uintptr_t>(srcBuf);
    sge.len = sizeof(srcBuf);
    urma_sge_t dstSge{};
    dstSge.addr = reinterpret_cast<uintptr_t>(tseg->seg.priv);
    dstSge.len = sizeof(srcBuf);
    urma_jfs_wr_t wr{};
    wr.opcode = URMA_OPC_WRITE;
    wr.rw.src.sge = &sge;
    wr.rw.src.num_sge = 1;
    wr.rw.dst.sge = &dstSge;
    wr.rw.dst.num_sge = 1;
    urma_jfs_wr_t *bad = nullptr;

    auto t0 = std::chrono::steady_clock::now();
    EXPECT_EQ(ds_urma_mock_post_jetty_send_wr(jetty, &wr, &bad), URMA_SUCCESS);
    auto t1 = std::chrono::steady_clock::now();
    auto usSubmit = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
    EXPECT_LT(usSubmit, 10000) << "Submit took " << usSubmit << "us (should not wait for injected latency)";

    // Worker should NOT have run yet — latency is 10ms.
    urma_cr_t crs[2]{};
    int n = ds_urma_mock_poll_jfc(jfc, 2, crs);
    EXPECT_EQ(n, 0) << "Worker ran before latency expired";

    // Wait for completion (latency=10ms + slop).
    n = 0;
    for (int i = 0; i < 500 && n == 0; ++i) {
        n = ds_urma_mock_poll_jfc(jfc, 2, crs);
        if (n == 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    EXPECT_GE(n, 1);
    EXPECT_EQ(std::memcmp(tseg->seg.priv, srcBuf, sizeof(srcBuf)), 0);

    ds_urma_mock_unimport_jetty(tjt);
    ds_urma_mock_unregister_seg(tseg);
    ds_urma_mock_delete_jetty(jetty);
    ds_urma_mock_delete_jfc(jfc);
    ds_urma_mock_delete_jfc(jfcRecv);
    ds_urma_mock_delete_context(ctx);
    unsetenv("URMA_MOCK_LATENCY_US");
}

TEST_F(UrmaMockBackendTest, PostSendWrAsyncNonInlineSnapshotsSourceAtPost)
{
    setenv("URMA_MOCK_LATENCY_US", "10000", 1);

    auto *rawDev = datasystem::urma_mock::MockUrmaBackend::Instance().GetDevices()[0]->GetPrivRawDev();
    urma_context_t *ctx = ds_urma_mock_create_context(rawDev, 0);
    ASSERT_NE(ctx, nullptr);
    urma_jfc_t *jfc = ds_urma_mock_create_jfc(ctx, nullptr);
    urma_jetty_cfg_t jCfg{};
    jCfg.jfs_cfg.jfc = jfc;
    urma_jetty_t *jetty = ds_urma_mock_create_jetty(ctx, &jCfg);
    urma_seg_cfg_t sCfg{};
    sCfg.len = 256;
    sCfg.token_value.value = 0xa00000;
    auto *tseg = ds_urma_mock_register_seg(ctx, &sCfg);
    urma_token_t tok{};
    tok.value = 0xa00000;
    urma_rjetty_t rjetty{};
    auto *tjt = ds_urma_mock_import_jetty(ctx, &rjetty, &tok);
    auto *jfcRecv = GetTjettyRecvJfc(tjt);

    char srcBuf[32] = "post-time-data";
    urma_sge_t sge{};
    sge.addr = reinterpret_cast<uintptr_t>(srcBuf);
    sge.len = 32;
    urma_sge_t dstSge{};
    dstSge.addr = reinterpret_cast<uintptr_t>(tseg->seg.priv);
    dstSge.len = 32;
    urma_jfs_wr_t wr{};
    wr.opcode = URMA_OPC_WRITE;
    wr.rw.src.sge = &sge;
    wr.rw.src.num_sge = 1;
    wr.rw.dst.sge = &dstSge;
    wr.rw.dst.num_sge = 1;
    urma_jfs_wr_t *bad = nullptr;
    EXPECT_EQ(ds_urma_mock_post_jetty_send_wr(jetty, &wr, &bad), URMA_SUCCESS);
    std::memcpy(srcBuf, "completion-time-data", 21);

    urma_cr_t crs[2]{};
    int n = 0;
    for (int i = 0; i < 500 && n == 0; ++i) {
        n = ds_urma_mock_poll_jfc(jfc, 2, crs);
        if (n == 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    EXPECT_GE(n, 1);
    EXPECT_EQ(std::memcmp(tseg->seg.priv, "post-time-data", 14), 0);

    ds_urma_mock_unimport_jetty(tjt);
    ds_urma_mock_unregister_seg(tseg);
    ds_urma_mock_delete_jetty(jetty);
    ds_urma_mock_delete_jfc(jfc);
    ds_urma_mock_delete_jfc(jfcRecv);
    ds_urma_mock_delete_context(ctx);
    unsetenv("URMA_MOCK_LATENCY_US");
}

TEST_F(UrmaMockBackendTest, PostSendWrAsyncInlineSnapshotsSourceAtPost)
{
    setenv("URMA_MOCK_LATENCY_US", "10000", 1);

    auto *rawDev = datasystem::urma_mock::MockUrmaBackend::Instance().GetDevices()[0]->GetPrivRawDev();
    urma_context_t *ctx = ds_urma_mock_create_context(rawDev, 0);
    ASSERT_NE(ctx, nullptr);
    urma_jfc_t *jfc = ds_urma_mock_create_jfc(ctx, nullptr);
    urma_jetty_cfg_t jCfg{};
    jCfg.jfs_cfg.jfc = jfc;
    urma_jetty_t *jetty = ds_urma_mock_create_jetty(ctx, &jCfg);
    urma_seg_cfg_t sCfg{};
    sCfg.len = 256;
    sCfg.token_value.value = 0xa00000;
    auto *tseg = ds_urma_mock_register_seg(ctx, &sCfg);
    urma_token_t tok{};
    tok.value = 0xa00000;
    urma_rjetty_t rjetty{};
    auto *tjt = ds_urma_mock_import_jetty(ctx, &rjetty, &tok);
    auto *jfcRecv = GetTjettyRecvJfc(tjt);

    char srcBuf[32] = "inline-post-data";
    urma_sge_t sge{};
    sge.addr = reinterpret_cast<uintptr_t>(srcBuf);
    sge.len = 32;
    urma_sge_t dstSge{};
    dstSge.addr = reinterpret_cast<uintptr_t>(tseg->seg.priv);
    dstSge.len = 32;
    urma_jfs_wr_t wr{};
    wr.opcode = URMA_OPC_WRITE;
    wr.flag.bs.inline_flag = 1;
    wr.rw.src.sge = &sge;
    wr.rw.src.num_sge = 1;
    wr.rw.dst.sge = &dstSge;
    wr.rw.dst.num_sge = 1;
    urma_jfs_wr_t *bad = nullptr;
    EXPECT_EQ(ds_urma_mock_post_jetty_send_wr(jetty, &wr, &bad), URMA_SUCCESS);
    std::memcpy(srcBuf, "inline-mutated-data", 20);

    urma_cr_t crs[2]{};
    int n = 0;
    for (int i = 0; i < 500 && n == 0; ++i) {
        n = ds_urma_mock_poll_jfc(jfc, 2, crs);
        if (n == 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    EXPECT_GE(n, 1);
    EXPECT_EQ(std::memcmp(tseg->seg.priv, "inline-post-data", 17), 0);

    ds_urma_mock_unimport_jetty(tjt);
    ds_urma_mock_unregister_seg(tseg);
    ds_urma_mock_delete_jetty(jetty);
    ds_urma_mock_delete_jfc(jfc);
    ds_urma_mock_delete_jfc(jfcRecv);
    ds_urma_mock_delete_context(ctx);
    unsetenv("URMA_MOCK_LATENCY_US");
}

TEST_F(UrmaMockBackendTest, PostSendWrAsyncConcurrentSubmitsDrain)
{
    // 100 concurrent PostSendWr submissions should all complete without
    // deadlock or loss. Catches under-sized pool + missing concurrency
    // safety in the Submit path.
    setenv("URMA_MOCK_LATENCY_US", "100", 1);  // 100us per task

    auto *rawDev = datasystem::urma_mock::MockUrmaBackend::Instance().GetDevices()[0]->GetPrivRawDev();
    urma_context_t *ctx = ds_urma_mock_create_context(rawDev, 0);
    ASSERT_NE(ctx, nullptr);
    urma_jfc_t *jfc = ds_urma_mock_create_jfc(ctx, nullptr);
    urma_jetty_cfg_t jCfg{};
    jCfg.jfs_cfg.jfc = jfc;
    urma_jetty_t *jetty = ds_urma_mock_create_jetty(ctx, &jCfg);
    constexpr uint64_t kSegSize = 4096 * 100;
    urma_seg_cfg_t sCfg{};
    sCfg.len = kSegSize;
    sCfg.token_value.value = 0xb00000;
    auto *tseg = ds_urma_mock_register_seg(ctx, &sCfg);
    urma_token_t tok{};
    tok.value = 0xb00000;
    urma_rjetty_t rjetty{};
    auto *tjt = ds_urma_mock_import_jetty(ctx, &rjetty, &tok);
    auto *jfcRecv = GetTjettyRecvJfc(tjt);

    constexpr int kWrCount = 80;
    auto *srcBuf = new char[kWrCount * 8];
    for (int i = 0; i < kWrCount; ++i) {
        std::memcpy(srcBuf + i * 8, "AAAAAAAA", 8);
    }
    constexpr int kThreads = 8;
    std::vector<std::thread> ths;
    std::atomic<int> submitErrs{ 0 };
    for (int t = 0; t < kThreads; ++t) {
        ths.emplace_back([&, t]() {
            for (int i = 0; i < kWrCount / kThreads; ++i) {
                int idx = t * (kWrCount / kThreads) + i;
                urma_sge_t sge{};
                sge.addr = reinterpret_cast<uintptr_t>(srcBuf + idx * 8);
                sge.len = 8;
                urma_sge_t dstSge{};
                dstSge.addr = reinterpret_cast<uintptr_t>(static_cast<char *>(tseg->seg.priv) + idx * 8);
                dstSge.len = 8;
                urma_jfs_wr_t wr{};
                wr.opcode = URMA_OPC_WRITE;
                wr.rw.src.sge = &sge;
                wr.rw.src.num_sge = 1;
                wr.rw.dst.sge = &dstSge;
                wr.rw.dst.num_sge = 1;
                urma_jfs_wr_t *bad = nullptr;
                if (ds_urma_mock_post_jetty_send_wr(jetty, &wr, &bad) != URMA_SUCCESS) {
                    submitErrs.fetch_add(1);
                }
            }
        });
    }
    for (auto &th : ths) {
        th.join();
    }
    EXPECT_EQ(submitErrs.load(), 0);

    // Drain all CRs.
    int totalCr = 0;
    urma_cr_t crs[16]{};
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (totalCr < kWrCount && std::chrono::steady_clock::now() < deadline) {
        int n = ds_urma_mock_poll_jfc(jfc, 16, crs);
        totalCr += n;
        if (n == 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    EXPECT_EQ(totalCr, kWrCount);
    // Spot-check dst memory (not all bytes because order is FIFO/parallel
    // but each 8-byte slot should be one of the two values).
    EXPECT_EQ(std::memcmp(static_cast<char *>(tseg->seg.priv), srcBuf, 8), 0);

    delete[] srcBuf;
    ds_urma_mock_unimport_jetty(tjt);
    ds_urma_mock_unregister_seg(tseg);
    ds_urma_mock_delete_jetty(jetty);
    ds_urma_mock_delete_jfc(jfc);
    ds_urma_mock_delete_jfc(jfcRecv);
    ds_urma_mock_delete_context(ctx);
    unsetenv("URMA_MOCK_LATENCY_US");
}

#endif  // USE_URMA_MOCK

}  // namespace
}  // namespace rdma
}  // namespace common
}  // namespace datasystem
