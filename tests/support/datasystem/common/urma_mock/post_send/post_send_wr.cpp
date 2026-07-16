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

#include "datasystem/common/urma_mock/post_send/post_send_wr.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "datasystem/common/log/log.h"
#include "datasystem/common/urma_mock/inject/fault_inject.h"
#include "datasystem/common/urma_mock/mock_registry.h"
#include "datasystem/common/urma_mock/objects/mock_jfc.h"
#include "datasystem/common/urma_mock/objects/mock_jetty.h"
#include "datasystem/common/urma_mock/objects/mock_seg.h"
#include "datasystem/common/urma_mock/objects/mock_tjetty.h"
#include "datasystem/common/urma_mock/post_send/mock_thread_pool.h"
#include "datasystem/common/urma_mock/segment/segment_view.h"
#include "datasystem/common/urma_mock/urma_mock_backend.h"

namespace datasystem {
namespace urma_mock {

namespace {
class TransferSnapshot {
public:
    TransferSnapshot(bool isRead, bool snapshotWritePayload, size_t numSge)
        : isRead_(isRead), snapshotWritePayload_(snapshotWritePayload)
    {
        entries_.reserve(numSge);
    }
    ~TransferSnapshot() = default;

    void Append(const urma_sge_t &local, size_t remoteOffset, size_t copyLen)
    {
        Entry entry;
        entry.remoteOffset = remoteOffset;
        entry.copyLen = copyLen;
        entry.localAddr = static_cast<uintptr_t>(local.addr);
        if (snapshotWritePayload_ && entry.localAddr != 0 && copyLen > 0) {
            const auto *src = reinterpret_cast<const uint8_t *>(static_cast<uintptr_t>(local.addr));
            entry.srcData.assign(src, src + copyLen);
        }
        entries_.push_back(std::move(entry));
    }

    uint32_t Execute(uintptr_t remoteBase) const
    {
        if (remoteBase == 0) {
            return 0;
        }
        uint32_t totalBytes = 0;
        for (const auto &entry : entries_) {
            if (entry.copyLen == 0) {
                continue;
            }
            if (isRead_) {
                if (entry.localAddr != 0) {
                    std::memcpy(reinterpret_cast<uint8_t *>(entry.localAddr),
                                reinterpret_cast<const uint8_t *>(remoteBase) + entry.remoteOffset, entry.copyLen);
                }
            } else if (!entry.srcData.empty()) {
                std::memcpy(reinterpret_cast<uint8_t *>(remoteBase) + entry.remoteOffset, entry.srcData.data(),
                            entry.srcData.size());
            } else if (entry.localAddr != 0) {
                std::memcpy(reinterpret_cast<uint8_t *>(remoteBase) + entry.remoteOffset,
                            reinterpret_cast<const uint8_t *>(entry.localAddr), entry.copyLen);
            }
            totalBytes += static_cast<uint32_t>(entry.copyLen);
        }
        return totalBytes;
    }

    bool IsRead() const
    {
        return isRead_;
    }

private:
    struct Entry {
        std::vector<uint8_t> srcData;
        uintptr_t localAddr = 0;
        size_t remoteOffset = 0;
        size_t copyLen = 0;
    };

    std::vector<Entry> entries_;
    bool isRead_;
    bool snapshotWritePayload_;
};

struct PostSendSnapshot {
    TransferSnapshot transfer;
    uint64_t userCtx = 0;
    uint32_t immData = 0;
    uint32_t localId = 0;

    PostSendSnapshot(bool isRead, bool snapshotWritePayload, size_t numSge, uint64_t userCtx, uint32_t immData,
                     uint32_t localId)
        : transfer(isRead, snapshotWritePayload, numSge), userCtx(userCtx), immData(immData), localId(localId)
    {
    }
};

struct PostSendLookup {
    std::shared_ptr<MockSeg> seg;
    std::shared_ptr<MockJfc> sendJfc;
    std::shared_ptr<MockJfc> recvJfc;
    std::shared_ptr<PostSendSnapshot> snap;
    bool remoteMapped = false;
};

std::shared_ptr<MockTjetty> FindTargetJetty(SideTables &tables, urma_target_jetty_t *wrTjetty, MockTjetty *boundTjetty)
{
    for (auto &[raw, mockTjt] : tables.tjetty) {
        if ((wrTjetty != nullptr && raw == wrTjetty) || mockTjt.get() == boundTjetty) {
            return mockTjt;
        }
    }
    return nullptr;
}

std::shared_ptr<MockJfc> FindJfc(SideTables &tables, MockJfc *target)
{
    if (target == nullptr) {
        return nullptr;
    }
    for (auto &[raw, mockJfc] : tables.jfc) {
        (void)raw;
        if (mockJfc.get() == target) {
            return mockJfc;
        }
    }
    return nullptr;
}

std::shared_ptr<MockSeg> FindWrTargetSeg(SideTables &tables, const urma_sg_t &remoteSg, uintptr_t firstRemoteAddr,
                                         uintptr_t &fallbackBase, bool &containsRemoteAddress)
{
    containsRemoteAddress = false;
    auto *wrRemoteSeg = remoteSg.sge[0].tseg;
    if (wrRemoteSeg == nullptr) {
        return nullptr;
    }
    for (auto &[raw, mockSeg] : tables.tseg) {
        if (raw != wrRemoteSeg || mockSeg == nullptr || mockSeg->GetPtr() == nullptr) {
            continue;
        }
        fallbackBase = GetSegmentAddressBase(mockSeg);
        containsRemoteAddress = firstRemoteAddr == 0 || ContainsAddress(mockSeg, firstRemoteAddr);
        return mockSeg;
    }
    return nullptr;
}

std::shared_ptr<MockSeg> FindBoundRemoteSeg(SideTables &tables, MockSeg *remoteSeg, uintptr_t firstRemoteAddr,
                                            uintptr_t &bestBase)
{
    if (remoteSeg == nullptr) {
        return nullptr;
    }
    for (auto &[raw, mockSeg] : tables.tseg) {
        (void)raw;
        if (mockSeg.get() == remoteSeg && (firstRemoteAddr == 0 || ContainsAddress(mockSeg, firstRemoteAddr))) {
            bestBase = GetSegmentAddressBase(mockSeg);
            return mockSeg;
        }
    }
    if (firstRemoteAddr != 0 && !ContainsAddress(remoteSeg, firstRemoteAddr)) {
        return nullptr;
    }
    for (auto &[raw, mockSeg] : tables.tseg) {
        (void)raw;
        if (mockSeg.get() == remoteSeg) {
            bestBase = GetSegmentAddressBase(mockSeg);
            return mockSeg;
        }
    }
    return nullptr;
}

std::shared_ptr<MockSeg> FindTokenRemoteSeg(SideTables &tables, const std::string &token, uintptr_t firstRemoteAddr,
                                            uintptr_t &bestBase)
{
    std::shared_ptr<MockSeg> best;
    if (firstRemoteAddr == 0 || token.empty()) {
        return best;
    }
    for (auto &[raw, mockSeg] : tables.tseg) {
        (void)raw;
        if (mockSeg == nullptr || mockSeg->GetKey() != token || !ContainsAddress(mockSeg, firstRemoteAddr)) {
            continue;
        }
        auto base = GetSegmentAddressBase(mockSeg);
        if (best == nullptr || base >= bestBase) {
            best = mockSeg;
            bestBase = base;
        }
    }
    return best;
}

std::shared_ptr<MockSeg> FindPostSendSegment(SideTables &tables, const std::shared_ptr<MockTjetty> &tjtShptr,
                                             const urma_sg_t &remoteSg)
{
    uintptr_t bestBase = 0;
    const uintptr_t firstRemoteAddr = static_cast<uintptr_t>(remoteSg.sge[0].addr);
    bool wrTargetContainsRemoteAddress = false;
    auto fallback = FindWrTargetSeg(tables, remoteSg, firstRemoteAddr, bestBase, wrTargetContainsRemoteAddress);
    if (fallback != nullptr && wrTargetContainsRemoteAddress) {
        return fallback;
    }
    auto seg = FindBoundRemoteSeg(tables, tjtShptr->GetRemoteSeg(), firstRemoteAddr, bestBase);
    if (seg != nullptr) {
        return seg;
    }
    return FindTokenRemoteSeg(tables, tjtShptr->GetToken(), firstRemoteAddr, bestBase);
}

std::shared_ptr<PostSendSnapshot> BuildPostSendSnapshot(const urma_jfs_wr_t *wr,
                                                        const std::shared_ptr<MockSeg> &segShptr,
                                                        const urma_sg_t &remoteSg, const urma_sg_t &localSg,
                                                        bool isRead, uint32_t localId)
{
    const uint64_t segSize = segShptr->GetSize();
    const uintptr_t dstAddressBase = GetSegmentAddressBase(segShptr);
    const bool singleRemoteSge = remoteSg.num_sge == 1 && localSg.num_sge > 1;
    const uint32_t numSge = singleRemoteSge ? localSg.num_sge : std::min(remoteSg.num_sge, localSg.num_sge);
    const bool snapshotWritePayload = !isRead;
    auto snap = std::make_shared<PostSendSnapshot>(isRead, snapshotWritePayload, numSge, wr->user_ctx,
                                                   wr->rw.notify_data, localId);
    uint64_t remoteCursor = 0;
    for (uint32_t i = 0; i < numSge; ++i) {
        const auto &remote = singleRemoteSge ? remoteSg.sge[0] : remoteSg.sge[i];
        const auto &local = localSg.sge[i];
        if (remote.addr == 0 || local.addr == 0) {
            continue;
        }
        uintptr_t remoteAddr = static_cast<uintptr_t>(remote.addr);
        if (remoteAddr < dstAddressBase) {
            continue;
        }
        uint64_t off = static_cast<uint64_t>(remoteAddr - dstAddressBase);
        if (off >= segSize) {
            continue;
        }
        if (singleRemoteSge) {
            off += remoteCursor;
            if (off >= segSize || remoteCursor >= remote.len) {
                break;
            }
        }
        size_t dstOff = static_cast<size_t>(off);
        uint64_t remain = segSize - dstOff;
        uint64_t remoteRemain =
            singleRemoteSge ? static_cast<uint64_t>(remote.len) - remoteCursor : static_cast<uint64_t>(remote.len);
        size_t copyLen = std::min({ remoteRemain, static_cast<uint64_t>(local.len), remain });
        snap->transfer.Append(local, dstOff, copyLen);
        remoteCursor += copyLen;
    }
    return snap;
}

class PostSendExecutor {
public:
    PostSendExecutor(MockJetty &jetty, const urma_jfs_wr_t &wr, MockTjetty *boundTjetty, const urma_sg_t &remoteSg,
                     const urma_sg_t &localSg, bool isRead)
        : jetty_(jetty), wr_(wr), boundTjetty_(boundTjetty), remoteSg_(remoteSg), localSg_(localSg), isRead_(isRead)
    {
    }
    ~PostSendExecutor() = default;

    urma_status_t Run(urma_jfs_wr_t **badWr)
    {
        PostSendLookup lookup;
        auto prepareRc = Prepare(lookup);
        if (prepareRc != URMA_SUCCESS) {
            SetBadWr(badWr, nullptr);
            return prepareRc;
        }
        if (lookup.seg == nullptr) {
            SetBadWr(badWr, nullptr);
            return URMA_SUCCESS;
        }
        return Submit(lookup, badWr);
    }

private:
    urma_status_t Prepare(PostSendLookup &lookup)
    {
        MockTablesLock lock;
        auto &tables = lock.GetTables();
        auto tjtShptr = FindTargetJetty(tables, wr_.tjetty, boundTjetty_);
        if (tjtShptr == nullptr) {
            return URMA_SUCCESS;
        }
        lookup.sendJfc = FindJfc(tables, jetty_.GetSendJfc());
        lookup.seg = FindPostSendSegment(tables, tjtShptr, remoteSg_);
        if (lookup.seg == nullptr) {
            return URMA_SUCCESS;
        }
        lookup.recvJfc = FindJfc(tables, tjtShptr->GetRemoteRecvJfc());
        lookup.snap = BuildPostSendSnapshot(&wr_, lookup.seg, remoteSg_, localSg_, isRead_,
                                            static_cast<uint32_t>(jetty_.GetId()));
        lookup.remoteMapped = lookup.seg->GetPtr() != nullptr;
        return URMA_SUCCESS;
    }

    static void Execute(const PostSendLookup &lookup)
    {
        auto dstBase = reinterpret_cast<uintptr_t>(lookup.seg->GetPtr());
        uint32_t totalBytes = dstBase == 0 ? 0 : lookup.snap->transfer.Execute(dstBase);
        MockCr cr;
        cr.status = static_cast<urma_status_t>(lookup.remoteMapped ? URMA_CR_SUCCESS : URMA_CR_REM_ACCESS_ABORT_ERR);
        cr.userCtx = lookup.snap->userCtx;
        cr.byteCnt = totalBytes;
        cr.localId = lookup.snap->localId;
        cr.opcodeIsRead = lookup.snap->transfer.IsRead();
        cr.immData = lookup.snap->immData;
        if (lookup.sendJfc != nullptr) {
            lookup.sendJfc->PushCr(cr);
        }
    }

    urma_status_t Submit(PostSendLookup &lookup, urma_jfs_wr_t **badWr) const
    {
        const uint64_t latencyUs = MockThreadPool::ResolveLatencyFromEnv();
        if (latencyUs == 0) {
            Execute(lookup);
            SetBadWr(badWr, nullptr);
            return URMA_SUCCESS;
        }

        auto &pool = MockUrmaBackend::Pool();
        bool submitted = pool.Submit([lookup = std::move(lookup), latencyUs]() mutable {
            if (latencyUs > 0) {
                std::this_thread::sleep_for(std::chrono::microseconds(latencyUs));
            }
            Execute(lookup);
        });
        if (!submitted) {
            SetBadWr(badWr, const_cast<urma_jfs_wr_t *>(&wr_));
            LOG(WARNING) << "[MockUrma] PostSendWr queue full (cap=" << pool.QueueCap() << "), returning URMA_E_AGAIN";
            return URMA_E_AGAIN;
        }
        SetBadWr(badWr, nullptr);
        return URMA_SUCCESS;
    }

    static void SetBadWr(urma_jfs_wr_t **badWr, urma_jfs_wr_t *wr)
    {
        if (badWr != nullptr) {
            *badWr = wr;
        }
    }

    MockJetty &jetty_;
    const urma_jfs_wr_t &wr_;
    MockTjetty *boundTjetty_;
    const urma_sg_t &remoteSg_;
    const urma_sg_t &localSg_;
    bool isRead_;
};
}  // namespace

urma_status_t MockPostSendOne(MockJetty *jetty, const urma_jfs_wr_t *wr, urma_jfs_wr_t **badWr)
{
    if (jetty == nullptr || wr == nullptr) {
        if (badWr) {
            *badWr = const_cast<urma_jfs_wr_t *>(wr);
        }
        return URMA_E_INVALID;
    }
    auto *boundTjetty = jetty->GetDstTjetty();
    auto *wrTjetty = wr->tjetty;
    VLOG(1) << "[MockUrma] PostSendWr jetty_id=" << jetty->GetId()
            << " wrTjetty=" << (wrTjetty != nullptr ? "set" : "null")
            << " boundTjetty=" << (boundTjetty != nullptr ? "set" : "null") << " opcode=" << wr->opcode
            << " num_sge=" << wr->rw.src.num_sge;
    if (wrTjetty == nullptr && boundTjetty == nullptr) {
        // No binding (jetty not yet imported to a remote peer). Treat as no-op success.
        if (badWr) {
            *badWr = nullptr;
        }
        return URMA_SUCCESS;
    }
    const auto &src = wr->rw.src;
    const auto &dst = wr->rw.dst;
    const bool isRead = (wr->opcode == URMA_OPC_READ);
    const auto &remoteSg = isRead ? src : dst;
    const auto &localSg = isRead ? dst : src;
    if (remoteSg.sge == nullptr || remoteSg.num_sge == 0 || localSg.sge == nullptr || localSg.num_sge == 0) {
        if (badWr) {
            *badWr = nullptr;
        }
        return URMA_E_INVALID;
    }

    PostSendExecutor executor(*jetty, *wr, boundTjetty, remoteSg, localSg, isRead);
    return executor.Run(badWr);
}

urma_status_t MockPostSendWr(MockJetty *jetty, const urma_jfs_wr_t *wr, urma_jfs_wr_t **badWr)
{
    for (auto *cur = wr; cur != nullptr; cur = cur->next) {
        urma_status_t ret = MockPostSendOne(jetty, cur, badWr);
        if (ret != URMA_SUCCESS) {
            if (badWr != nullptr && *badWr == nullptr) {
                *badWr = const_cast<urma_jfs_wr_t *>(cur);
            }
            return ret;
        }
    }
    if (badWr != nullptr) {
        *badWr = nullptr;
    }
    return URMA_SUCCESS;
}

}  // namespace urma_mock
}  // namespace datasystem
