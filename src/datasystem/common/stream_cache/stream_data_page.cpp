/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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

#include <linux/futex.h>
#include <cstdint>
#include <numeric>
#include <utility>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

#include "datasystem/common/string_intern/string_ref.h"
#include "securec.h"

#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/stream_cache/stream_data_page.h"
#include "datasystem/common/util/bitmask_enum.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/memory.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/stream/stream_config.h"

namespace datasystem {
Status PageLock::FutexWait(uint32_t *lockArea, uint32_t *waitCount, uint32_t val, uint64_t timeoutMs)
{
    auto t = MilliSecondsToTimeSpec(timeoutMs);
    auto fetchVal1 = __atomic_fetch_add(waitCount, 1, __ATOMIC_SEQ_CST);
    auto res = syscall(SYS_futex, lockArea, FUTEX_WAIT, val, &t, nullptr, 0);
    auto fetchVal2 = __atomic_fetch_sub(waitCount, 1, __ATOMIC_SEQ_CST);
    // Always log if the ref count is abnormally large, it can be a sign of problem.
    const int warningVal = 1000;
    LOG_IF(INFO, fetchVal2 > warningVal) << FormatString(
        "Wait count before increment: %zu, wait count before decrement: %zu", fetchVal1, fetchVal2);
    // Examine the return code of res. EAGAIN is actually ok for FUTEX_WAIT.
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        res != -1 || errno == EAGAIN || errno == ETIMEDOUT || errno == EINTR, K_RUNTIME_ERROR,
        FormatString("Futex wait error. Errno = %d. Message %s", errno, StrErr(errno)));
    RETURN_OK_IF_TRUE(res == 0 || errno == EAGAIN || errno == EINTR);
    RETURN_STATUS(K_TRY_AGAIN, FormatString("[%s:%s] Timeout after %zu ms", __FUNCTION__, __LINE__, timeoutMs));
}

Status PageLock::FutexWake(uint32_t *lockArea, uint32_t *waitCount, int numToWakeUp)
{
    PerfPoint point1(PerfKey::PAGE_WAKE_CONSUMER);
    // syscall futex is not cheap. Only call it when there are waiters. OK if there is none.
    auto numWaiter = __atomic_load_n(waitCount, __ATOMIC_SEQ_CST);
    RETURN_OK_IF_TRUE(numWaiter == 0);
    PerfPoint point(PerfKey::PAGE_FUTEX_WAKE);
    auto res = syscall(SYS_futex, lockArea, FUTEX_WAKE, numToWakeUp, nullptr, nullptr, 0);
    point.Record();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        res != -1, K_RUNTIME_ERROR, FormatString("futex wake error. Errno = %d. Message %s", errno, StrErr(errno)));
    VLOG_IF(SC_INTERNAL_LOG_LEVEL, res > 0) << FormatString("Wake up %zu waiters", res);
    return Status::OK();
}

PageLock::PageLock(uint32_t *lockArea, uint32_t *waitArea, uint32_t lockId)
    : lockFlag_(lockArea), waitCount_(waitArea), lockId_(lockId)
{
}

Status PageLock::Lock(uint64_t timeoutMs)
{
    const uint64_t minTimeoutMs = 5;
    timeoutMs = std::max(minTimeoutMs, timeoutMs);
    PerfPoint point(PerfKey::PAGE_INSERT_GET_LOCK);
    Timer timer;
    uint64_t useTimeMs = 0;
    const uint64_t futexThreshold = 10;
    auto lockFunc = [this]() {
        uint32_t val = __atomic_load_n(lockFlag_, __ATOMIC_SEQ_CST);
        if (val & WRITE_LOCK_NUM) {
            return false;
        }
        // We only need the lower order bit for locked or unlocked. The rest of the bits are used to
        // store the lock id.
        uint32_t lockVal = (lockId_ << SHIFT) | WRITE_LOCK_NUM;
        // Compare and set lock area
        return __atomic_compare_exchange_n(lockFlag_, &val, lockVal, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
    };
    do {
        // We will do some hybrid approach. If we can't get the lock, spin for certain number of times.
        // The reason is producer will not hold the page lock for a long time. More precisely,
        // the producer only holds the lock to get the offset which shouldn't take long.
        // After certain number of spins, and we still can't get the lock, we will do a futex wait.
        RETURN_OK_IF_TRUE(lockFunc());
        useTimeMs = static_cast<uint64_t>(timer.ElapsedMilliSecond());
        CHECK_FAIL_RETURN_STATUS(useTimeMs < timeoutMs, K_TRY_AGAIN,
                                 FormatString("[%s:%s] Timeout after %zu ms", __FUNCTION__, __LINE__, timeoutMs));
        if (static_cast<uint64_t>(timer.ElapsedMilliSecond() >= futexThreshold)) {
            auto remainingMs = timeoutMs - useTimeMs;
            Status rc = PageLock::FutexWait(lockFlag_, waitCount_, WRITE_LOCK_NUM, remainingMs);
            useTimeMs = static_cast<uint64_t>(timer.ElapsedMilliSecond());
            if (rc.IsOk()) {
                continue;
            }
            RETURN_IF_NOT_OK_EXCEPT(rc, K_TRY_AGAIN);
        }
    } while (useTimeMs < timeoutMs);
    RETURN_STATUS(K_TRY_AGAIN, FormatString("[%s:%s] Timeout after %zu ms", __FUNCTION__, __LINE__, timeoutMs));
}

void PageLock::Unlock()
{
    PerfPoint point(PerfKey::PAGE_INSERT_RELEASE_LOCK);
    if (__atomic_load_n(lockFlag_, __ATOMIC_SEQ_CST) & WRITE_LOCK_NUM) {
        uint32_t expectedVal = (lockId_ << SHIFT) | WRITE_LOCK_NUM;
        if (__atomic_compare_exchange_n(lockFlag_, &expectedVal, NO_LOCK_NUM, false, __ATOMIC_SEQ_CST,
                                        __ATOMIC_SEQ_CST)) {
            VLOG(SC_DEBUG_LOG_LEVEL) << "Success to unlock the write lock";
            //  There is no need to wake up all producers. Only one of them can write
            LOG_IF_ERROR(PageLock::FutexWake(lockFlag_, waitCount_, 1), "Futex unlock");
        }
    }
}

bool PageLock::TryUnlockByLockId(uint32_t lockId)
{
    // If the page is locked with lockId, construct the expected value
    uint32_t expectedVal = (lockId << SHIFT) | WRITE_LOCK_NUM;
    // Switch the lock ownership to this worker (and has lock id 0).
    uint32_t newVal = WRITE_LOCK_NUM;
    return __atomic_compare_exchange_n(lockFlag_, &expectedVal, newVal, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
}

StreamPageLock::StreamPageLock(std::shared_ptr<StreamDataPage> page) : pageLocked_(false), page_(std::move(page))
{
}

StreamPageLock::~StreamPageLock()
{
    if (pageLocked_) {
        page_->Unlock();
    }
}

Status StreamPageLock::Lock(uint64_t timeoutMs)
{
    RETURN_RUNTIME_ERROR_IF_NULL(page_);
    RETURN_IF_NOT_OK(page_->Lock(timeoutMs));
    pageLocked_ = true;
    return Status::OK();
}

void StreamPageLock::Unlock()
{
    if (pageLocked_) {
        page_->Unlock();
        pageLocked_ = false;
    }
}

void ElementHeader::Set(Ptr ptr, Size size, Version version)
{
    headerPtr_ = ptr;
    headerSize_ = size;
    headerVersion_ = version;
}

HeaderAndData::HeaderAndData(const Element &element, const ElementHeader &header, uint64_t streamNo)
    : Element(std::move(element)), ElementHeader(std::move(header)), streamNo(streamNo)
{
}

HeaderAndData::HeaderAndData(const Ptr ptr, const Size size, uint64_t streamNo) : Element(ptr, size), streamNo(streamNo)
{
}

HeaderAndData::Size HeaderAndData::TotalSize() const
{
    if (headerSize_) {
        return size + headerSize_ + sizeof(Version);
    }
    return size;
}

Status HeaderAndData::MemoryCopyTo(Ptr dest) const
{
    if (headerSize_) {
        // Copy header version number
        *dest = headerVersion_;
        dest++;
        // Copy header
        RETURN_IF_NOT_OK(HugeMemoryCopy(dest, headerSize_, headerPtr_, headerSize_));
        dest += headerSize_;
    }
    // Copy raw data
    return HugeMemoryCopy(dest, size, ptr, size);
}

DataVerificationHeader::DataVerificationHeader(SeqNo seqNo, SenderProducerNo senderProducerNo, Address address,
                                               Port port)
{
    hdr.seqNo = seqNo;
    hdr.senderProducerNo = senderProducerNo;
    hdr.address = address;
    hdr.port = port;
}

DataVerificationHeader::DataVerificationHeader(const ElementHeader &ele)
{
    HugeMemoryCopy(bytes, sizeof(bytes), ele.headerPtr_, sizeof(bytes));
}

DataVerificationHeader::SeqNo DataVerificationHeader::GetSeqNo() const
{
    return hdr.seqNo;
}

DataVerificationHeader::SenderProducerNo DataVerificationHeader::GetSenderProducerNo() const
{
    return hdr.senderProducerNo;
}

DataVerificationHeader::Address DataVerificationHeader::GetAddress() const
{
    return hdr.address;
}

DataVerificationHeader::Port DataVerificationHeader::GetPort() const
{
    return hdr.port;
}

DataVerificationHeader::Size DataVerificationHeader::HeaderSize() const
{
    return sizeof(bytes);
}

void DataVerificationHeader::Set(SeqNo seqNo, SenderProducerNo senderProducerNo, Address address, Port port)
{
    hdr.seqNo = seqNo;
    hdr.senderProducerNo = senderProducerNo;
    hdr.address = address;
    hdr.port = port;
}

Status DataVerificationHeader::ExtractHeader(DataElement &element, ElementHeader &header)
{
    CHECK_FAIL_RETURN_STATUS(
        element.size > sizeof(bytes), K_OUT_OF_RANGE,
        FormatString("Element (header + data) size %llu is not greater than DataVerificationHeader size %lu",
                     element.size, sizeof(bytes)));
    header.Set(element.ptr, sizeof(bytes), DATA_VERIFICATION_HEADER);
    element.ptr += sizeof(bytes);
    element.size -= sizeof(bytes);
    return Status::OK();
}

ShmKey StreamPageBase::CreatePageId(const std::shared_ptr<ShmUnitInfo> &shmInfo)
{
    return ShmKey::Intern(
        FormatString("F:%zu-M:%zu-O:%zu-S:%zu", shmInfo->fd, shmInfo->mmapSize, shmInfo->offset, shmInfo->size));
}

StreamPageBase::StreamPageBase(std::shared_ptr<ShmUnitInfo> shmInfo)
{
    pageUnit_ = std::move(shmInfo);
    // Use the shmView as the id string rather than generating uuid
    pageUnit_->id = CreatePageId(pageUnit_);
}

StreamPageBase::StreamPageBase(std::shared_ptr<ShmUnitInfo> shmInfo, std::shared_ptr<client::MmapTableEntry> mmapEntry)
    : StreamPageBase(std::move(shmInfo))
{
    mmapEntry_ = std::move(mmapEntry);
}

void StreamPageBase::Init(bool isClient)
{
    startOfPage_ = reinterpret_cast<uint8_t *>(pageUnit_->pointer) + ((isClient) ? pageUnit_->offset : 0);
}

ShmView StreamPageBase::GetShmView() const
{
    ShmView v = { .fd = pageUnit_->fd, .mmapSz = pageUnit_->mmapSize, .off = pageUnit_->offset, .sz = pageUnit_->size };
    return v;
}

std::shared_ptr<ShmUnitInfo> StreamPageBase::GetShmUnitInfo() const
{
    // Return a new copy of pageUnit_, not pageUnit_ itself.
    return std::make_shared<ShmUnitInfo>(pageUnit_->id, GetShmView(), pageUnit_->pointer);
}

StreamLobPage::StreamLobPage(std::shared_ptr<ShmUnitInfo> shmInfo, bool isClient)
    : StreamPageBase(std::move(shmInfo)), isClient_(isClient)
{
}

StreamLobPage::StreamLobPage(std::shared_ptr<ShmUnitInfo> shmInfo, bool isClient,
                             std::shared_ptr<client::MmapTableEntry> mmapEntry)
    : StreamPageBase(std::move(shmInfo), std::move(mmapEntry)), isClient_(isClient)
{
}

Status StreamLobPage::Insert(const HeaderAndData &element)
{
    size_t totalFreeSpace = pageUnit_->size;
    auto spaceNeeded = element.TotalSize();
    CHECK_FAIL_RETURN_STATUS(spaceNeeded <= totalFreeSpace, K_NO_SPACE, "Not enough space");
    RETURN_IF_NOT_OK(element.MemoryCopyTo(startOfPage_));
    LOG(INFO) << FormatString("[%s] Big element insert successful. Size %zu", GetPageId(), element.size);
    return Status::OK();
}

Status StreamLobPage::Init()
{
    RETURN_RUNTIME_ERROR_IF_NULL(pageUnit_->pointer);
    StreamPageBase::Init(isClient_);
    return Status::OK();
}

StreamDataPage::StreamDataPage(std::shared_ptr<ShmUnitInfo> shmInfo, uint32_t lockId, bool isClient, bool isSharedPage,
                               std::shared_ptr<client::MmapTableEntry> mmapEntry)
    : StreamPageBase(std::move(shmInfo), std::move(mmapEntry)),
      lockId_(lockId),
      isClient_(isClient),
      maxElementSize_(0),
      isSharedPage_(isSharedPage)
{
}

Status StreamDataPage::Init()
{
    RETURN_RUNTIME_ERROR_IF_NULL(pageUnit_->pointer);
    StreamPageBase::Init(isClient_);
    // We are going to traverse the shared memory page to set up various pointers
    // based on known offsets.
    auto *data = startOfPage_;
    pageHeader_ = reinterpret_cast<decltype(pageHeader_)>(data);
    // Tail leading to the next StreamDataPage. It is at the end of the page. 32 bytes.
    tail_ = reinterpret_cast<SharedMemView *>(data + PageSize() - sizeof(SharedMemView));
    // First area is the lock area. Always round up to 8 bytes in size.
    pageLock_ = std::make_shared<PageLock>(&pageHeader_->lockArea_, &pageHeader_->lockWait_, lockId_);
    // Start of the slot directory. Each slot is 4 byte. Slot directory grows forward and Element data are
    // packed at the end of the page before the tail, and grows backward.
    slotDir_ = reinterpret_cast<SlotFlagOffset *>(&pageHeader_->slot0_);
    if (isClient_) {
        auto slot0 = __atomic_load_n(slotDir_, __ATOMIC_SEQ_CST);
        isSharedPage_ = slot0 & PAGE_SHARED_BIT;
    }
    // Compute how much space left. Slot 0 is in use. Space for slot 1 is pre-allocated.
    // The remaining space (not counting the tail) should be at least big enough to hold 1 byte of element
    maxElementSize_ = static_cast<int64_t>(PageSize()) - static_cast<int64_t>(PageOverhead(isSharedPage_));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(maxElementSize_ > 0, K_INVALID,
                                         FormatString("Page size %zu too small", PageSize()));
    nextPage_ = std::make_shared<SharedMemViewImpl>(tail_, sizeof(SharedMemView), lockId_);
    RETURN_IF_NOT_OK(nextPage_->Init());
    return Status::OK();
}

Status StreamDataPage::ResetToEmpty()
{
    // This is very much like InitEmptyPage except the underlying page has been
    // called InitEmptyPage() earlier. We only reset a few things to allow to
    // be reused as empty page.
    CHECK_FAIL_RETURN_STATUS(!isClient_, K_INVALID, "Only worker can init the page");
    // Lock the page
    StreamPageLock xlock(shared_from_this());
    RETURN_IF_NOT_OK(xlock.Lock(std::numeric_limits<uint64_t>::max()));
    // Wait until the reference drop to 1. This is to solve a racing condition that a producer fixed
    // a page that has been recycled. We must drain the producers. These late producers will time
    // out waiting for the lock and unfix the current page.
    do {
        uint32_t expected = __atomic_load_n(&pageHeader_->refCount_, __ATOMIC_RELAXED);
        if (expected == 1) {
            break;
        }
        VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("Waiting for page<%s> to be unreferenced. Current ref count %zu",
                                                  GetPageId(), expected);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    } while (true);
    auto totalFreeSpace = PagePayloadSize();
    __atomic_store_n(&pageHeader_->totalFreeSpace_, totalFreeSpace, __ATOMIC_RELAXED);
    // Slot count back to 0.
    __atomic_store_n(&pageHeader_->slotCount_, 0, __ATOMIC_RELAXED);
    // begCursor is 0 so function like Insert can detect the page has been recycled.
    __atomic_store_n(&pageHeader_->begCursor_, 0, __ATOMIC_RELAXED);
    // Clear the next pointer
    nextPage_->SetView(ShmView(), false, std::numeric_limits<uint64_t>::max());
    // Clear the page level BigElement bit
    UnsetPageHasBigElement();
    return Status::OK();
}

Status StreamDataPage::InitEmptyPage()
{
    CHECK_FAIL_RETURN_STATUS(!isClient_, K_INVALID, "Only worker can init the page");
    size_t freeSpace = PageSize();
    // Clear everything up to slotDir_
    size_t destSz = reinterpret_cast<uint8_t *>(slotDir_) - startOfPage_;
    freeSpace -= destSz;  // before slot0
    auto rc = memset_s(startOfPage_, PageSize(), 0, destSz);
    CHECK_FAIL_RETURN_STATUS(rc == 0, K_RUNTIME_ERROR, FormatString("memset_s fails. Errno = %d", errno));
    // Clear the tail pointer which is at the end of the page
    uint8_t *endOfPage = startOfPage_ + PageSize();
    auto *nextPtr = reinterpret_cast<uint8_t *>(tail_);
    destSz = endOfPage - nextPtr;
    RETURN_IF_NOT_OK(nextPage_->Init(true));
    freeSpace -= destSz;  // SharedMemView
    // Manually set those fields that are not zero.
    freeSpace -= GetMetaSize(isSharedPage_);  // slot 0
    // Free space is the amount of space left.
    // Another way to verify the total free space of an empty page (see TryUnlockByLockId) and they should match
    auto totalFreeSpace = PagePayloadSize();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        freeSpace == totalFreeSpace, K_RUNTIME_ERROR,
        FormatString("Free space mismatch. Expect %zu but get %zu", freeSpace, totalFreeSpace));
    // Must be able to insert one element with max size
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        static_cast<size_t>(maxElementSize_) + GetMetaSize(isSharedPage_) == freeSpace, K_RUNTIME_ERROR,
        FormatString("Expect max element size %zu + one slot == free space %zu", maxElementSize_, freeSpace));
    __atomic_store_n(&pageHeader_->totalFreeSpace_, freeSpace, __ATOMIC_SEQ_CST);
    // Init first cursor on this page to 1 for now. It will be updated later by the caller.
    __atomic_store_n(&pageHeader_->begCursor_, 1, __ATOMIC_SEQ_CST);
    // One reference to this page
    __atomic_store_n(&pageHeader_->refCount_, 1, __ATOMIC_SEQ_CST);
    // The slot directory is always one plus the number of elements such that
    // the size of an element is inferred from the neighbour offset.
    auto offset = static_cast<SlotOffset>(nextPtr - reinterpret_cast<uint8_t *>(slotDir_));
    auto slotFlag = isSharedPage_ ? PAGE_SHARED_BIT : 0;
    auto slotAddr = GetSlotAddr(0);
    slotAddr->StoreAll(isSharedPage_, slotFlag, offset, 0);
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString(
        "Init page<%s> success. freeSpace = %zu, maxElementSize = %zu, slotDir_[0] = %d", GetPageId(), freeSpace,
        maxElementSize_, slotDir_[0]);
    return Status::OK();
}

Status StreamDataPage::Lock(uint64_t timeoutMs)
{
    return pageLock_->Lock(timeoutMs);
}

void StreamDataPage::Unlock()
{
    pageLock_->Unlock();
}

void StreamDataPage::TryUnlockByLockId(uint32_t lockId)
{
    if (pageLock_->TryUnlockByLockId(lockId)) {
        // If we can unlock, we now own the lock. Previous producer held the lock while it crashed,
        // the page can be in an inconsistent state, let's fix up (if possible) before we unlock
        // (a) Decrement the reference count.
        (void)ReleasePage(FormatString("%s:%s", __FUNCTION__, __LINE__));
        // (b) The in flight slot directory and the totalFreeSpace can't be trusted anymore
        //     if the producer crashed after it held the lock but before it can release the lock.
        //     What we can do is bring both slot count to a slot with the consistency bit.
        //     total free space must be recalculated.
        uint32_t slotCount = 0;
        auto pendingSlotCount = GetSlotCount();
        auto totalFreeSpace = PagePayloadSize();
        for (uint32_t i = 0; i < pendingSlotCount; ++i) {
            auto slot = i + 1;  // slot directory is always one plus more.
            if (GetSlotFlag(slot) & ELEMENT_DATA_CONSISTENT) {
                ++slotCount;
                totalFreeSpace -= GetMetaSize(isSharedPage_);
                // Keep in mind the slot directory grow forward but elements grow backward. So its length
                // should be calculated from the previous slot.
                totalFreeSpace -= (GetSlotOffset(slot - 1) - GetSlotOffset(slot));
            } else {
                break;
            }
        }
        __atomic_store_n(&pageHeader_->slotCount_, slotCount, __ATOMIC_SEQ_CST);
        __atomic_store_n(&pageHeader_->totalFreeSpace_, totalFreeSpace, __ATOMIC_SEQ_CST);
        auto begCursor = pageHeader_->begCursor_;
        VLOG(SC_NORMAL_LOG_LEVEL) << FormatString(
            "[Page:%s] Page recover success. begCursor = %zu, slot count = %zu, freeSpace = %zu", GetPageId(),
            begCursor, slotCount, totalFreeSpace);
        // Let go of the lock
        Unlock();
    }
}

uint64_t StreamDataPage::GetBegCursor() const
{
    auto begCursor = __atomic_load_n(&pageHeader_->begCursor_, __ATOMIC_RELAXED);
    return begCursor;
}

uint32_t StreamDataPage::GetSlotCount() const
{
    return __atomic_load_n(&pageHeader_->slotCount_, __ATOMIC_ACQUIRE);
}

uint64_t StreamDataPage::GetLastCursor() const
{
    return GetBegCursor() + GetSlotCount() - 1;
}

bool StreamDataPage::Empty() const
{
    return GetSlotCount() == 0;
}

void StreamDataPage::UpdateSlotConsistentBit(uint32_t slot)
{
    auto slotAddr = GetSlotAddr(slot);
    slotAddr->SetFlagBit(ELEMENT_DATA_CONSISTENT);
}

uint32_t StreamDataPage::GetRefCount() const
{
    return __atomic_load_n(&pageHeader_->refCount_, __ATOMIC_RELAXED);
}

Status StreamDataPage::RefPage(const std::string &logPrefix)
{
    PerfPoint point(PerfKey::PAGE_REF_INC);
    auto curCount = __atomic_fetch_add(&pageHeader_->refCount_, 1, __ATOMIC_RELAXED);
    if (VLOG_IS_ON(SC_INTERNAL_LOG_LEVEL)) {
        VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s, Page:%s] refCount after increase: %zu", logPrefix,
                                                    GetPageId(), 1 + curCount);
    }
    return Status::OK();
}

Status StreamDataPage::ReleasePage(const std::string &logPrefix)
{
    // All callers of ReleasePage pass in a log prefix containing the stream name
    PerfPoint point(PerfKey::PAGE_REF_DEC);
    constexpr static uint64_t MIN_REF_COUNT = 2;
    bool success = false;
    uint32_t curCount = 0;
    do {
        curCount = __atomic_load_n(&pageHeader_->refCount_, __ATOMIC_SEQ_CST);
        // The initial reference count is always 1 when it is created.
        // RefPage/ReleasePage must be called in the correct order.
        // We can't call ReleasePage first and then followed by RefPage.
        // So in this case, we expect the reference count is at least two.
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            curCount >= MIN_REF_COUNT, K_RUNTIME_ERROR,
            FormatString("[%s, Page:%s] Unexpected reference count %zu", logPrefix, GetPageId(), curCount));
        success = __atomic_compare_exchange_n(&pageHeader_->refCount_, &curCount, curCount - 1, false, __ATOMIC_RELAXED,
                                              __ATOMIC_RELAXED);
    } while (!success);
    // Always log if the ref count is abnormally large, it can be a sign of problem.
    const int warningVal = 1000;
    if (curCount > warningVal) {
        LOG(INFO) << FormatString("[%s, Page:%s] refCount after decrease: %zu", logPrefix, GetPageId(), curCount - 1);
    } else {
        const int logPerCount = VLOG_IS_ON(SC_INTERNAL_LOG_LEVEL) ? 1 : 1000;
        LOG_EVERY_N(INFO, logPerCount) << FormatString("[%s, Page:%s] refCount after decrease: %zu", logPrefix,
                                                       GetPageId(), curCount - 1);
    }
    return Status::OK();
}

bool StreamDataPage::HasNextPage() const
{
    ShmView v = GetNextPage();
    return v.fd != 0 && v.fd != -1;
}

void StreamDataPage::SetNextPage(const ShmView &shm)
{
    nextPage_->SetView(shm, false, std::numeric_limits<uint64_t>::max());
}

ShmView StreamDataPage::GetNextPage() const
{
    ShmView v;
    bool isFreePage;
    nextPage_->GetView(v, isFreePage, std::numeric_limits<uint64_t>::max());
    // This form of GetNext only returns a view if the page is in use.
    if (isFreePage) {
        return {};
    }
    return v;
}

Status StreamDataPage::WakeUpConsumers()
{
    return PageLock::FutexWake(&pageHeader_->slotCount_, &pageHeader_->slotWait_);
}

inline void SetAttributeBits(InsertFlags flags, SlotFlag &offset)
{
    if (TESTFLAG(flags, InsertFlags::REMOTE_ELEMENT)) {
        offset |= REMOTE_ELEMENT_BIT;
    }
    if (TESTFLAG(flags, InsertFlags::BIG_ELEMENT)) {
        offset |= BIG_ELEMENT_BIT;
    }
    if (TESTFLAG(flags, InsertFlags::HEADER)) {
        offset |= HEADER_BIT;
    }
}

void StreamDataPage::SetPageHasBigElement()
{
    // It is an expensive operation to traverse each slot to check if is
    // a big element row. To optimize the work, we are going to steal
    // the high bits of slot0 which is never use until now. If the
    // bit is set, there exists at one big element
    SlotFlagOffset offset = __atomic_load_n(slotDir_, __ATOMIC_ACQUIRE);
    offset |= BIG_ELEMENT_BIT;
    __atomic_store_n(slotDir_, offset, __ATOMIC_RELEASE);
}

void StreamDataPage::UnsetPageHasBigElement()
{
    // It is an expensive operation to traverse each slot to check if is
    // a big element row. To optimize the work, we are going to steal
    // the high bits of slot0 which is never use until now. If the
    // bit is set, there exists at one big element
    SlotFlagOffset offset = __atomic_load_n(slotDir_, __ATOMIC_ACQUIRE);
    offset &= ~BIG_ELEMENT_BIT;
    __atomic_store_n(slotDir_, offset, __ATOMIC_RELEASE);
}

bool StreamDataPage::PageHasBigElement()
{
    SlotFlagOffset offset = __atomic_load_n(slotDir_, __ATOMIC_RELAXED);
    return TESTFLAG(offset, BIG_ELEMENT_BIT);
}

size_t StreamDataPage::GetMetaSize(bool isSharedPage)
{
    return isSharedPage ? sizeof(SlotType) : sizeof(SlotFlagOffset);
}

size_t StreamDataPage::PageOverhead(bool isSharedPage)
{
    // Everything up to and including slot0, and we can use slot1 as a reference.
    // Also take account of the next page pointer. Also reserve space for slot 1.
    // To prevent the compiler adding any padding, we explicitly use offsetof plus
    // sizeof rather than to use sizeof(StreamPageHeader). Without the 'packed'
    // attribute, c++ compiler can the structure with another 4 bytes at the end
    // of the struct.
    return offsetof(StreamPageHeader, slot0_) + sizeof(SharedMemView) + GetMetaSize(isSharedPage)
           + GetMetaSize(isSharedPage);
}

size_t StreamDataPage::PagePayloadSize()
{
    return PageSize() - offsetof(StreamPageHeader, slot0_) - sizeof(SharedMemView) - GetMetaSize(isSharedPage_);
}

size_t StreamDataPage::GetFreeSpaceSize()
{
    return __atomic_load_n(&pageHeader_->totalFreeSpace_, __ATOMIC_RELAXED);
}

SlotFlag StreamDataPage::GetSlotFlag(size_t index)
{
    auto addr = GetSlotAddr(index);
    return isSharedPage_ ? addr->value.flag : (addr->flagWithOffset & ~SLOT_VALUE_MASK);
}

SlotOffset StreamDataPage::GetSlotOffset(size_t index)
{
    auto addr = GetSlotAddr(index);
    return isSharedPage_ ? addr->value.offset : (addr->flagWithOffset & SLOT_VALUE_MASK);
}

SlotType *StreamDataPage::GetSlotAddr(size_t index)
{
    if (!isSharedPage_) {
        return reinterpret_cast<SlotType *>(slotDir_ + index);
    }
    // (flag0, offset0, streamNo0), (flag1, offset1, streamNo1), ...
    return reinterpret_cast<SlotType *>(slotDir_) + index;
}

Status StreamDataPage::ExtractBigElementsUpTo(uint64_t ackCursor, std::vector<std::pair<uint64_t, ShmView>> &bigId,
                                              bool deCouple)
{
    auto begCursor = GetBegCursor();
    RETURN_OK_IF_TRUE(ackCursor < begCursor);
    // Because we are modifying the page, we need to lock to block
    // other producers.
    StreamPageLock pageLock(shared_from_this());
    const uint64_t DEFAULT_TIMEOUT_MS = 1000;
    RETURN_IF_NOT_OK(pageLock.Lock(DEFAULT_TIMEOUT_MS));
    RETURN_OK_IF_TRUE(!PageHasBigElement());
    size_t offset1 = reinterpret_cast<uint8_t *>(slotDir_) - startOfPage_;
    auto slotCount = GetSlotCount();
    for (size_t i = 0; i < slotCount; ++i) {
        uint64_t cursor = begCursor + i;
        if (cursor > ackCursor) {
            break;
        }
        auto slotAddr = GetSlotAddr(i + 1);
        DataElement ele;
        ele.attr_ = slotAddr->LoadFlag(isSharedPage_);
        // Like Receive, if the data is not ready, break out from the loop. We will resume again next time.
        if (!ele.DataIsReady()) {
            break;
        }
        if (!ele.IsBigElement()) {
            continue;
        }
        auto offset = slotAddr->LoadOffset(isSharedPage_);
        SlotOffset b4 = GetSlotAddr(i)->LoadOffset(isSharedPage_);
        ele.size = b4 - offset;
        ele.ptr = startOfPage_ + offset1 + offset;
        ele.id = cursor;
        // We are going to decouple the pointer to the big element page, and turn off the BigElement bit.
        // If we need to revisit this page again, we will then ignore it.
        ShmView pageView;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ParseShmViewPb(ele.ptr, ele.size, pageView), "ReleaseBigElementsUpTo");
        if (deCouple) {
            // We no longer consider this as a big element row.
            slotAddr->ClearFlagBit(BIG_ELEMENT_BIT);
        }
        bigId.emplace_back(ele.id, pageView);
    }
    return Status::OK();
}

Status StreamDataPage::Insert(const HeaderAndData &element, uint64_t timeoutMs, InsertFlags &flags,
                              const std::string &logPrefix)
{
    INJECT_POINT("producer_insert");
    PerfPoint point(PerfKey::PAGE_INSERT_ELEMENT);
    auto *totalFreeSpace_ = &pageHeader_->totalFreeSpace_;
    auto *slotCount_ = &pageHeader_->slotCount_;
    size_t finalElementSize = element.TotalSize();
    size_t spaceNeeded = GetMetaSize(isSharedPage_) + finalElementSize;
    // Make sure this element is not exceeding the maximum free space. There is no way
    // any page can hold the big element. We need to account a new slot for Element.
    // Slot 0 is in use. Space for slot 1 is pre-allocated,
    // and so we can allow element of max stream element size.
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        finalElementSize <= static_cast<size_t>(maxElementSize_), K_INVALID,
        FormatString("Element size %zu (plus internal overhead) is exceeding the maximum free space %zu",
                     finalElementSize, maxElementSize_));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(element.size > 0, K_INVALID, "Element size should be greater than 0");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(element.ptr != nullptr, K_INVALID, "Element ptr should not be a nullptr");
    // The maximum length we can support is 30 bits
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        (finalElementSize & ~(static_cast<uint64_t>(SLOT_VALUE_MASK))) == 0, K_INVALID,
        FormatString("Element size %zu is exceeding the maximum length", finalElementSize));
    StreamPageLock pageLock(shared_from_this());
    // A few shortcuts before we try to hold the lock for insert.
    // These methods involve looking at some atomic fields.
    // (a) If the totalFreeSpace is too small, don't bother to get the lock
    auto totalFreeSpace = __atomic_load_n(totalFreeSpace_, __ATOMIC_RELAXED);
    CHECK_FAIL_RETURN_STATUS(spaceNeeded <= totalFreeSpace, K_NO_SPACE, "Not enough space");
    // (b) If this page has a next pointer. If there is one, just follow the pointer.
    CHECK_FAIL_RETURN_STATUS(!HasNextPage(), K_SC_END_OF_PAGE, "Check next page for new elements");
    // Now we lock the page for insert because this page is shared by producers.
    // Consumers on the other hand traverse the page without any lock.
    if (!TESTFLAG(flags, InsertFlags::SKIP_LOCK)) {
        RETURN_IF_NOT_OK(pageLock.Lock(timeoutMs));
    }
    INJECT_POINT("producer_obtained_lock");
    // There is a racing condition that a page can be cycled and put into the free list.
    // One way to detect this page is on a free list is check the begCursor_.
    auto begCursor = __atomic_load_n(&pageHeader_->begCursor_, __ATOMIC_RELAXED);
    CHECK_FAIL_RETURN_STATUS(begCursor > 0, K_TRY_AGAIN, "Page is already recycled");
    // After we get the lock, do the same check again
    CHECK_FAIL_RETURN_STATUS(!HasNextPage(), K_SC_END_OF_PAGE, "Check next page for new elements.");
    totalFreeSpace = __atomic_load_n(totalFreeSpace_, __ATOMIC_RELAXED);
    CHECK_FAIL_RETURN_STATUS(spaceNeeded <= totalFreeSpace, K_NO_SPACE, "Not enough space");
    totalFreeSpace = __atomic_sub_fetch(totalFreeSpace_, spaceNeeded, __ATOMIC_RELAXED);
    INJECT_POINT("producer_update_free_space");
    auto numElement = __atomic_load_n(slotCount_, __ATOMIC_ACQUIRE);

    SlotOffset offset = GetSlotOffset(numElement) - static_cast<SlotOffset>(finalElementSize);
    uint8_t *dest = reinterpret_cast<uint8_t *>(slotDir_) + offset;
    // We will do asynchronous memory copy by letting go of the page lock
    // once we know where we will write the data to.
    // Need to distinguish an inserted element is from a local producer or a remote producer.
    // If coming from a remote producer, set the high bit. Same for big element
    SlotFlag slotFlag = 0;
    SetAttributeBits(flags, slotFlag);
    // The slot directory is always one plus the number of elements.
    auto slotAddr = GetSlotAddr(numElement + 1);
    slotAddr->StoreAll(isSharedPage_, slotFlag, offset, element.streamNo);
    if (TESTFLAG(flags, InsertFlags::BIG_ELEMENT)) {
        SetPageHasBigElement();
    }
    INJECT_POINT("producer_update_slot_directory");
    // Slot count is the last step to update. Consumer may futex sleep on the slotCount_
    // and we should only it when free space and slot offset are set.
    __atomic_store_n(slotCount_, 1 + numElement, __ATOMIC_RELEASE);
    INJECT_POINT("producer_update_pending_slot_count_holding_lock");
    // Let go of the lock
    pageLock.Unlock();
    INJECT_POINT("producer_update_pending_slot_count_without_lock");
    // Caution! After this point, we no longer hold any lock.
    PerfPoint perfPoint(PerfKey::PAGE_ELEMENT_MEMORY_COPY);
    RETURN_IF_NOT_OK(element.MemoryCopyTo(dest));
    perfPoint.RecordAndReset(PerfKey::PAGE_CAS_SLOT_COUNT);
    // Update the slot with the consistent bit
    UpdateSlotConsistentBit(numElement + 1);
    perfPoint.Record();
    if (!TESTFLAG(flags, InsertFlags::DELAY_WAKE)) {
        RETURN_IF_NOT_OK(WakeUpConsumers());
    }
    const int logPerCount = VLOG_IS_ON(SC_INTERNAL_LOG_LEVEL) ? 1 : 1000;
    LOG_EVERY_N(INFO, logPerCount) << FormatString(
        "[%sCursor %zu] Add element success. slot = %zu, offset = %zu, length = %zu, freeSpace = %zu, bigEle = %s, "
        "header = %s, sharedPage = %s, streamNo = %zu, pageId = %s",
        (!logPrefix.empty() ? logPrefix + " " : ""), pageHeader_->begCursor_ + numElement, numElement + 1, offset,
        finalElementSize, totalFreeSpace, BoolToString(TESTFLAG(flags, InsertFlags::BIG_ELEMENT)),
        BoolToString(TESTFLAG(flags, InsertFlags::HEADER)), BoolToString(isSharedPage_), element.streamNo, GetPageId());
    SETFLAG(flags, InsertFlags::INSERT_SUCCESS);
    return Status::OK();
}

Status CalcMaxAllowRows(void *buf, std::vector<size_t> &sz, const size_t totalFreeSpace, bool isSharedPage,
                        StreamMetaShm *streamMetaShm, uint8_t *&src, size_t &spaceNeeded, size_t &numInsert,
                        size_t &totalLength)
{
    size_t bufSz = std::accumulate(sz.begin(), sz.end(), 0ul);
    // Elements are packed in reverse order. We will find out how many elements we can insert
    // and the caller can continue from where we left off next time.
    src = reinterpret_cast<uint8_t *>(buf) + bufSz;
    spaceNeeded = 0;
    for (size_t i = 0; i < sz.size(); ++i) {
        auto eleSz = sz[i];
        auto sizeNeeded = eleSz + StreamDataPage::GetMetaSize(isSharedPage);  // account for one slot
        if (spaceNeeded + sizeNeeded > totalFreeSpace) {
            break;
        }
        if (streamMetaShm != nullptr) {
            RETURN_IF_NOT_OK(streamMetaShm->TryIncUsage(eleSz));
        }
        // We can take this element and fit on the page
        spaceNeeded += sizeNeeded;
        src -= eleSz;  // Elements are packed in reverse order
        totalLength += eleSz;
        ++numInsert;
    }
    return Status::OK();
}

Status StreamDataPage::BatchInsert(void *buf, std::vector<size_t> &sz, uint64_t timeoutMs,
                                   std::pair<size_t, size_t> &res, InsertFlags flags,
                                   const std::vector<bool> &headerBits, StreamMetaShm *streamMetaShm)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!isSharedPage_, K_RUNTIME_ERROR,
                                         FormatString("BatchInsert not allow apply for shared page %s", GetPageId()));
    size_t numInsert = 0;
    size_t totalLength = 0;
    Raii raii([&res, &numInsert, &totalLength]() {
        res.first = numInsert;
        res.second = totalLength;
    });
    // This is a special form of batch insert mainly for remote worker.
    auto *totalFreeSpace_ = &pageHeader_->totalFreeSpace_;
    auto *slotCount_ = &pageHeader_->slotCount_;
    StreamPageLock pageLock(shared_from_this());
    if (!TESTFLAG(flags, InsertFlags::SKIP_LOCK)) {
        RETURN_IF_NOT_OK(pageLock.Lock(timeoutMs));
    }
    // There is a racing condition that a page can be cycled and put into the free list.
    // One way to detect this page is on a free list is check the begCursor_.
    auto begCursor = __atomic_load_n(&pageHeader_->begCursor_, __ATOMIC_RELAXED);
    CHECK_FAIL_RETURN_STATUS(begCursor > 0, K_TRY_AGAIN, "Page is already recycled");
    // (a) If this page has a next pointer. If there is one, just follow the pointer.
    CHECK_FAIL_RETURN_STATUS(!HasNextPage(), K_SC_END_OF_PAGE, "Check next page for new elements");
    auto totalFreeSpace = __atomic_load_n(totalFreeSpace_, __ATOMIC_RELAXED);
    // Elements are packed in reverse order. We will find out how many elements we can insert
    // and the caller can continue from where we left off next time.
    uint8_t *src = nullptr;
    size_t spaceNeeded = 0;
    auto rc = CalcMaxAllowRows(buf, sz, totalFreeSpace, isSharedPage_, streamMetaShm, src, spaceNeeded, numInsert,
                               totalLength);
    // (b) If the totalFreeSpace is too small, next page.
    if (numInsert == 0) {
        return rc.IsError() ? rc : Status(K_NO_SPACE, "Not enough space");
    }
    uint32_t numElement = __atomic_load_n(slotCount_, __ATOMIC_ACQUIRE);
    totalFreeSpace = __atomic_sub_fetch(totalFreeSpace_, spaceNeeded, __ATOMIC_RELAXED);
    for (size_t i = 0; i < numInsert; ++i) {
        auto slot = numElement + i + 1;  // slot directory is always one more
        SlotFlagOffset offset = GetSlotOffset(slot - 1) - static_cast<uint32_t>(sz[i]);
        if (headerBits[i]) {
            SetAttributeBits(flags | InsertFlags::HEADER, offset);
        } else {
            SetAttributeBits(flags, offset);
        }
        __atomic_store_n(static_cast<SlotFlagOffset *>(slotDir_ + slot), offset, __ATOMIC_RELEASE);
    }
    if (TESTFLAG(flags, InsertFlags::BIG_ELEMENT)) {
        SetPageHasBigElement();
    }
    uint8_t *dest = reinterpret_cast<uint8_t *>(slotDir_) + GetSlotOffset(numElement + numInsert);
    // Slot count is the last step to update. Consumer may futex sleep on the slotCount_
    // and we should only it when free space and slot offset are set.
    __atomic_store_n(slotCount_, numElement + static_cast<int32_t>(numInsert), __ATOMIC_RELEASE);
    // Just like the Insert case, we will let go of the lock once we get all the offsets
    pageLock.Unlock();
    // Caution! After this point, we no longer hold any lock.
    RETURN_IF_NOT_OK(HugeMemoryCopy(dest, totalLength, src, totalLength));
    for (size_t i = 0; i < numInsert; ++i) {
        auto slot = numElement + i + 1;  // slot directory is always one more
        UpdateSlotConsistentBit(slot);
    }
    // Wake up (futex) any reader
    if (!TESTFLAG(flags, InsertFlags::DELAY_WAKE)) {
        RETURN_IF_NOT_OK(WakeUpConsumers());
    }
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString(
        "Batch add %zu element into %s success. "
        "begCursor = %zu, freeSpace = %zu, bigEle = %s",
        numInsert, GetPageId(), pageHeader_->begCursor_ + numElement, totalFreeSpace,
        BoolToString(TESTFLAG(flags, InsertFlags::BIG_ELEMENT)));
    return Status::OK();
}

Status StreamDataPage::WaitForNewElement(uint64_t lastRecvCursor, uint64_t timeoutMs)
{
    uint32_t slotCount = GetSlotCount();
    auto endCursor = pageHeader_->begCursor_ + slotCount;
    // Early exit if we have some new elements since the last read
    RETURN_OK_IF_TRUE(lastRecvCursor + 1 < endCursor);
    // If there is a next page, check the next page.
    if (HasNextPage()) {
        // But refresh the slotCount and check again. This slotCount is final.
        slotCount = GetSlotCount();
        endCursor = pageHeader_->begCursor_ + slotCount;
        RETURN_OK_IF_TRUE(lastRecvCursor + 1 < endCursor);
        // Now we truly exhaust all the elements on this page
        RETURN_STATUS(K_SC_END_OF_PAGE, "Check next page for new elements.");
    }
    // In all other cases, continue to futex wait.
    if (timeoutMs == 0) {
        RETURN_STATUS(K_TRY_AGAIN, "Non-blocking call and there is no new element inserted");
    }
    INJECT_POINT("StreamDataPage.WaitOnFutexForever", [this, timeoutMs, slotCount]() {
        // This is for the testcase ProducerTest::TestConsumerFutexWake. We wait until
        // the producer signal us to proceed to wait on the futex
        LOG(INFO) << "Wait for signal from producer. TimeoutMs = " << timeoutMs << ". slotCount = " << slotCount;
        while (!HasNextPage()) {
            std::this_thread::yield();
        }
        // Wait a bit before we wait on the futex
        const auto sleepMs = 5'000ul;
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepMs));
        return Status::OK();
    });
    Status rc = PageLock::FutexWait(&pageHeader_->slotCount_, &pageHeader_->slotWait_, slotCount, timeoutMs);
    if (rc.IsOk()) {
        // Update the slotCount again for spurious wake up
        slotCount = GetSlotCount();
        endCursor = pageHeader_->begCursor_ + slotCount;
        RETURN_OK_IF_TRUE(lastRecvCursor + 1 < endCursor);
        // We can also be waked up CreateNewPage. But let the caller handle it.
        RETURN_STATUS(K_TRY_AGAIN, "No new element inserted");
    }
    return rc;
}

Status StreamDataPage::ParseShmViewPb(const void *ptr, size_t sz, ShmView &out)
{
    ShmViewPb pb;
    bool success = pb.ParseFromArray(ptr, sz);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(success, K_OUT_OF_RANGE, "ShmViewPb parse error");
    ShmView v{ .fd = pb.fd(), .mmapSz = pb.mmap_size(), .off = static_cast<ptrdiff_t>(pb.offset()), .sz = pb.size() };
    out = v;
    return Status::OK();
}

Status StreamDataPage::SerializeToShmViewPb(const ShmView &pageView, std::string &out)
{
    ShmViewPb pb;
    pb.set_fd(pageView.fd);
    pb.set_mmap_size(pageView.mmapSz);
    pb.set_offset(pageView.off);
    pb.set_size(pageView.sz);
    bool rc = pb.SerializeToString(&out);
    CHECK_FAIL_RETURN_STATUS(rc, K_RUNTIME_ERROR, "Serialization error");
    return Status::OK();
}

Status StreamDataPage::Receive(uint64_t lastRecvCursor, uint64_t timeoutMs, std::vector<DataElement> &out,
                               const std::string &logPrefix)
{
    const auto &begCursor = pageHeader_->begCursor_;
    auto startCursor = lastRecvCursor + 1;
    if (startCursor < begCursor) {
        RETURN_STATUS_LOG_ERROR(
            K_OUT_OF_RANGE, FormatString("[P:%s] Starting read position %zu not on this page [%zu, %zu)", GetPageId(),
                                         startCursor, begCursor, begCursor + GetSlotCount()));
    }
    INJECT_POINT("StreamDataPage::Receive.sleep");
    // Unlike producers, consumers do not hold the lock to read. We rely on
    // certain fields are updated atomically.
    RETURN_IF_NOT_OK(WaitForNewElement(lastRecvCursor, timeoutMs));
    uint32_t slotCount = GetSlotCount();
    auto endCursor = begCursor + slotCount;
    // All slot offsets are relative to the slot directory
    size_t offset1 = reinterpret_cast<uint8_t *>(slotDir_) - startOfPage_;
    // Go over the cursor in [startCursor .. endCursor)
    for (auto i = startCursor; i < endCursor; ++i) {
        auto slot = i - begCursor;
        auto slotAddr = GetSlotAddr(slot + 1);
        DataElement ele;
        ele.attr_ = slotAddr->LoadFlag(isSharedPage_, __ATOMIC_ACQUIRE);
        INJECT_POINT("StreamDataPage::Receive.fake.BIG_ELEMENT", [startCursor, begCursor, &ele]() {
            LOG(INFO) << "startCursor = " << startCursor << " begCursor = " << begCursor;
            if (startCursor != begCursor) {
                ele.attr_ |= BIG_ELEMENT_BIT;
                ele.attr_ |= ELEMENT_DATA_CONSISTENT;
            }
            return Status::OK();
        });
        // If the data is still in flight, return whatever we have so far.
        if (!ele.DataIsReady()) {
            break;
        }
        auto offset = slotAddr->LoadOffset(isSharedPage_, __ATOMIC_ACQUIRE);
        // The slot directory is always one plus the number of elements.
        // Data grows backward. So its size is calculated from the offset
        // of slot before it.
        SlotOffset b4 = GetSlotAddr(slot)->LoadOffset(isSharedPage_);
        ele.size = b4 - offset;
        // Set up the pointer
        ele.ptr = startOfPage_ + offset1 + offset;
        // Pass the cursor as well
        ele.id = i;
        // get stream no.
        ele.streamNo_ = slotAddr->LoadStreamNo(isSharedPage_);
        // Tag where the elements come from
        const int logPerCount = VLOG_IS_ON(SC_INTERNAL_LOG_LEVEL) ? 1 : 1000;
        LOG_EVERY_N(INFO, logPerCount) << FormatString(
            "[%sCursor %zu] Fetch element success. slot = %zu, offset = %zu, length = %zu, local = %s, "
            "bigElement = %s, header = %s, sharedPage = %s, streamNo = %zu, pageId = %s",
            (!logPrefix.empty() ? logPrefix + " " : ""), ele.id, slot + 1, offset, ele.size,
            BoolToString(!ele.IsRemote()), BoolToString(ele.IsBigElement()), BoolToString(ele.HasHeader()),
            BoolToString(isSharedPage_), ele.streamNo_, GetPageId());
        out.emplace_back(ele);
    }
    return Status::OK();
}

Status StreamDataPage::Seal(const ShmView &nextPage, uint64_t timeoutMs,
                            std::function<Status(const ShmView &, std::shared_ptr<StreamDataPage> &)> locatePage,
                            const std::string &logPrefix)
{
    // Do not seal a page with null pointer
    CHECK_FAIL_RETURN_STATUS(nextPage.fd > 0, K_INVALID,
                             FormatString("[%s] Seal a page with invalid pointer %s", GetPageId(), nextPage.ToStr()));
    // To seal a page, we update the next pointer. All producers will then stop inserting
    // into the current page. The last cursor on the page is used as an index key to the next page.
    // Some racing condition to consider.
    // (a) Page has no room for producer A to insert a new element.
    // (b) A updates the next pointer and use the last cursor as the key.
    // (c) Producer B however can insert a small element to the page.
    // (d) The begCursor of the new page will have the same cursor value as the element inserted by B.
    // So when we *seal* a page, we must block any producer to insert any more
    // new element to the old page.
    StreamPageLock pageLock(shared_from_this());
    RETURN_IF_NOT_OK(pageLock.Lock(timeoutMs));
    // Never seal an empty page
    CHECK_FAIL_RETURN_STATUS(!Empty(), K_RUNTIME_ERROR, FormatString("[%s] Empty page", GetPageId()));
    // If we have a pointer and is in use already, report error unless it is the same given ShmView
    ShmView v = GetNextPage();
    if (v.fd <= 0) {
        // This is the index key to the next page and to the index chain.
        uint64_t lastAppendCursor = GetLastCursor();
        std::shared_ptr<StreamDataPage> page;
        RETURN_IF_NOT_OK(locatePage(nextPage, page));
        // Main reason why we need to lock the current page in order to set the starting cursor of the next page.
        // Also, atomically update two atomic fields.
        auto func = [this, &nextPage, lastAppendCursor, &page]() {
            __atomic_store_n(&page->pageHeader_->begCursor_, lastAppendCursor + 1, __ATOMIC_SEQ_CST);
            tail_->CopyFrom(nextPage);
        };
        nextPage_->LockExclusiveAndExec(func, std::numeric_limits<uint64_t>::max());
        VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s] Chain page<%s> [%zu, ) to page<%s> [%zu, %zu]", logPrefix,
                                                    page->GetPageId(), lastAppendCursor + 1, GetPageId(),
                                                    GetBegCursor(), lastAppendCursor);
        // Wake up anyone waiting on the old page
        RETURN_IF_NOT_OK(WakeUpConsumers());
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(v == nextPage, K_RUNTIME_ERROR,
                             FormatString("Page<%s> is sealed already. Next page %s", GetPageId(), v.ToStr()));
    RETURN_STATUS(K_DUPLICATED, FormatString("Page<%s> is sealed already", GetPageId()));
}

size_t StreamDataPage::GetTotalEleSize()
{
    if (GetSlotCount() == 0) {
        return 0;
    }
    auto end = GetSlotAddr(0)->LoadOffset(isSharedPage_);
    auto start = GetSlotAddr(GetSlotCount())->LoadOffset(isSharedPage_);
    if (end <= start) {
        LOG(WARNING) << FormatString("The layout of this page may be confusing, start: %zu, end: %zu", start, end);
        return 0;
    }
    return end - start;
}

Status DataElement::CheckAttribute() const
{
    // For compatibility with downlevel client, we only make sure no reserved bits are in use.
    auto highBits = attr_;
    CLEARFLAG(highBits, SLOT_VALUE_MASK);
    CLEARFLAG(highBits, ELEMENT_DATA_CONSISTENT);
    CLEARFLAG(highBits, REMOTE_ELEMENT_BIT);
    CLEARFLAG(highBits, BIG_ELEMENT_BIT);
    CLEARFLAG(highBits, HEADER_BIT);
    INJECT_POINT("StreamDataPage.CheckHighBits", [&highBits](uint32_t v) {
        highBits = v;
        return Status::OK();
    });
    // What remains should be 0. If any bit is set, this is uplevel code.
    RETURN_OK_IF_TRUE(highBits == 0);
    std::stringstream oss;
    oss << "Incompatibility with up level worker detected. Slot value = 0x" << std::hex << attr_;
    RETURN_STATUS_LOG_ERROR(K_CLIENT_WORKER_VERSION_MISMATCH, oss.str());
}

bool DataElement::DataIsReady() const
{
    return TESTFLAG(attr_, ELEMENT_DATA_CONSISTENT);
}

bool DataElement::IsRemote() const
{
    return TESTFLAG(attr_, REMOTE_ELEMENT_BIT);
}

bool DataElement::IsBigElement() const
{
    return TESTFLAG(attr_, BIG_ELEMENT_BIT);
}

bool DataElement::HasHeader() const
{
    return TESTFLAG(attr_, HEADER_BIT);
}

uint64_t DataElement::GetStreamNo() const
{
    return streamNo_;
}
}  // namespace datasystem
