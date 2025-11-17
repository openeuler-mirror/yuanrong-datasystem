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

#ifndef DATASYSTEM_STREAM_CACHE_STREAM_DATA_PAGE_H
#define DATASYSTEM_STREAM_CACHE_STREAM_DATA_PAGE_H

#include <utility>

#include "datasystem/client/mmap_table.h"
#include "datasystem/common/object_cache/lock.h"
#include "datasystem/common/shared_memory/shm_unit.h"
#include "datasystem/common/stream_cache/cursor.h"
#include "datasystem/common/stream_cache/stream_meta_shm.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/bitmask_enum.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/stream/element.h"
#include "datasystem/utils/optional.h"
#include "datasystem/protos/stream_posix.pb.h"

namespace datasystem {
// For the lock on StreamDataPage, we only need exclusive lock functionality.
// Producers always lock exclusively. Consumer never locks the page.
// We only need the lower order bit for locked or unlocked. The rest of the bits
// are to store the lockId. So totally 4 bytes are sufficient.
class PageLock {
public:
    constexpr static int NO_LOCK_NUM = 0;
    constexpr static int WRITE_LOCK_NUM = 1;
    constexpr static int SHIFT = 1;
    explicit PageLock(uint32_t *lockArea, uint32_t *waitArea, uint32_t lockId = 0);
    ~PageLock() = default;

    /**
     * Lock exclusively
     * @param timeoutMs in milliseconds
     * @return OK if successful
     */
    Status Lock(uint64_t timeoutMs);

    /**
     * Unlock a previously held lock
     * @param tid
     */
    void Unlock();

    /**
     * @brief General form of calling futex wait
     * @param lockArea
     * @param waitCount
     * @param timeoutMs
     * @return
     */
    static Status FutexWait(uint32_t *lockArea, uint32_t *waitCount, uint32_t val, uint64_t timeoutMs);

    /**
     * @brief General form of calling futex wake
     * @param lockArea
     * @param waitCount
     * @param numToWakeUp
     * @return
     */
    static Status FutexWake(uint32_t *lockArea, uint32_t *waitCount, int numToWakeUp = INT_MAX);

    /**
     * Unlock by lockID. Used by worker for client crash recovery.
     * @param lockId
     * @return true if the lock is held by the given lockId
     */
    bool TryUnlockByLockId(uint32_t lockId);

private:
    uint32_t *lockFlag_;
    uint32_t *waitCount_;
    uint32_t lockId_;
};

// A slot is 4 bytes. The first 8 high bits are used for special purposes.
// That leaves us 24 bits for offset. Maximum offset is then 16'777'215ul
constexpr static uint32_t REMOTE_ELEMENT_BIT = static_cast<uint32_t>(0x80000000);
constexpr static uint32_t ELEMENT_DATA_CONSISTENT = static_cast<uint32_t>(0x40000000);
constexpr static uint32_t BIG_ELEMENT_BIT = static_cast<uint32_t>(0x20000000);
constexpr static uint32_t HEADER_BIT = static_cast<uint32_t>(0x10000000);
constexpr static uint32_t FUTURE_1_BIT = static_cast<uint32_t>(0x08000000);
constexpr static uint32_t FUTURE_2_BIT = static_cast<uint32_t>(0x04000000);
constexpr static uint32_t FUTURE_3_BIT = static_cast<uint32_t>(0x02000000);
constexpr static uint32_t FUTURE_4_BIT = static_cast<uint32_t>(0x01000000);
constexpr static uint32_t SLOT_VALUE_MASK = static_cast<uint32_t>(0x00FFFFFF);
// Is shared page if the first bit of slot0 is set
constexpr static uint32_t PAGE_SHARED_BIT = static_cast<uint32_t>(0x80000000);

// This class extends the Element class with the additional attribute byte from above
class DataElement : public Element {
public:
    bool DataIsReady() const;
    bool IsRemote() const;
    bool IsBigElement() const;
    bool HasHeader() const;
    Status CheckAttribute() const;
    uint64_t GetStreamNo() const;
    uint32_t attr_{ 0 };
    uint64_t streamNo_{ 0 };
};

class ElementHeader {
public:
    typedef uint8_t *Ptr;
    typedef uint32_t Size;
    typedef uint8_t Version;
    Ptr headerPtr_{ nullptr };
    Size headerSize_{ 0 };
    Version headerVersion_{ 0 };
    void Set(Ptr ptr, Size size, Version version);
};

constexpr static ElementHeader::Version DATA_VERIFICATION_HEADER = static_cast<ElementHeader::Version>(1);
class HeaderAndData : public Element, public ElementHeader {
public:
    typedef uint32_t Size;
    typedef uint8_t *Ptr;
    HeaderAndData(const Element &element, const ElementHeader &header, uint64_t streamNo);
    HeaderAndData(const Ptr ptr, const Size size, uint64_t streamNo);
    Size TotalSize() const;
    Status MemoryCopyTo(Ptr dest) const;
    uint64_t streamNo{ 0 };
};

// This struct contain fields to be copied in front of element data if needed.
struct DataVerificationHeader {
    typedef uint64_t SeqNo;
    typedef uint64_t SenderProducerNo;
    typedef uint32_t Size;
    typedef uint32_t Address;
    typedef uint16_t Port;

    union {
        struct {
            SeqNo seqNo;
            SenderProducerNo senderProducerNo;
            Address address;
            Port port;
        } hdr;
        uint8_t bytes[sizeof(SeqNo) + sizeof(SenderProducerNo) + sizeof(Address) + sizeof(Port)];
    };

    DataVerificationHeader(SeqNo seqNo = std::numeric_limits<SeqNo>::max(),
                           SenderProducerNo localProducerNo = std::numeric_limits<SenderProducerNo>::max(),
                           Address address = std::numeric_limits<Address>::max(),
                           Port port = std::numeric_limits<Port>::max());
    DataVerificationHeader(const ElementHeader &ele);
    SeqNo GetSeqNo() const;
    SenderProducerNo GetSenderProducerNo() const;
    Address GetAddress() const;
    Port GetPort() const;
    Size HeaderSize() const;
    void Set(SeqNo seqNo, SenderProducerNo producerNo, Address address, Port port);
    static Status ExtractHeader(DataElement &element, ElementHeader &header);
};

class StreamPageBase {
public:
    explicit StreamPageBase(std::shared_ptr<ShmUnitInfo>);
    explicit StreamPageBase(std::shared_ptr<ShmUnitInfo> shmInfo, std::shared_ptr<client::MmapTableEntry> mmapEntry);
    virtual ~StreamPageBase() = default;

    /**
     * Initialization
     * @param isClient
     */
    void Init(bool isClient);

    /**
     * @brief return the start address
     * @return start address
     */
    void *GetPointer() const
    {
        return startOfPage_;
    }

    /**
     * @brief return the page size.
     * @return page size
     */
    auto PageSize() const
    {
        return pageUnit_->GetSize();
    }

    static ShmKey CreatePageId(const std::shared_ptr<ShmUnitInfo> &pageUnit);

    /**
     * @brief Return the page id
     * @return
     */
    ShmKey GetPageId() const
    {
        return pageUnit_->GetId();
    }

    /**
     * @brief Return ShmView of this page
     */
    ShmView GetShmView() const;

    /**
     * @brief Return the ShmUnitInfo for this page
     * @return
     */
    std::shared_ptr<ShmUnitInfo> GetShmUnitInfo() const;

protected:
    std::shared_ptr<ShmUnitInfo> pageUnit_;
    std::shared_ptr<client::MmapTableEntry> mmapEntry_;
    uint8_t *startOfPage_{ nullptr };

private:
};

// A blank stream page (without any header) that contains only raw data
class StreamLobPage : public StreamPageBase, public std::enable_shared_from_this<StreamLobPage> {
public:
    explicit StreamLobPage(std::shared_ptr<ShmUnitInfo> shmInfo, bool isClient);
    explicit StreamLobPage(std::shared_ptr<ShmUnitInfo> shmInfo, bool isClient,
                           std::shared_ptr<client::MmapTableEntry> mmapEntry);
    ~StreamLobPage() override = default;
    Status Init();
    Status Insert(const HeaderAndData &element);

private:
    const bool isClient_;
};

enum class InsertFlags : uint32_t {
    NONE = 0,
    REMOTE_ELEMENT = 1u,
    DELAY_WAKE = 1u << 1,
    SKIP_LOCK = 1u << 2,
    BIG_ELEMENT = 1u << 3,
    HEADER = 1u << 4,
    INSERT_SUCCESS = 1u << 5
};
ENABLE_BITMASK_ENUM_OPS(InsertFlags);

namespace worker {
namespace stream_cache {
class ExclusivePageQueue;
}
}  // namespace worker

namespace client {
namespace stream_cache {
class ProducerImpl;
}
}  // namespace client

using SlotFlagOffset = uint32_t;
using SlotFlag = uint32_t;
using SlotOffset = uint32_t;

union SlotType {
    SlotFlagOffset flagWithOffset;  // 4 bytes for exclusive page.
    struct FlagOffsetStreamNo {     // for shared page.
        SlotFlag flag;              // 4 bytes
        SlotOffset offset;          // 4 bytes
        uint64_t streamNo;          // 8 bytes
    } value;
    uint64_t flagAndOffset;

    void StoreAll(bool enableSharedPage, SlotFlag flag, SlotOffset offset, uint64_t streamNo,
                  int memModel = __ATOMIC_RELEASE)
    {
        if (enableSharedPage) {
            SlotType slot;
            slot.value.flag = flag;
            slot.value.offset = offset;
            slot.value.streamNo = streamNo;
            __atomic_store_n(&flagAndOffset, slot.flagAndOffset, memModel);
            __atomic_store_n(&value.streamNo, slot.value.streamNo, memModel);
        } else {
            SlotFlagOffset flagOffset = flag & ~SLOT_VALUE_MASK;
            flagOffset |= offset & SLOT_VALUE_MASK;
            __atomic_store_n(&flagWithOffset, flagOffset, memModel);
        }
    }

    SlotFlag LoadFlag(bool enableSharedPage, int memModel = __ATOMIC_SEQ_CST)
    {
        if (enableSharedPage) {
            return __atomic_load_n(&value.flag, memModel);
        }
        return __atomic_load_n(&flagWithOffset, memModel) & ~SLOT_VALUE_MASK;
    }

    SlotOffset LoadOffset(bool enableSharedPage, int memModel = __ATOMIC_SEQ_CST)
    {
        if (enableSharedPage) {
            return __atomic_load_n(&value.offset, memModel);
        }
        return __atomic_load_n(&flagWithOffset, memModel) & SLOT_VALUE_MASK;
    }

    uint64_t LoadStreamNo(bool enableSharedPage, int memModel = __ATOMIC_SEQ_CST)
    {
        if (enableSharedPage) {
            return __atomic_load_n(&value.streamNo, memModel);
        }
        return 0;
    }

    void SetFlagBit(SlotFlag addBit)
    {
        SlotFlag slotFlag = __atomic_load_n(&flagWithOffset, __ATOMIC_ACQUIRE);
        SETFLAG(slotFlag, addBit);
        __atomic_store_n(&flagWithOffset, slotFlag, __ATOMIC_RELEASE);
    }

    void ClearFlagBit(SlotFlag delBit)
    {
        SlotFlag slotFlag = __atomic_load_n(&flagWithOffset, __ATOMIC_ACQUIRE);
        CLEARFLAG(slotFlag, delBit);
        __atomic_store_n(&flagWithOffset, slotFlag, __ATOMIC_RELEASE);
    }
};
// Begin memory layout of the page in order. Do NOT add anything that
// breaks alignment. All fields must be packed without any gap.
struct StreamPageHeader {
    uint64_t begCursor_;       // 8 bytes
    uint32_t lockArea_;        // 4 bytes
    uint32_t lockWait_;        // 4 bytes
    uint32_t refCount_;        // 4 bytes
    uint32_t totalFreeSpace_;  // 4 bytes
    uint32_t slotCount_;       // 4 bytes
    uint32_t slotWait_;        // 4 bytes
    SlotType slot0_;           // start of variable size array.
};

class StreamDataPage : public StreamPageBase, public std::enable_shared_from_this<StreamDataPage> {
public:
    explicit StreamDataPage(std::shared_ptr<ShmUnitInfo> shmInfo, uint32_t lockId, bool isClient,
                            bool isSharedPage = false, std::shared_ptr<client::MmapTableEntry> mmapEntry = nullptr);
    ~StreamDataPage() override = default;

    /**
     * Initialization
     */
    Status Init();

    /**
     * @brief Init a page to empty. Call by worker only
     * @return Status object
     */
    Status InitEmptyPage();

    /**
     * @brief Reset a valid page back to empty
     * @return
     */
    Status ResetToEmpty();

    /**
     * @brief Insert one single element
     * @param element header + data
     * @param timeoutMs timeout in millisecond
     * @param logPrefix
     * @return Status object
     */
    Status Insert(const HeaderAndData &element, uint64_t timeoutMs, InsertFlags &flags,
                  const std::string &logPrefix = "");

    /**
     * @brief Wake up consumer waiting for new element
     */
    Status WakeUpConsumers();

    /**
     * @brief Receive a vector of elements in the form of Element
     * @param lastRecvCursor
     * @param timeoutMs timeout in millisecond
     * @param[out] out
     * @param logPrefix
     * @return Status object
     */
    Status Receive(uint64_t lastRecvCursor, uint64_t timeoutMs, std::vector<DataElement> &out,
                   const std::string &logPrefix = "");

    /**
     * @brief Return ShmView of the next page
     */
    ShmView GetNextPage() const;

    /**
     * Set the ShmView of the next page
     * @param shm
     */
    void SetNextPage(const ShmView &shm);

    /**
     * @brief Atomic check if there is a next page.
     * @return T/F
     */
    bool HasNextPage() const;

    /**
     * @brief Get the cursor of the beginning slot
     * @return
     */
    uint64_t GetBegCursor() const;

    /**
     * @brief Get the last cursor of the page
     */
    uint64_t GetLastCursor() const;

    /**
     * @brief Atomically get the number of elements on the page
     * @return number of elements on the page
     */
    uint32_t GetSlotCount() const;

    /**
     * @brief Check if the page is empty or not
     */
    bool Empty() const;

    /**
     * @brief Atomically increase the reference count
     */
    Status RefPage(const std::string &logPrefix = "");

    /**
     * @brief Atomically decrease the reference count
     */
    Status ReleasePage(const std::string &logPrefix = "");

    /**
     * @brief Atomically get the reference count
     */
    uint32_t GetRefCount() const;

    /**
     * Lock a page exclusively for producer.
     * @param timeoutMs in millisecond
     * @return Status
     * @note Consumer is not affected
     */
    Status Lock(uint64_t timeoutMs);

    /**
     * Unlock a page.
     */
    void Unlock();

    /**
     * Unlock and repair a page held by a crashed client with a given lockId
     * @param lockId
     */
    void TryUnlockByLockId(uint32_t lockId);

    /**
     * @brief Batch insert.
     * @param[in] buf contiguous payload of the elements in reverse order
     * @param[in] sz vector of the size of the elements
     * @param[in] headerBits Is data contain header for each element.
     * @param[in] streamMetaShm The pointer to streamMetaShm
     * @return Status
     */
    Status BatchInsert(void *buf, std::vector<size_t> &sz, uint64_t timeoutMs, std::pair<size_t, size_t> &res,
                       InsertFlags flags, const std::vector<bool> &headerBits, StreamMetaShm *streamMetaShm);

    /**
     * Size of the overhead of a page.
     * @return
     */
    static size_t PageOverhead(bool enableSharedPage = false);

    /**
     * Size of the payload of a page, include slot value/streamNo/data.
     * @return size_t
     */
    size_t PagePayloadSize();

    static Status ParseShmViewPb(const void *ptr, size_t sz, ShmView &out);

    static Status SerializeToShmViewPb(const ShmView &pageView, std::string &out);

    StreamPageHeader *GetPageHeader()
    {
        return pageHeader_;
    }

    std::shared_ptr<SharedMemViewImpl> &GetSharedMemViewForNextPage()
    {
        return nextPage_;
    }

    Status Seal(const ShmView &nextPage, uint64_t timeoutMs,
                std::function<Status(const ShmView &, std::shared_ptr<StreamDataPage> &)> locatePage,
                const std::string &logPrefix);

    Status ExtractBigElementsUpTo(uint64_t ackCursor, std::vector<std::pair<uint64_t, ShmView>> &bigId, bool deCouple);

    bool IsSharedPage() const
    {
        return isSharedPage_;
    }

    static size_t GetMetaSize(bool isSharedPage);

    size_t GetFreeSpaceSize();

    size_t GetTotalEleSize();

private:
    friend class worker::stream_cache::ExclusivePageQueue;
    friend class client::stream_cache::ProducerImpl;
    const uint32_t lockId_;
    const bool isClient_;
    int64_t maxElementSize_;
    std::shared_ptr<PageLock> pageLock_{ nullptr };
    StreamPageHeader *pageHeader_{ nullptr };
    // End of page header. After the header, followed by stream page size of number of free bytes.
    // After the free space, we have the pointer to the next StreamDataPage (if any).
    SlotFlagOffset *slotDir_{ nullptr };
    SharedMemView *tail_{ nullptr };  // Tail pointer to next page
    std::shared_ptr<SharedMemViewImpl> nextPage_;
    bool isSharedPage_{ false };
    void UpdateSlotConsistentBit(uint32_t slot);
    Status WaitForNewElement(uint64_t lastRecvCursor, uint64_t timeoutMs);
    SlotOffset GetSlotOffset(size_t index);
    SlotFlag GetSlotFlag(size_t index);
    void SetPageHasBigElement();
    void UnsetPageHasBigElement();
    bool PageHasBigElement();
    SlotType *GetSlotAddr(size_t index);
};

// A helper class to lock a page and will ensure the lock will be released on exit
class StreamPageLock {
public:
    explicit StreamPageLock(std::shared_ptr<StreamDataPage> page);
    ~StreamPageLock();

    /**
     * Lock a page exclusively for producer.
     * @param timeoutMs in millisecond
     * @return Status
     * @note Consumer is not affected
     */
    Status Lock(uint64_t timeoutMs);

    /**
     * Unlock a page.
     */
    void Unlock();

private:
    bool pageLocked_ = false;
    std::shared_ptr<StreamDataPage> page_;
};
}  // namespace datasystem

#endif  // DATASYSTEM_STREAM_CACHE_STREAM_DATA_PAGE_H
