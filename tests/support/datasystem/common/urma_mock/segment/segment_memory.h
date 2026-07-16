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

#ifndef DATASYSTEM_COMMON_URMA_MOCK_SEGMENT_SEGMENT_MEMORY_H
#define DATASYSTEM_COMMON_URMA_MOCK_SEGMENT_SEGMENT_MEMORY_H

#include <cstdint>
#include <string>
#include <utility>

#include "datasystem/common/urma_mock/objects/mock_seg.h"
#include "datasystem/common/urma_mock/registry/import_endpoint_registry.h"

namespace datasystem {
namespace urma_mock {

/**
 * @brief RAII owner for fd and mmap state prepared before MockSeg is created.
 */
class SegmentBacking {
public:
    SegmentBacking() = default;
    ~SegmentBacking();
    SegmentBacking(const SegmentBacking &) = delete;
    SegmentBacking &operator=(const SegmentBacking &) = delete;
    SegmentBacking(SegmentBacking &&other) noexcept;
    SegmentBacking &operator=(SegmentBacking &&other) noexcept;

    /**
     * @brief Release resources still owned by this backing object.
     */
    void Reset();

    /**
     * @brief Mark resources as moved into MockSeg.
     */
    void ReleaseOwnership();

    /**
     * @brief Get the mapped address.
     * @return Mapped address.
     */
    void *GetPtr() const;

    /**
     * @brief Get the mapping length.
     * @return Mapping length in bytes.
     */
    uint64_t GetSize() const;

    /**
     * @brief Get the backing shared memory name.
     * @return Shared memory name or memfd label.
     */
    const std::string &GetShmName() const;

    /**
     * @brief Get the fd passed to MockSeg.
     * @return Backing fd, or -1 when not fd-backed.
     */
    int GetShmFd() const;

    /**
     * @brief Check whether POSIX shm name ownership moves into MockSeg.
     * @return true if MockSeg should unlink shmName.
     */
    bool IsOwner() const;

    /**
     * @brief Get the mmap ownership mode.
     * @return Mapping ownership mode.
     */
    MockSeg::MappingOwner GetMappingOwner() const;

    /**
     * @brief Get the wire-visible virtual address.
     * @return Wire-visible virtual address.
     */
    uint64_t GetWireVa() const;

    /**
     * @brief Get the memfd offset for the mapping.
     * @return memfd offset.
     */
    uint64_t GetMemfdOffset() const;

    /**
     * @brief Get the memfd fd moved into MockSeg.
     * @return memfd fd, or -1 when absent.
     */
    int GetMemfdFd() const;

private:
    friend class SegmentMemoryManager;

    /**
     * @brief Set prepared mapping state.
     * @param[in] shmName Backing shared memory name or memfd label.
     * @param[in] shmFd Backing fd.
     * @param[in] ptr Mapped address.
     * @param[in] size Mapping length.
     * @param[in] isOwner Whether POSIX shm name ownership moves to MockSeg.
     * @param[in] mappingOwner Mapping ownership mode.
     * @param[in] wireVa Wire-visible virtual address.
     */
    void SetMapping(const std::string &shmName, int shmFd, void *ptr, uint64_t size, bool isOwner,
                    MockSeg::MappingOwner mappingOwner, uint64_t wireVa);

    /**
     * @brief Set memfd metadata for the prepared mapping.
     * @param[in] fd memfd fd moved to MockSeg.
     * @param[in] offset File offset for ptr.
     */
    void SetMemfd(int fd, uint64_t offset);

    /**
     * @brief Move resource ownership from another backing object.
     * @param[in,out] other Source backing object.
     */
    void MoveFrom(SegmentBacking &other);

    std::string shmName_;
    int shmFd_ = -1;
    void *ptr_ = nullptr;
    uint64_t size_ = 0;
    bool isOwner_ = false;
    MockSeg::MappingOwner mappingOwner_ = MockSeg::MappingOwner::MOCK_MMAP;
    uint64_t wireVa_ = 0;
    uint64_t memfdOffset_ = 0;
    int memfdFd_ = -1;
    bool ownsResource_ = false;
};

/**
 * @brief Result category produced when resolving an imported segment.
 */
enum class SegmentImportKind : uint8_t {
    BACKING,      ///< Import resolved to a local mmap backing.
    PLACEHOLDER,  ///< Import found a local-only remote endpoint placeholder.
    FAILED,       ///< Import could not be resolved.
};

/**
 * @brief Segment import request parameters from the URMA ABI layer.
 */
struct SegmentImportRequest {
    uint64_t size = 0;      ///< Requested import size.
    std::string token;      ///< Segment token string carried by URMA metadata.
    uint64_t remoteVa = 0;  ///< Wire-visible remote virtual address.
};

/**
 * @brief Segment import resolution result consumed by the backend facade.
 */
class SegmentImportResult {
public:
    SegmentImportResult() = default;
    ~SegmentImportResult() = default;
    SegmentImportResult(const SegmentImportResult &) = delete;
    SegmentImportResult &operator=(const SegmentImportResult &) = delete;
    SegmentImportResult(SegmentImportResult &&) noexcept = default;
    SegmentImportResult &operator=(SegmentImportResult &&) noexcept = default;

    /**
     * @brief Create a failed import result.
     * @return Failed import result.
     */
    static SegmentImportResult Failed();

    /**
     * @brief Create a result backed by an mmap view.
     * @param[in] backing Prepared segment backing.
     * @param[in] ep Matched endpoint when imported through a registered endpoint.
     * @param[in] hasEndpoint Whether ep is valid.
     * @return Backing import result.
     */
    static SegmentImportResult Backing(SegmentBacking &&backing, const ImportEndpoint &ep, bool hasEndpoint);

    /**
     * @brief Create a local-only remote placeholder result.
     * @param[in] ep Matched endpoint.
     * @return Placeholder import result.
     */
    static SegmentImportResult Placeholder(const ImportEndpoint &ep);

    /**
     * @brief Get result category.
     * @return Import result category.
     */
    SegmentImportKind GetKind() const;

    /**
     * @brief Get prepared segment backing.
     * @return Mutable backing object.
     */
    SegmentBacking &GetBacking();

    /**
     * @brief Get matched import endpoint.
     * @return Import endpoint metadata.
     */
    const ImportEndpoint &GetEndpoint() const;

    /**
     * @brief Check whether endpoint metadata is valid.
     * @return true if this result has endpoint metadata.
     */
    bool HasEndpoint() const;

private:
    SegmentImportKind kind_ = SegmentImportKind::FAILED;
    SegmentBacking backing_;
    ImportEndpoint endpoint_;
    bool hasEndpoint_ = false;
};

/**
 * @brief Segment memory strategy used by register_seg and import_seg.
 */
class SegmentMemoryManager {
public:
    /**
     * @brief Get the process-wide segment memory manager.
     * @return Segment memory manager instance.
     */
    static const SegmentMemoryManager &Instance();

    /**
     * @brief Create the local segment backing for register_seg.
     * @param[in] size Segment size in bytes.
     * @param[in] key Segment key.
     * @param[in] businessVa Business virtual address passed by URMA ABI.
     * @param[out] backing Prepared backing object.
     * @return true on success.
     */
    bool CreateLocal(uint64_t size, const std::string &key, uint64_t businessVa, SegmentBacking &backing) const;

private:
    friend class SegmentImporter;

    SegmentMemoryManager() = default;
    ~SegmentMemoryManager() = default;

    /**
     * @brief Import a peer segment through a registered endpoint.
     * @param[in] ep Endpoint registered during import-segment negotiation.
     * @param[in] token Segment token.
     * @param[in] size Requested segment size.
     * @param[out] backing Imported mmap view.
     * @return true on success.
     */
    bool ImportViaEndpoint(const ImportEndpoint &ep, uint64_t token, uint64_t size, SegmentBacking &backing) const;

    /**
     * @brief Import an in-process peer segment by duplicating its backing fd.
     * @param[in] localPeerSeg Existing local peer segment.
     * @param[in] size Requested import size.
     * @param[out] backing Imported mmap view.
     * @return true on success.
     */
    bool ImportLocal(const MockSeg &localPeerSeg, uint64_t size, SegmentBacking &backing) const;

    /**
     * @brief Import a peer segment through the POSIX shm fallback.
     * @param[in] token Import token.
     * @param[in] size Requested import size.
     * @param[in] remoteVa Wire-visible remote virtual address.
     * @param[out] backing Imported mmap view.
     * @return true on success.
     */
    bool ImportPosix(const std::string &token, uint64_t size, uint64_t remoteVa, SegmentBacking &backing) const;

    /**
     * @brief Borrow the business memfd already mapped at businessVa.
     * @param[in] size Segment size in bytes.
     * @param[in] businessVa Business virtual address passed by URMA ABI.
     * @param[out] backing Prepared borrowed backing object.
     * @return true on success.
     */
    bool AdoptBusinessMemfd(uint64_t size, uint64_t businessVa, SegmentBacking &backing) const;

    /**
     * @brief Create a memfd view for an already mapped business range.
     * @param[in] size Segment size in bytes.
     * @param[in] businessVa Business virtual address passed by URMA ABI.
     * @param[out] backing Prepared borrowed backing object.
     * @return true on success.
     */
    bool BorrowMappedRange(uint64_t size, uint64_t businessVa, SegmentBacking &backing) const;

    /**
     * @brief Create a mock-owned memfd mapping.
     * @param[in] size Segment size in bytes.
     * @param[in] businessVa Business virtual address used only for logging.
     * @param[out] backing Prepared mock memfd backing object.
     * @return true on success.
     */
    bool CreateMockMemfd(uint64_t size, uint64_t businessVa, SegmentBacking &backing) const;

    /**
     * @brief Create the POSIX shm fallback mapping.
     * @param[in] size Segment size in bytes.
     * @param[in] key Segment key.
     * @param[in] businessVa Business virtual address used to derive shm name.
     * @param[out] backing Prepared POSIX shm backing object.
     * @return true on success.
     */
    bool CreatePosixShm(uint64_t size, const std::string &key, uint64_t businessVa, SegmentBacking &backing) const;
};

/**
 * @brief Resolve import_seg into the first usable mock segment backing.
 */
class SegmentImporter {
public:
    /**
     * @brief Get the process-wide segment importer.
     * @return Segment importer instance.
     */
    static const SegmentImporter &Instance();

    /**
     * @brief Resolve an import_seg request through endpoint, local alias, then POSIX shm fallback.
     * @param[in] request Import request parameters.
     * @return Resolved import result.
     */
    SegmentImportResult Resolve(const SegmentImportRequest &request) const;

private:
    SegmentImporter() = default;
    ~SegmentImporter() = default;
};

}  // namespace urma_mock
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_URMA_MOCK_SEGMENT_SEGMENT_MEMORY_H
