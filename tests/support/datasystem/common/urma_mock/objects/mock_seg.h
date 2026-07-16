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

#ifndef DATASYSTEM_COMMON_URMA_MOCK_OBJECTS_MOCK_SEG_H
#define DATASYSTEM_COMMON_URMA_MOCK_OBJECTS_MOCK_SEG_H

#include <cstdint>
#include <string>

namespace datasystem {
namespace urma_mock {
class MockContext;

/**
 * @brief Mock URMA memory segment backed by either business memfd or mock mmap.
 */
class MockSeg {
public:
    /**
     * @brief Ownership of the local mmap view.
     */
    enum class MappingOwner : uint8_t {
        BORROWED,   ///< Mapping lifetime is owned by business code.
        MOCK_MMAP,  ///< Mapping lifetime is owned by the mock segment.
    };

    /**
     * @brief Create a mock segment object for a mapped memory range.
     * @param[in] id Segment id.
     * @param[in] ctx Owning context.
     * @param[in] size Segment size in bytes.
     * @param[in] key Segment key.
     * @param[in] shmName shm_open name when the mock owns the backing object.
     * @param[in] shmFd Backing fd.
     * @param[in] ptr Mapped address.
     * @param[in] isOwner Whether this object owns the backing fd/name.
     * @param[in] mappingOwner Mapping ownership mode.
     */
    MockSeg(uint64_t id, MockContext *ctx, uint64_t size, const std::string &key, const std::string &shmName, int shmFd,
            void *ptr, bool isOwner, MappingOwner mappingOwner = MappingOwner::MOCK_MMAP);
    ~MockSeg();

    /**
     * @brief Get the segment id.
     * @return Segment id.
     */
    uint64_t GetId() const;

    /**
     * @brief Get the owning context.
     * @return Owning context.
     */
    MockContext *GetContext() const;

    /**
     * @brief Get the segment size.
     * @return Segment size in bytes.
     */
    uint64_t GetSize() const;

    /**
     * @brief Get the segment key.
     * @return Segment key.
     */
    const std::string &GetKey() const;

    /**
     * @brief Get the shm_open name used by mock-owned mappings.
     * @return Shared memory name.
     */
    const std::string &GetShmName() const;

    /**
     * @brief Get the backing fd.
     * @return Backing fd, or -1 after ownership has moved.
     */
    int GetShmFd() const;

    /**
     * @brief Get the mapped address.
     * @return Mapped address.
     */
    void *GetPtr() const;

    /**
     * @brief Check whether this object owns the backing fd/name.
     * @return true if the backing object is owned by this segment.
     */
    bool OwnsMapping() const;

    /**
     * @brief Get the dup'd business memfd fd.
     * @return memfd fd, or -1 when the segment uses shm_open fallback.
     */
    int GetMemfdFd() const;

    /**
     * @brief Set the dup'd business memfd fd.
     * @param[in] fd memfd fd owned by MockSeg.
     */
    void SetMemfdFd(int fd);

    /**
     * @brief Get the file offset corresponding to ptr_ or remoteVa_.
     * @return memfd file offset.
     */
    uint64_t GetMemfdOffset() const;

    /**
     * @brief Set the file offset corresponding to ptr_ or remoteVa_.
     * @param[in] offset memfd file offset.
     */
    void SetMemfdOffset(uint64_t offset);

    /**
     * @brief Check whether the segment was imported from a peer process.
     * @return true if ptr_ is a MAP_FIXED view of the peer memfd.
     */
    bool IsRemote() const;

    /**
     * @brief Mark whether the segment is imported from a peer process.
     * @param[in] v Remote import flag.
     */
    void SetIsRemote(bool v);

    /**
     * @brief Get the wire-visible peer virtual address.
     * @return Remote virtual address used by URMA metadata.
     */
    uint64_t GetRemoteVa() const;

    /**
     * @brief Set the wire-visible peer virtual address.
     * @param[in] v Remote virtual address used by URMA metadata.
     */
    void SetRemoteVa(uint64_t v);

    /**
     * @brief Get the client id associated with an imported remote segment.
     * @return Remote client id.
     */
    const std::string &GetRemoteClientId() const;

    /**
     * @brief Set the client id associated with an imported remote segment.
     * @param[in] v Remote client id.
     */
    void SetRemoteClientId(const std::string &v);

    /**
     * @brief Get the host encoded in the remote endpoint.
     * @return Remote host string.
     */
    const std::string &GetRemoteHost() const;

    /**
     * @brief Set the host encoded in the remote endpoint.
     * @param[in] v Remote host string.
     */
    void SetRemoteHost(const std::string &v);

    /**
     * @brief Get the port encoded in the remote endpoint.
     * @return Remote port, or -1 if not set.
     */
    int GetRemotePort() const;

    /**
     * @brief Set the port encoded in the remote endpoint.
     * @param[in] v Remote port.
     */
    void SetRemotePort(int v);

private:
    uint64_t id_;
    MockContext *ctx_;
    uint64_t size_;
    std::string key_;
    std::string shmName_;
    int shmFd_ = -1;
    void *ptr_ = nullptr;
    bool isOwner_ = true;
    MappingOwner mappingOwner_ = MappingOwner::MOCK_MMAP;
    int memfdFd_ = -1;          // dup of business memfd fd (or -1)
    uint64_t memfdOffset_ = 0;  // file offset corresponding to ptr_/remoteVa_
    bool isRemote_ = false;     // imported via HELLO_ACK mmap MAP_FIXED
    uint64_t remoteVa_ = 0;     // /wire.seg_va
    std::string remoteClientId_;
    std::string remoteHost_;
    int remotePort_ = -1;
};

}  // namespace urma_mock
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_URMA_MOCK_OBJECTS_MOCK_SEG_H
