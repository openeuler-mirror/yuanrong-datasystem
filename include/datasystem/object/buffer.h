/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: This file is used to read and write data and publish data to the server.
 */

#ifndef DATASYSTEM_OBJECT_BUFFER_H
#define DATASYSTEM_OBJECT_BUFFER_H

#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <unordered_set>
#include <vector>

#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
class ObjectClientImpl;
class Lock;
}  // namespace object_cache
namespace kv_cache {
class KVClient; // namespace kv_cache
}
struct ObjectBufferInfo;
class ObjectClient;
struct RemoteH2DHostInfo;
}  // namespace datasystem

namespace datasystem {

class Buffer : public std::enable_shared_from_this<Buffer> {
public:
    Buffer() = default;

    Buffer(Buffer &&other) noexcept;

    Buffer &operator=(Buffer &&other) noexcept;

    virtual ~Buffer();

    Buffer(const Buffer &other) = delete;
    Buffer &operator=(const Buffer &other) = delete;

    /// \brief Write data to buffer.
    ///
    /// \param[in] data The address of object data.
    /// \param[in] length The length of object data.
    ///
    /// \return Status of the result.
    Status MemoryCopy(const void *data, uint64_t length);

    /// \brief Get the data size of the buffer.
    ///
    /// \return The data size of the buffer.
    int64_t GetSize() const;

    /// \brief Publish mutable data to the server.
    ///
    /// \param[in] nestedKeys Object key of the nested object.
    ///
    /// \return Status of the result.
    Status Publish(const std::unordered_set<std::string> &nestedKeys = {});

    /// \brief Publish immutable data to the server.
    ///
    /// \param[in] nestedKeys Object key of the nested object.
    ///
    /// \return Status of the result.
    Status Seal(const std::unordered_set<std::string> &nestedKeys = {});

    /// \brief A write lock is executed on the memory to protect the memory from concurrent reads and writes.
    ///
    /// \param[in] timeoutSec Try-lock timeout, seconds.
    ///
    /// \return Status of the result.
    Status WLatch(uint64_t timeoutSec = 60 /* default is 60s */);

    /// \brief A Read lock is executed on the memory to protect the memory from concurrent writes (allow concurrent
    ///  reads).
    ///
    /// \param[in] timeoutSec Try-lock timeout, default value is 60 seconds.
    ///
    /// \return Status of the result.
    Status RLatch(uint64_t timeoutSec = 60 /* default is 60s */);

    /// \brief Unlock the read latch on memory.
    ///
    /// \return Status of the result.
    Status UnRLatch();

    /// \brief Unlock the write latch on memory.
    ///
    /// \return Status of the result.
    Status UnWLatch();

    /// \brief Gets a mutable data pointer.
    ///
    /// \return A void * to the data.
    void *MutableData();

    /// \brief Gets a immutable data pointer.
    ///
    /// \return A const void * to the data.
    const void *ImmutableData();

    /// \brief Invalidates data on the current host.
    ///
    /// \return Status of the result.
    Status InvalidateBuffer();

    /// \brief Getter function for remote host info.
    ///
    /// \return The remote host info for RemoteH2D purpose.
    RemoteH2DHostInfo *GetRemoteHostInfo();

private:
    friend class datasystem::object_cache::ObjectClientImpl;
    friend class KVClient;
    friend class WorkerOCServiceImpl;

    Buffer(std::shared_ptr<ObjectBufferInfo> bufferInfo,
           const std::shared_ptr<object_cache::ObjectClientImpl> &clientImpl);

    /// \brief The only purpose of having this function is to encapsulate the above private Buffer constructor,
    ///         to make it work with std::make_shared<Buffer>. Directly use std::make_shared with a private
    ///         constructor will cause compilation errors.
    ///
    /// \param[in] bufferInfo The buffer information for creating buffer.
    /// \param[in] clientImpl Object client impl.
    /// \param[out] buffer The buffer instance.
    ///
    /// \return New created buffer.
    static Status CreateBuffer(std::shared_ptr<ObjectBufferInfo> bufferInfo,
                               std::shared_ptr<object_cache::ObjectClientImpl> clientImpl,
                               std::shared_ptr<Buffer> &buffer);
    /// \brief Initialize the buffer information.
    ///
    /// \return K_OK if the initialization succeeds, K_RUNTIME_ERROR otherwise.
    Status Init();

    /// \brief Check if buffer is deprecated. If worker is down and buffer is shm buffer, this check would
    ///         return error, means the buffer is useless, user should destruct it as fast as possible.
    ///
    /// \return K_OK if worker is normal, error code otherwise.
    Status CheckDeprecated();

    /// \brief Reset the buffer, ignore the owned resource.
    void Reset();

    /// \brief Release the buffer owned resources.
    /// \param[in] clientPtr The raw pointer of client. The caller needs to guarantee its lifecycle.
    void Release(object_cache::ObjectClientImpl *clientPtr = nullptr);

    uint8_t *GetVisiblePointer();

    /// \brief Set visibility for shm buffer.
    ///
    /// \param[in] visible visible buffer or not.
    void SetVisibility(bool visible);

    /// \brief Check if shm buffer is visible.
    ///
    /// \return Status of the call.
    Status CheckVisible();

    std::shared_ptr<ObjectBufferInfo> bufferInfo_;
    std::weak_ptr<object_cache::ObjectClientImpl> clientImpl_;
    std::shared_ptr<object_cache::Lock> latch_;

    bool isShm_ = false;  // indicate whether the buffer will be created via a shm way
    bool isReleased_ = false;

    std::string clientId_;  // Used to determine whether the client has restarted.
};
}  // namespace datasystem
#endif  // DATASYSTEM_OBJECT_CACHE_BUFFER_H
