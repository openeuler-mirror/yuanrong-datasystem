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
 * Description: Declare stream cache producer.
 */
#ifndef DATASYSTEM_STREAM_CACHE_PRODUCER_H
#define DATASYSTEM_STREAM_CACHE_PRODUCER_H

#include <memory>
#include <string>
#include <utility>

#include "datasystem/stream/element.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {
namespace stream_cache {
class ProducerImpl;
class StreamClientImpl;
}  // namespace stream_cache
}  // namespace client
}  // namespace datasystem

namespace datasystem {
class __attribute ((visibility ("default"))) Producer {
public:
    ~Producer();

    /**
     * @brief Send one element of the stream.
     * @param[in] element The element that to be written.
     * @return K_OK on success; the error code otherwise.
     *         K_UNKNOWN_ERROR: it's up to return message.
     *         K_RUNTIME_ERROR: producer not init.
     *         K_OUT_OF_MEMORY: out of memory, or unable to secure enough memory for the element.
     *         K_RUNTIME_ERROR: element copy failed, it's up to return message.
     *         K_NOT_FOUND: the id of stream is not found.
     *         K_RUNTIME_ERROR: can not find mmap file or mmap fd failed.
     *         K_INVALID: invalid parameter.
     *         K_SC_STREAM_IN_RESET_STATE: stream currently in reset state.
     *         K_SC_ALREADY_CLOSED: producer is already closed/inactive.
     *         K_SC_STREAM_IN_USE: another thread is calling API from the same producer at the same time.
     */
    Status Send(const Element &element);

    /**
     * @brief Send one element of the stream, blocking version.
     * @param[in] element The element that to be written.
     * @param[in] timeoutMs The amount of time in milliseconds to wait for the send to complete in the range of
     * [0, INT32_MAX]. A value of 0 means that it will immediately return the error reason without waiting if the send
     * cannot be completed right away. A value greater than 0 makes this a possible blocking call where it will wait for
     * the operation to complete if needed. If the wait time exceeds the value then the function will stop waiting and
     * return the error reason.
     * @return K_OK on success; the error code otherwise.
     *         K_UNKNOWN_ERROR: it's up to return message.
     *         K_RUNTIME_ERROR: producer not init.
     *         K_OUT_OF_MEMORY: out of memory, or unable to secure enough memory for the element within timeoutMs.
     *         K_RUNTIME_ERROR: element copy failed, it's up to return message.
     *         K_NOT_FOUND: the id of stream is not found.
     *         K_RUNTIME_ERROR: can not find mmap file or mmap fd failed.
     *         K_INVALID: invalid parameter.
     *         K_SC_STREAM_IN_RESET_STATE: stream currently in reset state.
     *         K_SC_ALREADY_CLOSED: producer is already closed/inactive.
     *         K_SC_STREAM_IN_USE: another thread is calling API from the same producer at the same time.
     */
    Status Send(const Element &element, int64_t timeoutMs);

    /**
     * @brief Close the producer, after close it will not allow Send new Elements, and it will trigger flush operations
     *  when the local buffer had not flushed elements. Calling Close() on an already closed producer will return K_OK.
     * @return K_OK on success; the error code otherwise.
     *         K_UNKNOWN_ERROR: it's up to return message.
     *         K_NOT_FOUND: it's up to return message.
     *         K_RUNTIME_ERROR: it's up to return message.
     *         K_SC_STREAM_IN_USE: another thread is calling API from the same producer at the same time.
     */
    Status Close();

private:
    explicit Producer(std::shared_ptr<client::stream_cache::ProducerImpl> impl);

    /**
     * @cond Friend does not show up in the documentation.
     */
    friend class client::stream_cache::StreamClientImpl;
    // @endcond

    // for make_shared to access private/protected constructor.
    friend std::shared_ptr<Producer> std::make_shared<Producer>();
    // for make_unique to access private/protected constructor.
    friend std::unique_ptr<Producer> std::make_unique<Producer>();

    std::shared_ptr<client::stream_cache::ProducerImpl> impl_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_STREAM_CACHE_PRODUCER_H
