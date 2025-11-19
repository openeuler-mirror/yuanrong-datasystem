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

#ifndef DATASYSTEM_WORKER_STREAM_CACHE_PRODUCER_H
#define DATASYSTEM_WORKER_STREAM_CACHE_PRODUCER_H

#include <string>
#include <vector>

#include "datasystem/common/stream_cache/cursor.h"

namespace datasystem {
namespace worker {
namespace stream_cache {
class Producer {
public:
    /**
     * @brief Construct Producer.
     * @param[in] producerId The producer id.
     */
    Producer(std::string producerId, std::string streamName, std::shared_ptr<Cursor> cursor);
    Producer(const Producer &producer) = delete;
    Producer &operator=(const Producer &producer) = delete;
    Producer(Producer &&producer) noexcept = delete;
    Producer &operator=(Producer &&producer) noexcept = delete;

    virtual ~Producer() = default;

    /**
     * @brief Seals the current page and clears flush count.
     * @return Status of the call.
     */
    Status CleanupProducer();

    /**
     * @brief Get producer id.
     * @return Id of producer.
     */
    [[nodiscard]] std::string GetId() const;

    /**
     * Get the element count of the cursor
     */
    uint64_t GetElementCount()
    {
        return cursor_ == nullptr ? 0 : cursor_->GetElementCount();
    }

    /**
     * Get the request count of the cursor and reset it to 0
     */
    uint64_t GetRequestCountAndReset()
    {
        return cursor_ == nullptr ? 0 : cursor_->GetRequestCountAndReset();
    }

    void SetForceClose();

    /**
     * @brief Set the element count to val
     * @param val value to set element count to
     */
    void SetElementCount(uint64_t val)
    {
        if (cursor_) {
            cursor_->SetElementCount(val);
        }
    }

    /**
     * @brief Get the element count and reset it to 0.
     * @return
     */
    uint64_t GetElementCountAndReset()
    {
        return cursor_ == nullptr ? 0 : cursor_->GetElementCountAndReset();
    }

    void SetCursor(std::shared_ptr<Cursor> &&cursor)
    {
        cursor_ = std::move(cursor);
    }

protected:
    const std::string id_;
    const std::string streamName_;
    // A work area that is shared between the corresponding client::stream_cache::ProducerImpl
    // sz is the size of this work area. It is set up in the function StreamDataObject::AddCursor.
    std::shared_ptr<Cursor> cursor_;
};
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem
#endif
