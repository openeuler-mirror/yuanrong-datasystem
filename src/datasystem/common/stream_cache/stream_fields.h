/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: A common class for holding stream configuration fields internally.
 */
#ifndef DATASYSTEM_COMMON_STREAM_CACHE_STREAM_FIELDS_H
#define DATASYSTEM_COMMON_STREAM_CACHE_STREAM_FIELDS_H

#include <atomic>
#include <vector>

#include "datasystem/stream/stream_config.h"

/**
 * @brief A simple class to facilitate the passing around of fields related to streams. Similar to std::pair, but has
 * the benefit of named fields and is better for future growth if new fields are needed.
 */
namespace datasystem {
class StreamFields {
public:
    /**
     * @brief Init constructor
     */
    StreamFields()
        : maxStreamSize_(0),
          pageSize_(0),
          autoCleanup_(false),
          retainForNumConsumers_(0),
          encryptStream_(false),
          reserveSize_(0)
    {
    }

    /**
     * @brief Basic constructor
     */
    StreamFields(uint64_t maxStreamSize, size_t pageSize, bool autoCleanup, uint64_t retainForNumConsumers,
                 bool encryptStream, uint64_t reserveSize, int32_t streamMode)
        : maxStreamSize_(maxStreamSize),
          pageSize_(pageSize),
          autoCleanup_(autoCleanup),
          retainForNumConsumers_(retainForNumConsumers),
          encryptStream_(encryptStream),
          reserveSize_(reserveSize),
          streamMode_(static_cast<StreamMode>(streamMode))
    {
    }

    /**
     * @brief Equality operator
     * @return true if the objects are the same
     */
    bool operator==(const StreamFields &other) const
    {
        return (this->maxStreamSize_ == other.maxStreamSize_ && this->pageSize_ == other.pageSize_
                && this->autoCleanup_ == other.autoCleanup_
                && this->retainForNumConsumers_ == other.retainForNumConsumers_
                && this->encryptStream_ == other.encryptStream_
                && this->streamMode_ == other.streamMode_);
    }

    /**
     * @brief Inequality operator
     * @return true if the objects are not the same
     */
    bool operator!=(const StreamFields &other) const
    {
        return !(*this == other);
    }

    /**
     * @brief Check if the fields are empty/un-initialized
     * @return True if the fields are empty
     */
    bool Empty() const
    {
        return (maxStreamSize_ == 0 && pageSize_ == 0);
    }

    uint64_t maxStreamSize_;
    size_t pageSize_;
    bool autoCleanup_;
    uint64_t retainForNumConsumers_;
    bool encryptStream_;
    uint64_t reserveSize_;
    StreamMode streamMode_ = StreamMode::MPMC;
};

class RetainDataState {
public:
    enum State : uint32_t { INIT = 1, RETAIN = 2, NOT_RETAIN = 3 };
    std::vector<std::string> StateNames{ "", "INIT", "RETAIN", "NOT_RETAIN" };
    void Init(uint64_t retainForNumConsumers)
    {
        if (retainForNumConsumers == 0) {
            SetRetainDataState(State::NOT_RETAIN);
        } else {
            SetRetainDataState(State::RETAIN);
        }
    }

    void SetRetainDataState(State nextState)
    {
        // Only update from INIT to RETAIN/NOT_RETAIN, or from RETAIN to NOT_RETAIN, not backward
        while (true) {
            State currentState = retainDataState_;
            if (currentState >= nextState
                || retainDataState_.compare_exchange_weak(currentState, nextState, std::memory_order_release,
                                                          std::memory_order_acquire)) {
                break;
            }
        }
    }

    State GetRetainDataState()
    {
        return retainDataState_;
    }

    std::string PrintCurrentState()
    {
        return StateNames[retainDataState_];
    }

    // Called when first producer creation fails and reverted
    void RollBackToInit()
    {
        retainDataState_ = State::INIT;
    }

    bool IsDataRetained()
    {
        return retainDataState_ == State::RETAIN;
    }

private:
    std::atomic<State> retainDataState_{ State::INIT };
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_STREAM_CACHE_STREAM_FIELDS_H
