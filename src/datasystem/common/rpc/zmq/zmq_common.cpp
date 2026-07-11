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
 * Description: Zmq RPC module utilities.
 */
#include "datasystem/common/rpc/zmq/zmq_common.h"

#include <utility>

namespace datasystem {
Status AckRequest(ZmqMsgFrames &frames, ZmqMessage &reply)
{
    CHECK_FAIL_RETURN_STATUS(!frames.empty(), K_RUNTIME_ERROR, "Empty frames");
    /**
     * The first message is always the Status object.
     */
    ZmqMessage first = std::move(frames.front());
    frames.pop_front();

    /**
     * We can do an early exit if there is any error. Server will
     * not send any message body.
     */
    RETURN_IF_NOT_OK(ZmqMessageToStatus(first));

    if (!frames.empty()) {
        reply = std::move(frames.front());
        frames.pop_front();
    }
    return Status::OK();
}
}  // namespace datasystem
