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
* Description: Zmq streaming base class.
* Including stream RPC and non-blocking unary RPC.
* In stream RPC, we have three combinations of reader and writer streaming mode for client and server, respectively.
* In non-blocking unary RPC, we have ServerUnaryWriterReader.
*/
#ifndef DATASYSTEM_COMMON_RPC_ZMQ_STREAM_BASE_H
#define DATASYSTEM_COMMON_RPC_ZMQ_STREAM_BASE_H

#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rpc/zmq/zmq_common.h"
#include "datasystem/common/rpc/zmq/zmq_msg_queue.h"
#include "datasystem/common/rpc/zmq/zmq_payload.h"
#include "datasystem/common/util/thread_local.h"

namespace datasystem {
/**
 * Base class for streaming (client and server).
 */
class StreamBase {
public:
    explicit StreamBase(bool sendPayload, bool recvPayload);
    virtual ~StreamBase() = default;

    virtual Status SendAll(ZmqSendFlags flags) = 0;
    virtual Status ReadAll(ZmqRecvFlags flags) = 0;
    virtual Status Finish() = 0;

protected:
    bool HasSendPayloadOp() const;

    bool HasRecvPayloadOp() const;

    void SetMetaAuthInfo()
    {
        meta_.set_access_key(std::move(g_ReqAk));
        meta_.set_signature(std::move(g_ReqSignature));
        meta_.set_timestamp(g_ReqTimestamp);
    }

    ZmqMsgFrames inMsg_;
    ZmqMsgFrames outMsg_;
    MetaPb meta_;

private:
    bool sendPayload_;
    bool recvPayload_;
};
}
#endif  // DATASYSTEM_COMMON_RPC_ZMQ_STREAM_BASE_H
