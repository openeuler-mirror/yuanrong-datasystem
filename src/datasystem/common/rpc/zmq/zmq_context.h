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
 * Description: ZMQ context declaration.
 */
#ifndef DATASYSTEM_COMMON_RPC_ZMQ_CONTEXT_H
#define DATASYSTEM_COMMON_RPC_ZMQ_CONTEXT_H

#include <atomic>
#include <deque>
#include <mutex>
#include <set>

#include <zmq.h>
#include <datasystem/utils/status.h>

#include "datasystem/common/rpc/zmq/zmq_constants.h"
#include "datasystem/common/rpc/zmq/zmq_socket_ref.h"

namespace datasystem {
enum class ZmqSocketType : int {
    PAIR = ZMQ_PAIR,
    REQ = ZMQ_REQ,
    REP = ZMQ_REP,
    DEALER = ZMQ_DEALER,
    ROUTER = ZMQ_ROUTER
};

class ZmqContext final {
public:
    explicit ZmqContext(int numIo = datasystem::ZMQ_CONTEXT_IO_THREADS, int numSo = ZMQ_CONTEXT_MAX_SOCKETS);
    ~ZmqContext();

    /**
     * @brief Create and track a ZMQ socket.
     * @param[in] type Zmq socket type.
     * @return Zmq socket reference.
     */
    void *CreateZmqSocket(ZmqSocketType type);

    /**
     * @brief Close and stop tracking a ZMQ socket.
     * @param[in] sock Zmq socket reference.
     * @return True if the socket is found in the list.
     */
    bool CloseSocket(void *ref);

    /**
     * @brief Close the ZMQ context.
     */
    void Close(bool logging = true);

    Status Init();

    auto GetHandle() const
    {
        return ctx_;
    }

private:
    std::atomic<bool> interruptFlag_;
    std::mutex mux_;
    void *ctx_;
    int numIo_;
    int numSo_;
    std::set<intptr_t> allSocks_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_ZMQ_CONTEXT_H
