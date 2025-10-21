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
 * Description: RPC options.
 */
#ifndef DATASYSTEM_COMMON_RPC_RPC_OPTIONS_H
#define DATASYSTEM_COMMON_RPC_RPC_OPTIONS_H

#include <string>

namespace datasystem {
/**
 * @brief Options for RPC setting up.
 * @note Not all options are allowed to change.
 */
class RpcOptions final {
public:
    RpcOptions();
    ~RpcOptions() = default;

    /**
     * @brief Set time out in milliseconds.
     * @param[in] timeout Timeout in milliseconds.
     */
    void SetTimeout(int timeout);

    /**
     * @brief Get time out in milliseconds.
     * @return Timeout in milliseconds.
     */
    int GetTimeout() const;

    /**
     * @brief Set the high watermark.
     * @note Increase the frontend zmq socket hwm to infinite, using `SetHWM(0)`.
     * @param[in] hwm High watermark value.
     */
    void SetHWM(int hwm);

    /**
     * @brief Get the high watermark.
     * @return High watermark value.
     */
    int GetHWM() const;

private:
    int timeout_;
    int hwm_;  // RPC high-watermark

    // ZMQ specific options
    friend class ZmqSocket;
    friend class ZmqContext;
    friend class ZmqStubConn;
    friend class ZmqStub;
    template <typename W, typename R>
    friend class MsgQue;
    /*
     * The ZMQ_LINGER option shall set the linger period for the specified socket.
     * The linger period determines how long pending messages which have yet to be sent to a peer shall
     * linger in memory after a socket is closed with zmq_close,
     * and further affects the termination of the socket's context with zmq_term.
     */
    int linger_;
    bool immediate_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_RPC_OPTIONS_H
