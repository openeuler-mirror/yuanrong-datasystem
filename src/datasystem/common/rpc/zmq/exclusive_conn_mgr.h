/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Manager for exclusive connections in thread local memory
 */
#ifndef DATASYSTEM_COMMON_RPC_ZMQ_EXCLUSIVE_CONN_MGR_H
#define DATASYSTEM_COMMON_RPC_ZMQ_EXCLUSIVE_CONN_MGR_H

#include <thread>
#include <unistd.h>
#include <sys/types.h>
#include "datasystem/common/log/log.h"
#include "datasystem/common/rpc/unix_sock_fd.h"
#include "datasystem/common/rpc/zmq/zmq_msg_decoder.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {

/**
 * @brief The common use-case is that a single thread only uses one client and so it only needs to manage one
 * connection in the thread local memory.
 * But, it is not impossible that a given user thread might have other clients created. Since this class exists in
 * thread local memory, we need an index or key to find the connection given a client identifier (exclusiveId).
 */
class ExclusiveConnMgr {
public:
    /**
     * @brief Constructor. Creates connection table with default number of allocated slots.
     */
    ExclusiveConnMgr();

    /**
     * @brief Destructor
     */
    ~ExclusiveConnMgr() = default;

    ExclusiveConnMgr(const ExclusiveConnMgr &) = delete;
    ExclusiveConnMgr &operator=(const ExclusiveConnMgr &) = delete;
    
    /**
     * @brief Fetchs the decoder for the exclusive connection
     * @param[in] exclusiveId The identifier to indicate which slot to get the decoder from.
     * @param[out] decoder Pointer to the decoder returned
     * @return Status of the call
     */
    Status GetExclusiveConnDecoder(int32_t exclusiveId, ZmqMsgDecoder *&decoder);

    /**
     * @brief Fetchs the encoder for the exclusive connection
     * @param[in] exclusiveId The identifier to indicate which slot to get the encoder from.
     * @param[out] decoder Pointer to the encoder returned
     * @return Status of the call
     */
    Status GetExclusiveConnEncoder(int32_t exclusiveId, ZmqMsgEncoder *&encoder);

    /**
     * @brief Creates the exclusive connection and saves it for future use
     * @param[in] exclusiveId The client identifier for the connection
     * @param[in] timeoutMs The timeout to use for send/recv calls in the connection
     * @param[in] sockPath The path to use for connect with server to create the socket connection
     * @return Status of the call
     */
    Status CreateExclusiveConnection(int32_t exclusiveId, int64_t timeoutMs, const std::string &sockPath);

    /**
     * @brief Returns the name of this exclusive conn manager. Used for diagnostic purposes.
     * @return The string name for this manager
     */
    std::string GetExclusiveConnMgrName() const;

    /**
     * @brief Closes and frees the exclusive connection.
     * @param[in] exclusiveId The client identifier for the connection
     * @return Status of the call
     */
    Status CloseExclusiveConn(int32_t exclusiveId);

private:
    class ExclusiveConn {
    public:
        ExclusiveConn() = default;
        ~ExclusiveConn()
        {
            VLOG(RPC_LOG_LEVEL) << "Exclusive conn destructor will close socket fd: " << sockFd_.GetFd();
            sockFd_.Close();
        }
        UnixSockFd sockFd_;
        std::unique_ptr<ZmqMsgEncoder> encoder_;
        std::unique_ptr<ZmqMsgDecoder> decoder_;
    };
    static const int TABLE_SIZE = 128;
    
    Status CreateExclusiveConn(std::unique_ptr<ExclusiveConn> &conn, const std::string &sockPath);

    std::unordered_map<int32_t, std::unique_ptr<ExclusiveConn>> sockMap_;
    std::string name_;
};

extern thread_local ExclusiveConnMgr gExclusiveConnMgr;

}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_ZMQ_EXCLUSIVE_CONN_MGR_H
