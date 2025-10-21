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
 * Description: ZMQ context definition.
 */
#include "datasystem/common/rpc/zmq/zmq_context.h"

#include <cstdint>
#include <limits>
#include <sys/resource.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/common/rpc/zmq/zmq_common.h"
#include "datasystem/common/rpc/zmq/zmq_socket_ref.h"
#include "datasystem/common/rpc/rpc_options.h"

namespace datasystem {
ZmqContext::ZmqContext(int numIo, int numSo) : interruptFlag_(false), ctx_(nullptr), numIo_(numIo), numSo_(numSo)
{
}

ZmqContext::~ZmqContext()
{
    Close(false);
}

void ZmqContext::Close(bool logging)
{
    /**
     * Once close. No one can create any more ZMQ socket.
     */
    bool expected = false;
    if (interruptFlag_.compare_exchange_strong(expected, true)) {
        /**
         * @note We must close all the ZMQ sockets. Otherwise ZMQ context will hang on shutdown.
         */
        std::lock_guard<std::mutex> lock(mux_);
        if (!allSocks_.empty() && logging) {
            LOG(WARNING) << FormatString("ZMQ context shutdown. %zu socket(s) not closed", allSocks_.size());
        }
        // Go through all the reference and close them.
        for (auto key : allSocks_) {
            ZmqSocketRef ref(reinterpret_cast<void *>(key));
            ref.Close();
        }
        allSocks_.clear();
        if (ctx_ != nullptr) {
            (void)zmq_ctx_term(ctx_);
            ctx_ = nullptr;
        }
        if (logging) {
            VLOG(RPC_LOG_LEVEL) << "ZMQ context (" << this << ") shutting down successfully.";
        }
    }
}

Status ZmqContext::Init()
{
    ctx_ = zmq_ctx_new();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(ctx_ != nullptr, K_RUNTIME_ERROR,
                                         FormatString("Unable to create ZMQ context: %s", zmq_strerror(errno)));

    int rc = zmq_ctx_set(ctx_, ZMQ_IO_THREADS, numIo_);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(rc == 0, K_RUNTIME_ERROR,
                                         FormatString("Unable to set ZMQ_IO_THREADS. %s", zmq_strerror(errno)));
    VLOG(RPC_KEY_LOG_LEVEL) << "ZmqContext created with ZMQ_IO_THREADS = " << numIo_;

    auto numSo = numSo_;
    struct rlimit rl;
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
        const int maxSocketsLimit = 100'000;
        if (rl.rlim_max > std::numeric_limits<int>::max()) {
            numSo = maxSocketsLimit;
        } else {
            numSo = std::min<int>(rl.rlim_max, maxSocketsLimit);
        }
        LOG(INFO) << "soft limit: " << rl.rlim_cur << ", hard limit: " << rl.rlim_max
                  << ", max sockets number: " << numSo;
    }

    rc = zmq_ctx_set(ctx_, ZMQ_MAX_SOCKETS, numSo);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(rc == 0, K_RUNTIME_ERROR,
                                         FormatString("Unable to set ZMQ_MAX_SOCKETS. %s", zmq_strerror(errno)));
    VLOG(RPC_KEY_LOG_LEVEL) << "ZmqContext created with ZMQ_CONTEXT_MAX_SOCKETS = " << numSo;

    return Status::OK();
}

void *ZmqContext::CreateZmqSocket(ZmqSocketType type)
{
    void *sock = nullptr;
    auto func = [this, &sock](ZmqSocketType type) {
        CHECK_FAIL_RETURN_STATUS(ctx_ != nullptr, K_INVALID, "Null context");
        sock = zmq_socket(ctx_, static_cast<int>(type));
        CHECK_FAIL_RETURN_STATUS(
            sock != nullptr, K_RUNTIME_ERROR,
            FormatString("Unable to create ZMQ socket of type %d: %s", static_cast<int>(type), zmq_strerror(errno)));
        {
            std::lock_guard<std::mutex> lock(mux_);
            auto key = reinterpret_cast<intptr_t>(sock);
            allSocks_.insert(key);
        }
        ZmqSocketRef ref(sock);
        // Generate an identity
        RpcOptions opt;
        RETURN_IF_NOT_OK(ref.Set(sockopt::ZmqRoutingId, GetStringUuid().substr(0, ZMQ_SHORT_UUID)));
        RETURN_IF_NOT_OK(ref.Set(sockopt::ZmqLinger, opt.linger_));
        RETURN_IF_NOT_OK(ref.Set(sockopt::ZmqSndtimeo, opt.timeout_));
        RETURN_IF_NOT_OK(ref.Set(sockopt::ZmqRcvtimeo, opt.timeout_));
        RETURN_IF_NOT_OK(ref.Set(sockopt::ZmqImmediate, opt.immediate_));
        return Status::OK();
    };
    Status rc = func(type);
    LOG_IF_ERROR(rc, "CreateZmqSocket");
    return sock;
}

bool ZmqContext::CloseSocket(void *sock)
{
    auto key = reinterpret_cast<intptr_t>(sock);
    ZmqSocketRef ref(sock);
    std::lock_guard<std::mutex> lock(mux_);
    ref.Close();
    return allSocks_.erase(key) > 0;
}
}  // namespace datasystem
