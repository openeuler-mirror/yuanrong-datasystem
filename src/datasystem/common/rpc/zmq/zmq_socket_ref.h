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
 * Description: Wrapper for zmq socket functions.
 */
#ifndef DATASYSTEM_COMMON_RPC_ZMQ_SOCKET_BASE_H
#define DATASYSTEM_COMMON_RPC_ZMQ_SOCKET_BASE_H

#include <cstring>
#include <zmq.h>

#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace sockopt {
template <int Opt, typename T, bool boolUnit>
struct FixedLengthOpt {};
template <int Opt, int NullInd>
struct VarLengthOpt {};
constexpr FixedLengthOpt<ZMQ_IMMEDIATE, int, true> ZmqImmediate{};
constexpr FixedLengthOpt<ZMQ_LINGER, int, false> ZmqLinger{};
constexpr FixedLengthOpt<ZMQ_SNDTIMEO, int, false> ZmqSndtimeo{};
constexpr FixedLengthOpt<ZMQ_RCVTIMEO, int, false> ZmqRcvtimeo{};
constexpr FixedLengthOpt<ZMQ_SNDHWM, int, false> ZmqSndhwm{};
constexpr FixedLengthOpt<ZMQ_RCVHWM, int, false> ZmqRcvhwm{};
constexpr FixedLengthOpt<ZMQ_CURVE_SERVER, int, true> ZmqCurveServer{};
constexpr FixedLengthOpt<ZMQ_BACKLOG, int, false> ZmqBacklog{};
constexpr FixedLengthOpt<ZMQ_FD, int, false> ZmqEventFd{};
constexpr FixedLengthOpt<ZMQ_EVENTS, int, false> ZmqEvents{};
constexpr FixedLengthOpt<ZMQ_PROBE_ROUTER, int, true> ZmqProbeRouter{};
constexpr VarLengthOpt<ZMQ_ROUTING_ID, 0> ZmqRoutingId{};
constexpr VarLengthOpt<ZMQ_LAST_ENDPOINT, 0> ZmqLastEndPt{};
constexpr VarLengthOpt<ZMQ_CURVE_PUBLICKEY, 2> ZmqCurvePublicKey{};
constexpr VarLengthOpt<ZMQ_CURVE_SECRETKEY, 2> ZmqCurveSecretKey{};
constexpr VarLengthOpt<ZMQ_CURVE_SERVERKEY, 2> ZmqCurveServerKey{};
}  // namespace sockopt

class ZmqSocketRef {
public:
    ZmqSocketRef() : sock_(nullptr)
    {
    }
    explicit ZmqSocketRef(void *sock) : sock_(sock)
    {
    }
    ~ZmqSocketRef() = default;

    // A ZMQ socket reference is not copyable but movable.
    // At any time, there is only one copy (like unique pointer) of the ZMQ socket.
    // Because of this reason, when the class goes out of scope, we can close the socket.
    ZmqSocketRef(const ZmqSocketRef &other) = delete;
    ZmqSocketRef &operator=(const ZmqSocketRef &other) = delete;

    ZmqSocketRef(ZmqSocketRef &&other) noexcept : sock_(other.sock_)
    {
        other.sock_ = nullptr;
    }
    ZmqSocketRef &operator=(ZmqSocketRef &&other) noexcept
    {
        if (&other != this) {
            sock_ = other.sock_;
            other.sock_ = nullptr;
        }
        return *this;
    }

    bool IsValid() const
    {
        return sock_ != nullptr;
    }

    void *GetHandle() const
    {
        return sock_;
    }

    template <int Opt, typename T>
    Status Set(sockopt::FixedLengthOpt<Opt, T, true>, bool val)
    {
        T v = val;
        return SetOption(Opt, &v, sizeof(v));
    }

    template <int Opt, typename T, bool boolUnit>
    Status Set(sockopt::FixedLengthOpt<Opt, T, boolUnit>, T val)
    {
        return SetOption(Opt, &val, sizeof(val));
    }

    template <int Opt, int NullInd>
    Status Set(sockopt::VarLengthOpt<Opt, NullInd>, const std::string &str)
    {
        return SetOption(Opt, str.data(), str.length());
    }

    template <int Opt, int NullInd>
    Status Set(sockopt::VarLengthOpt<Opt, NullInd>, const char *buf)
    {
        return SetOption(Opt, buf, strlen(buf));
    }

    template <int Opt, typename T, bool boolUnit>
    T Get(sockopt::FixedLengthOpt<Opt, T, boolUnit> o, T defaultVal) const
    {
        T val;
        if (Get(o, &val).IsOk()) {
            return val;
        }
        return defaultVal;
    }

    template <int Opt, int NullInd>
    std::string Get(sockopt::VarLengthOpt<Opt, NullInd>) const
    {
        const int maxLen = 256;
        std::string str(maxLen, '\0');
        size_t len = maxLen;
        if (GetOption(Opt, const_cast<char *>(str.data()), &len).IsOk()) {
            str.resize(len);
        }
        return str;
    }

    void Close();
    Status Bind(const std::string &endPoint);
    Status Connect(const std::string &endPoint, bool isIPv6 = false);
    Status RecvMsg(ZmqMessage &msg, ZmqRecvFlags flags);
    Status SendMsg(ZmqMessage &msg, ZmqSendFlags flags);

    static Status ZmqErrnoToStatus(int rc, const std::string &msg, StatusCode defaultRc = K_RUNTIME_ERROR);

private:
    void *sock_;
    Status SetOption(int option, const void *val, size_t len);
    Status GetOption(int option, void *val, size_t *len) const;

    template <int Opt, typename T, bool boolUnit>
    Status Get(sockopt::FixedLengthOpt<Opt, T, boolUnit>, T *val) const
    {
        size_t len = sizeof(T);
        RETURN_IF_NOT_OK(GetOption(Opt, val, &len));
        CHECK_FAIL_RETURN_STATUS(len == sizeof(T), K_RUNTIME_ERROR,
                                 FormatString("Incorrect type of getsockopt (%zu != %zu)", len, sizeof(T)));
        return Status::OK();
    }

    template <int Opt, int NullInd>
    Status Get(sockopt::VarLengthOpt<Opt, NullInd>, void *buf, size_t *len) const
    {
        return GetOption(Opt, buf, len);
    }
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_ZMQ_SOCKET_BASE_H
