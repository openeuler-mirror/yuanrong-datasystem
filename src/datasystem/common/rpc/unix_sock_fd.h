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
 * Description: Raw socket communication.
 */
#ifndef DATASYSTEM_COMMON_RPC_UNIX_SOCK_FD_H
#define DATASYSTEM_COMMON_RPC_UNIX_SOCK_FD_H

#include <fcntl.h>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <netinet/in.h>
#include <securec.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <deque>
#include <memory>

#include "datasystem/common/log/log.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/rpc_option.pb.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/utils.pb.h"

namespace datasystem {
class UnixSockFd {
public:
    explicit UnixSockFd(int fd) : fd_(fd)
    {
    }
    UnixSockFd() : fd_(RPC_NO_FILE_FD)
    {
    }
    ~UnixSockFd() = default;

    /**
     * @brief Return if the fd is initialized or not.
     */
    bool IsValid() const
    {
        return fd_ != RPC_NO_FILE_FD;
    }

    /**
     * @brief Return the socket descriptor.
     * @return Unix domain socket fd.
     */
    int GetFd() const
    {
        return fd_;
    }

    /**
     * @brief Shutdown read and write capabilities of fd.
     */
    void Close()
    {
        if (IsValid()) {
            shutdown(fd_, SHUT_RDWR);
            RETRY_ON_EINTR(close(fd_));
            fd_ = RPC_NO_FILE_FD;
        }
    }

    /**
     * @brief Receive a raw buffer.
     * @param[in] data -- Address for the receiving buffer
     * @param[in] size -- Size of the receiving buffer
     * @param[in] blocking. For non-blocking fd, force non-blocking if true.
     * @return Status of call.
     */
    Status Recv(void *data, size_t size, bool blocking) const;

    /**
     * @brief Send a raw buffer.
     * @param[in] buf Zmq immutable buffer.
     * @return Status of call.
     */
    Status Send(MemView &buf) const;

    /**
     * Receive a 32 bit integer.
     * @param[in] blocking. For non-blocking fd, force non-blocking if true.
     * @param[out] out Output of 32-bit integer.
     * @return Status of call.
     */
    Status Recv32(uint32_t &out, bool blocking) const;

    /**
     * Send a 32 bit integer
     * @param val Input of a 32-bit integer
     * @return Status of call
     */
    Status Send32(uint32_t val);

    /**
     * @brief Send a protobuf using a file descriptor (which can be a socket).
     * @note The protocol is we always send the length of the serialized protobuf
     * followed by the serialized protobuf.
     * @tparam T To-send protobuf type.
     * @param[in] pb To-send protobuf.
     * @return Status of call.
     */
    template <typename T>
    Status SendProtobuf(const T &pb)
    {
        PerfPoint point(PerfKey::ZMQ_SOCK_SEND_PB);
        auto protoSz = pb.ByteSizeLong();
        auto bufSz = protoSz + sizeof(uint32_t);
        std::unique_ptr<char[]> wa;
        void *buf = nullptr;
        if (bufSz <= waSz_) {
            buf = workArea_;
        } else {
            // Reduce the number of memset calls.
            try {
                wa = std::make_unique<char[]>(bufSz);
            } catch (const std::bad_alloc &e) {
                RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, e.what());
            }
            buf = wa.get();
        }
        {
            google::protobuf::io::ArrayOutputStream osWrapper(buf, bufSz);
            google::protobuf::io::CodedOutputStream output(&osWrapper);
            output.WriteLittleEndian32(protoSz);
            bool rc = pb.SerializeToCodedStream(&output);
            CHECK_FAIL_RETURN_STATUS(rc, StatusCode::K_RUNTIME_ERROR, "Serialization error");
        }
        MemView ptr(buf, bufSz);
        RETURN_IF_NOT_OK(Send(ptr));
        point.Record();
        return Status::OK();
    }

    /**
     * @brief Receive a protobuf.
     * @tparam T To-receive protobuf type.
     * @param pb To-receive protobuf.
     * @return Status of call.
     */
    template <typename T>
    Status RecvProtobuf(T &pb)
    {
        // We are trying to receive a protobuf where the first 4 bytes is the size
        // of the serialized protobuf followed by the protobuf itself.
        PerfPoint point(PerfKey::ZMQ_SOCK_RECV_PB);
        uint32_t sz;
        RETURN_IF_NOT_OK(Recv32(sz, true));
        std::unique_ptr<char[]> wa;
        void *buf = nullptr;
        if (sz <= waSz_) {
            buf = workArea_;
        } else {
            wa = std::make_unique<char[]>(sz);
            buf = wa.get();
        }
        // Block ourselves until we receive the rest of the buffer
        RETURN_IF_NOT_OK(Recv(buf, sz, true));
        bool rc = pb.ParseFromArray(buf, sz);
        point.Record();
        return rc ? Status::OK() : Status(StatusCode::K_INVALID, "Failed to ParseFromArray");
    }

    /**
     * @brief Send a Status object.
     * @param[in] rc Return code.
     * @return Status of call.
     */
    Status SendStatus(const Status &rc);

    /**
     * @brief Receive a status object.
     * @return Status of call.
     */
    Status RecvStatus();

    /**
     * @brief Set up a socket of type AF_UNIX.
     * @return Status of call.
     */
    Status CreateUnixSocket();

    /**
     * @brief Setup a socket of type AF_INET
     * @return Status of call.
     */
    Status CreateTcpIpSocket();

    /**
     * Disable Nagel algorithm
     * @return Status of call
     */
    Status SetNoDelay() const;

    /**
     * Turn on SO_KEEPALIVE
     * @return Status of call
     */
    Status KeepAlive() const;

    /**
     * @brief Initialize the fd_ for unix socket.
     * @param[in] path Unix socket path.
     * @param[in] addr User provided sockaddr.
     * @return Status of call.
     */
    static Status SetUpSockPath(const std::string &path, struct sockaddr_un &addr);

    /**
     * @brief Initialize the fd_ for tcp/ip socket.
     * @param[in] path tcp/ip endpoint in the form "x.y.z.w:port".
     * @param[in] addr User provided sockaddr.
     * @return Status of call.
     */
    static Status SetUpTcpIpAddr(const std::string &endPt, struct sockaddr_in &addr);

    /**
     * @brief Bind to socket.
     * @param[in] addr Created by SetUpSockPath.
     * @param[in] perm Permission of path to bind to.
     * @return Status of call.
     */
    Status Bind(sockaddr_un &addr, mode_t perm) const;

    /**
     * @brief Bind to socket.
     * @param[in] addr Created by SetUpTcpIpSocket.
     * @return Status of call.
     */
    Status Bind(sockaddr_in &addr) const;

    /**
     * @brief Bind to an endpoint starts with either "ipc://" or "tcp://"
     * @param[in] ZmqEndPt
     * @param[in] perm Permission of path to bind to (ipc:// only)
     * @param[out] bindStr (same as endpoint without ipc:// or tcp://, and random port is fully expanded)
     * @return Status
     */
    Status Bind(const std::string &ZmqEndPt, mode_t perm, std::string &bindStr);

    /**
     * @brief Connect to a remote end point.
     * @param[in] addr Created by SetUpSockPath.
     */
    Status Connect(sockaddr_un &addr) const;

    /**
     * @brief Connect to a remote end point.
     * @param[in] addr Created by SetUpTcpIpSocket.
     */
    Status Connect(sockaddr_in &addr) const;

    /**
     * @brief Connect to an endpoint starts with either "ipc://" or "tcp://"
     * @param ZmqEndPt
     * @return Status
     */
    Status Connect(const std::string &ZmqEndPt);

    /**
     * @brief Uses this UnixSockFd as a listener socket and accepts incoming connection. Creates a new UnixSockFd as
     * output as the connected socket for the caller.
     * @param[out] outSockFd The new socket that was created when the listening socket accepted the connection.
     * @return Status of the call.
     */
    Status Accept(UnixSockFd &outSockFd);

    /**
     * @brief Set timeout on a socket.
     * This timeout only provides a wakeup code. The caller codepath continuously retries.
     * Does not cause a failure of K_RPC_DEADLINE_EXCEEDED.
     * @param[in] timeout Timeout in milliseconds.
     * @return Status of call.
     */
    Status SetTimeout(int64_t timeout) const;

    /**
     * @brief Set timeout on a socket for exclusive connection mode.
     * Same as the above SetTimeout, however this version of the timeout disables the continuous retry logic and
     * enforces that if the timeout is exceeded, it will fail with Status of K_RPC_DEADLINE_EXCEEDED.
     * @param[in] timeout Timeout in milliseconds.
     * @return Status of call.
     */
    Status SetTimeoutEnforced(int64_t timeout);

    /**
     * @brief Set buf size on a socket
     * @param sz
     * @return Status of call.
     */
    Status SetBufSize(int sz) const;

    /**
     * @brief Make a socket non-blocking.
     * @return Status of call.
     */
    Status SetNonBlocking() const;

    /**
     * @brief Make a socket blocking.
     * @return Status of call.
     */
    Status SetBlocking() const;

    /**
     * Poll a socket for ready to send or receive
     * @param event POLLIN or POLLOUT
     * @param timeout in microseconds
     * @return OK or TRY_AGAIN
     */
    Status Poll(short event, int timeout) const;

    /**
     * Convert i/o error to Status
     * @param [in] errno
     * @param [in] file descriptor (for log message, optional)
     * @return Status
     */
    static Status ErrnoToStatus(int err, int fd = RPC_NO_FILE_FD);

    /**
     * Return the address/port a socket bind/connect to
     * @param out
     * @return Status
     */
    Status GetBindingHostPort(datasystem::HostPort &out) const;

private:
    // The underlying type of this enum (int) matches the third argument for getsockopt and setsockopt from sys/socket.h
    // system calls.
    enum class TimeoutType : int { SendTimeout = SO_SNDTIMEO, RecvTimeout = SO_RCVTIMEO };

    /**
     * @brief Sets the timeout for either send or recv.
     * @param[in] timeoutType The type to set (either send or recv)
     * @param[in] timeoutMs The amount of time in milliseconds to set for the timeout
     * @return Status of the call
     */
    Status SetTimeout(TimeoutType timeoutType, int64_t timeoutMs) const;

    /**
     * @brief Gets the timeout for either send or recv.
     * @param[in] timeoutType The type to get (either send or recv)
     * @param[out] timeoutMs The amount of time in milliseconds to set for the timeout
     * @return Status of the call
     */
    Status GetTimeout(TimeoutType timeoutType, int64_t &timeoutMs) const;

    /**
     * @brief Receive a raw buffer, has some retry logic but does not respect overall timeout
     * @param[in] data -- Address for the receiving buffer
     * @param[in] size -- Size of the receiving buffer
     * @param[in] blocking. For non-blocking fd, force non-blocking if true.
     * @return Status of call.
     */
    Status RecvNoTimeout(void *data, size_t size, bool blocking) const;

    /**
     * @brief Receive a raw buffer with timeout support
     * @param[in] data -- Address for the receiving buffer
     * @param[in] size -- Size of the receiving buffer
     * @return Status of call.
     */
    Status RecvWithTimeout(void *data, size_t size) const;

    /**
     * @brief Send a raw buffer.
     * @param[in] buf Zmq immutable buffer.
     * @return Status of call.
     */
    Status SendNoTimeout(MemView &buf) const;

    /**
     * @brief Send a raw buffer.
     * @param[in] buf Zmq immutable buffer.
     * @return Status of call.
     */
    Status SendWithTimeout(MemView &buf) const;

    /**
     * @brief Compute how much time has elapsed and the determine if the timeout has been exceeded.
     * @param[in] startTime The start time to compare with
     * @param[in] startingTimeout The amount of overall time that is allowed
     * @param[out] timeDiff The computed amount of time between current time now and the start
     * @param[out] timeRemaining The computed amount of time allowed for future send/recv calls
     * @return Status of the call. Return K_RPC_DEADLINE_EXCEEDED if the timeout has expired.
     */
    inline static Status CheckAndComputeTimeout(const std::chrono::time_point<std::chrono::steady_clock> &startTime,
                                                const int64_t &startingTimeoutMs, int64_t &timeDiff,
                                                int64_t &timeRemainingMs)
    {
        std::chrono::time_point<std::chrono::steady_clock> currentTime = std::chrono::steady_clock::now();
        timeDiff = std::chrono::duration_cast<std::chrono::milliseconds>(currentTime - startTime).count();
        timeRemainingMs = startingTimeoutMs - timeDiff;
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(timeRemainingMs > 0, K_RPC_DEADLINE_EXCEEDED, "Socket send/recv timeout");
        return Status::OK();
    }

    constexpr static int waSz_ = 64;
    int fd_;
    bool timeoutEnabled_{ false };
    char workArea_[waSz_]{};
};
/**
 * @brief Parse a zmq ipc transport.
 * @param[in] endPt Endpoint string.
 * @return Parsed endpoint string.
 */
inline std::string ParseEndPt(const std::string &endPt, const std::string &prefix)
{
    std::string ans;
    if (endPt.compare(0, prefix.size(), prefix) == 0) {
        ans = endPt.substr(prefix.size());
    }
    return ans;
}

inline std::string GetServiceSockName(int index)
{
    return ServiceSocketNames_descriptor()->value(index)->options().GetExtension(name);
}
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_UNIX_SOCK_FD_H
