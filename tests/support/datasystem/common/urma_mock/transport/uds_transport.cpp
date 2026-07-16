/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#include "datasystem/common/urma_mock/transport/uds_transport.h"

#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/strings_util.h"

namespace datasystem {
namespace urma_mock {

namespace {

// Linux: sun_path is 108 bytes including the NUL. Keep the bound path
// well under that to avoid surprises with long test-instance names.
constexpr socklen_t K_SUN_PATH_MAX = 108;
constexpr mode_t K_UDS_DIR_MODE = 0700;
constexpr int K_LISTEN_BACKLOG = 8;
constexpr size_t K_U32_WIRE_SIZE = sizeof(uint32_t);
constexpr size_t K_U16_WIRE_SIZE = sizeof(uint16_t);
constexpr size_t K_UDS_HEADER_MAGIC_OFFSET = 0;
constexpr size_t K_UDS_HEADER_TYPE_OFFSET = 4;
constexpr size_t K_UDS_HEADER_FLAGS_OFFSET = 6;
constexpr size_t K_UDS_HEADER_PAYLOAD_LEN_OFFSET = 8;
constexpr size_t K_UDS_HEADER_RESERVED_OFFSET = 12;
constexpr size_t K_RECV_PAYLOAD_BUF_SIZE = size_t{ 64 } * 1024;
constexpr size_t K_RECV_CONTROL_BUF_SIZE = 256;
constexpr int K_HEADER_IOV_COUNT = 1;
constexpr int K_PAYLOAD_IOV_COUNT = 2;

std::string GetEnvOrEmpty(const char *name)
{
    const char *v = std::getenv(name);
    return v == nullptr ? std::string{} : std::string(v);
}

std::string DefaultUdsBaseDir()
{
    return "/tmp/datasystem_urma_mock_" + std::to_string(static_cast<long long>(::getuid()));
}

std::string ResolveBaseDir(const char *overrideBase)
{
    std::string baseDir = overrideBase != nullptr ? std::string(overrideBase) : GetEnvOrEmpty("URMA_MOCK_UDS_BASE_DIR");
    return baseDir.empty() ? DefaultUdsBaseDir() : baseDir;
}

bool MkdirAll(const std::string &dir)
{
    if (dir.empty()) {
        return true;
    }
    size_t pos = dir[0] == '/' ? 1 : 0;
    while (pos <= dir.size()) {
        auto next = dir.find('/', pos);
        auto current = dir.substr(0, next);
        if (!current.empty() && ::mkdir(current.c_str(), K_UDS_DIR_MODE) != 0 && errno != EEXIST) {
            LOG(ERROR) << "[UdsListener] mkdir " << current << " failed: " << StrErr(errno);
            return false;
        }
        if (next == std::string::npos) {
            break;
        }
        pos = next + 1;
    }
    return true;
}

}  // namespace

std::string ResolveUdsPath(const char *overrideBase)
{
    std::string instance = GetEnvOrEmpty("URMA_MOCK_UDS_INSTANCE");
    if (instance.empty()) {
        // Default: deterministic mock instance. Tests may override the env.
        instance = "instance_default";
    }
    return ResolveBaseDir(overrideBase) + "/" + instance + "/uds.sock";
}

std::string ResolveUdsPathForInstance(const std::string &instance)
{
    return ResolveBaseDir(nullptr) + "/" + instance + "/uds.sock";
}

std::string ResolveUdsPathForHost(const std::string &host, int port)
{
    // receiver-side derivation. The peer host:port is whatever
    // UrmaJfrInfo::ToProto put on the wire (urma_info.h:49-50). For IPv6
    // the host already contains colons inside brackets (HostPort::ToString
    // format "[ipv6]:port"); concatenating with ":port" yields a
    // colon-separated instance component that is safe as a path segment.
    std::string instance = host + ":" + std::to_string(port);
    return ResolveBaseDir(nullptr) + "/" + instance + "/uds.sock";
}

UdsListener::UdsListener() = default;

UdsListener::~UdsListener()
{
    Close();
    if (!path_.empty()) {
        ::unlink(path_.c_str());
    }
}

void UdsListener::Close()
{
    if (listenFd_ >= 0) {
        // shutdown() first to wake any thread blocked in accept() on this
        // fd; close() alone does not always unblock accept on Linux when
        // invoked from a different thread.
        ::shutdown(listenFd_, SHUT_RDWR);
        ::close(listenFd_);
        listenFd_ = -1;
    }
}

const std::string &UdsListener::GetPath() const
{
    return path_;
}

int UdsListener::GetListenFd() const
{
    return listenFd_;
}

bool UdsListener::Bind(const std::string &path)
{
    path_ = path;
    // Ensure parent dir exists (mkdir -p semantics for one level).
    auto slash = path_.find_last_of('/');
    if (slash != std::string::npos && slash > 0) {
        std::string dir = path_.substr(0, slash);
        if (!MkdirAll(dir)) {
            return false;
        }
    }
    // Pre-clean: the test suite creates many listeners with the same
    // path. Bind will fail with EADDRINUSE if a stale socket lingers.
    ::unlink(path_.c_str());

    listenFd_ = ::socket(AF_UNIX, SOCK_SEQPACKET, 0);
    if (listenFd_ < 0) {
        LOG(ERROR) << "[UdsListener] socket failed: " << StrErr(errno);
        return false;
    }
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    if (path_.size() >= K_SUN_PATH_MAX) {
        LOG(ERROR) << "[UdsListener] path too long (" << path_.size() << " >= " << K_SUN_PATH_MAX << ")";
        ::close(listenFd_);
        listenFd_ = -1;
        return false;
    }
    std::memcpy(addr.sun_path, path_.c_str(), path_.size());
    addr.sun_path[path_.size()] = '\0';
    socklen_t addrLen = static_cast<socklen_t>(offsetof(sockaddr_un, sun_path) + path_.size() + 1);
    if (::bind(listenFd_, reinterpret_cast<sockaddr *>(&addr), addrLen) != 0) {
        LOG(ERROR) << "[UdsListener] bind " << path_ << " failed: " << StrErr(errno);
        ::close(listenFd_);
        listenFd_ = -1;
        return false;
    }
    if (::listen(listenFd_, K_LISTEN_BACKLOG) != 0) {
        LOG(ERROR) << "[UdsListener] listen failed: " << StrErr(errno);
        ::close(listenFd_);
        listenFd_ = -1;
        return false;
    }
    return true;
}

int UdsListener::Accept() const
{
    if (listenFd_ < 0) {
        return -1;
    }
    int peerFd = ::accept(listenFd_, nullptr, nullptr);
    if (peerFd < 0) {
        LOG(ERROR) << "[UdsListener] accept failed: " << StrErr(errno);
        return -1;
    }
    return peerFd;
}

UdsConnection::UdsConnection() = default;

UdsConnection::~UdsConnection()
{
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

bool UdsConnection::Connect(const std::string &path)
{
    fd_ = ::socket(AF_UNIX, SOCK_SEQPACKET, 0);
    if (fd_ < 0) {
        LOG(ERROR) << "[UdsConnection] socket failed: " << StrErr(errno);
        return false;
    }
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    if (path.size() >= K_SUN_PATH_MAX) {
        LOG(ERROR) << "[UdsConnection] path too long";
        ::close(fd_);
        fd_ = -1;
        return false;
    }
    std::memcpy(addr.sun_path, path.c_str(), path.size());
    addr.sun_path[path.size()] = '\0';
    socklen_t addrLen = static_cast<socklen_t>(offsetof(sockaddr_un, sun_path) + path.size() + 1);
    if (::connect(fd_, reinterpret_cast<sockaddr *>(&addr), addrLen) != 0) {
        LOG(ERROR) << "[UdsConnection] connect " << path << " failed: " << StrErr(errno);
        ::close(fd_);
        fd_ = -1;
        return false;
    }
    return true;
}

void UdsConnection::AdoptFd(int fd)
{
    fd_ = fd;
}

namespace {
void PackUdsHeader(uint8_t *outBuf, uint32_t magic, UdsMsgType type, uint16_t flags, uint32_t payloadLen)
{
    // 16B header layout (matches UnpackUdsHeader on the wire):
    //   [0..4)   magic           (uint32_t)
    //   [4..6)   type            (uint16_t)
    //   [6..8)   flags           (uint16_t)
    //   [8..12)  payload_len     (uint32_t)
    //   [12..16) reserved        (uint32_t, must be 0)
    uint8_t *p = outBuf;
    std::memset(p, 0, kUdsHeaderSize);
    std::memcpy(p + K_UDS_HEADER_MAGIC_OFFSET, &magic, K_U32_WIRE_SIZE);
    uint16_t type16 = static_cast<uint16_t>(type);
    std::memcpy(p + K_UDS_HEADER_TYPE_OFFSET, &type16, K_U16_WIRE_SIZE);
    std::memcpy(p + K_UDS_HEADER_FLAGS_OFFSET, &flags, K_U16_WIRE_SIZE);
    std::memcpy(p + K_UDS_HEADER_PAYLOAD_LEN_OFFSET, &payloadLen, K_U32_WIRE_SIZE);
    uint32_t reserved2 = 0;
    std::memcpy(p + K_UDS_HEADER_RESERVED_OFFSET, &reserved2, K_U32_WIRE_SIZE);
}
}  // namespace

bool UnpackUdsHeader(const uint8_t *buf, uint32_t *outMagic, UdsMsgType *outType, uint16_t *outFlags,
                     uint32_t *outPayloadLen)
{
    const uint8_t *p = buf;
    uint32_t magic = 0;
    std::memcpy(&magic, p + K_UDS_HEADER_MAGIC_OFFSET, K_U32_WIRE_SIZE);
    uint16_t type16 = 0;
    std::memcpy(&type16, p + K_UDS_HEADER_TYPE_OFFSET, K_U16_WIRE_SIZE);
    uint16_t flags = 0;
    std::memcpy(&flags, p + K_UDS_HEADER_FLAGS_OFFSET, K_U16_WIRE_SIZE);
    uint32_t payloadLen = 0;
    std::memcpy(&payloadLen, p + K_UDS_HEADER_PAYLOAD_LEN_OFFSET, K_U32_WIRE_SIZE);
    if (outMagic != nullptr) {
        *outMagic = magic;
    }
    if (outType != nullptr) {
        *outType = static_cast<UdsMsgType>(type16);
    }
    if (outFlags != nullptr) {
        *outFlags = flags;
    }
    if (outPayloadLen != nullptr) {
        *outPayloadLen = payloadLen;
    }
    return magic == kUdsMagic;
}

void CollectReceivedFds(msghdr &msg, std::vector<int> *outFds)
{
    if (outFds == nullptr) {
        return;
    }
    outFds->clear();
    for (cmsghdr *cmsg = CMSG_FIRSTHDR(&msg); cmsg != nullptr; cmsg = CMSG_NXTHDR(&msg, cmsg)) {
        if (cmsg->cmsg_level != SOL_SOCKET || cmsg->cmsg_type != SCM_RIGHTS) {
            continue;
        }
        size_t fdCount = (cmsg->cmsg_len - CMSG_LEN(0)) / sizeof(int);
        auto *fdData = CMSG_DATA(cmsg);
        for (size_t i = 0; i < fdCount; ++i) {
            int fd = -1;
            std::memcpy(&fd, fdData + (i * sizeof(int)), sizeof(fd));
            outFds->push_back(fd);
        }
    }
}

void CloseFds(const std::vector<int> &fds)
{
    for (int fd : fds) {
        if (fd >= 0) {
            ::close(fd);
        }
    }
}

struct UdsRecvFrame {
    UdsRecvFrame()
    {
        iov[0].iov_base = header;
        iov[0].iov_len = kUdsHeaderSize;
        iov[1].iov_base = payloadBuf.data();
        iov[1].iov_len = payloadBuf.size();
        msg.msg_iov = iov;
        msg.msg_iovlen = sizeof(iov) / sizeof(iov[0]);
        msg.msg_control = cmsgBuf.data();
        msg.msg_controllen = cmsgBuf.size();
    }

    uint8_t header[kUdsHeaderSize];
    std::vector<uint8_t> payloadBuf = std::vector<uint8_t>(K_RECV_PAYLOAD_BUF_SIZE);
    iovec iov[2];
    std::vector<uint8_t> cmsgBuf = std::vector<uint8_t>(K_RECV_CONTROL_BUF_SIZE);
    msghdr msg{};
};

void FillRecvPayload(const UdsRecvFrame &frame, uint32_t payloadLen, size_t bytesRead, std::vector<uint8_t> *outPayload,
                     bool *outPayloadTruncated)
{
    bool truncated = payloadLen > bytesRead;
    if (outPayload != nullptr) {
        size_t copyLen = std::min(payloadLen, static_cast<uint32_t>(bytesRead));
        outPayload->assign(frame.payloadBuf.begin(),
                           frame.payloadBuf.begin() + static_cast<std::vector<uint8_t>::difference_type>(copyLen));
    }
    if (outPayloadTruncated != nullptr) {
        *outPayloadTruncated = truncated;
    }
}

bool UdsConnection::Send(UdsMsgType type, uint16_t flags, const uint8_t *payload, uint32_t payloadLen,
                         const std::vector<int> &fds, int *outErrno)
{
    if (fd_ < 0) {
        if (outErrno) {
            *outErrno = EBADF;
        }
        return false;
    }
    uint8_t header[kUdsHeaderSize];
    PackUdsHeader(header, kUdsMagic, type, flags, payloadLen);

    msghdr msg{};
    iovec iov[2];
    int iovCount = K_HEADER_IOV_COUNT;
    iov[0].iov_base = header;
    iov[0].iov_len = kUdsHeaderSize;
    if (payloadLen > 0 && payload != nullptr) {
        iov[1].iov_base = const_cast<uint8_t *>(payload);
        iov[1].iov_len = payloadLen;
        iovCount = K_PAYLOAD_IOV_COUNT;
    }
    msg.msg_name = nullptr;
    msg.msg_namelen = 0;
    msg.msg_iov = iov;
    msg.msg_iovlen = iovCount;

    std::vector<uint8_t> cmsgBuf;
    if (!fds.empty()) {
        // SCM_RIGHTS cmsg: header + N file descriptors + alignment.
        size_t cmsgLen = CMSG_SPACE(fds.size() * sizeof(int));
        cmsgBuf.resize(cmsgLen);
        msg.msg_control = cmsgBuf.data();
        msg.msg_controllen = cmsgLen;
        cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
        cmsg->cmsg_level = SOL_SOCKET;
        cmsg->cmsg_type = SCM_RIGHTS;
        cmsg->cmsg_len = CMSG_LEN(fds.size() * sizeof(int));
        std::memcpy(CMSG_DATA(cmsg), fds.data(), fds.size() * sizeof(int));
    }

    ssize_t n = ::sendmsg(fd_, &msg, 0);
    if (n < 0) {
        if (outErrno) {
            *outErrno = errno;
        }
        return false;
    }
    // SOCK_SEQPACKET is message-oriented; a short send means truncation.
    size_t expected = kUdsHeaderSize + payloadLen;
    if (static_cast<size_t>(n) != expected) {
        if (outErrno) {
            *outErrno = EMSGSIZE;
        }
        return false;
    }
    return true;
}

bool UdsConnection::Recv(UdsMsgType *outType, uint16_t *outFlags, std::vector<uint8_t> *outPayload,
                         std::vector<int> *outFds, bool *outPayloadTruncated, int *outErrno) const
{
    if (fd_ < 0) {
        if (outErrno) {
            *outErrno = EBADF;
        }
        return false;
    }
    UdsRecvFrame frame;
    ssize_t n = ::recvmsg(fd_, &frame.msg, 0);
    if (n < 0) {
        if (outErrno) {
            *outErrno = errno;
        }
        return false;
    }
    std::vector<int> receivedFds;
    CollectReceivedFds(frame.msg, &receivedFds);
    bool fdsTransferred = false;
    datasystem::Raii closeReceivedFds([&receivedFds, &fdsTransferred]() {
        if (!fdsTransferred) {
            CloseFds(receivedFds);
        }
    });
    if (static_cast<size_t>(n) < kUdsHeaderSize) {
        if (outErrno) {
            *outErrno = EINVAL;
        }
        return false;
    }
    uint32_t magic = 0;
    uint32_t payloadLen = 0;
    if (!UnpackUdsHeader(frame.header, &magic, outType, outFlags, &payloadLen) || magic != kUdsMagic) {
        if (outErrno) {
            *outErrno = EINVAL;
        }
        return false;
    }
    size_t bytesRead = static_cast<size_t>(n) - kUdsHeaderSize;
    FillRecvPayload(frame, payloadLen, bytesRead, outPayload, outPayloadTruncated);
    if (outFds != nullptr) {
        *outFds = std::move(receivedFds);
        fdsTransferred = true;
    }
    return true;
}

bool UdsConnection::SetRecvTimeout(int timeoutMs)
{
    if (fd_ < 0 || timeoutMs < 0) {
        return false;
    }
    timeval tv{};
    tv.tv_sec = timeoutMs / 1000;
    tv.tv_usec = (timeoutMs % 1000) * 1000;
    return ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) == 0;
}

}  // namespace urma_mock
}  // namespace datasystem
