#include "internal/control_plane/socket_rpc_transport.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include <glog/logging.h>

#include "datasystem/transfer_engine/status_helper.h"

namespace datasystem {
namespace {

constexpr uint32_t kMagic = 0x54455250;  // 'TERP'

uint32_t ReadU32Be(const uint8_t *buf)
{
    uint32_t v = 0;
    std::memcpy(&v, buf, sizeof(v));
    return ntohl(v);
}

void WriteU32Be(uint32_t v, std::vector<uint8_t> *buf)
{
    const uint32_t be = htonl(v);
    const auto *p = reinterpret_cast<const uint8_t *>(&be);
    buf->insert(buf->end(), p, p + sizeof(be));
}

bool ReadExact(int fd, uint8_t *buf, size_t n)
{
    size_t off = 0;
    while (off < n) {
        const ssize_t rc = ::recv(fd, buf + off, n - off, 0);
        if (rc == 0) {
            return false;
        }
        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        off += static_cast<size_t>(rc);
    }
    return true;
}

bool WriteExact(int fd, const uint8_t *buf, size_t n)
{
    size_t off = 0;
    while (off < n) {
        const ssize_t rc = ::send(fd, buf + off, n - off, 0);
        if (rc <= 0) {
            if (rc < 0 && errno == EINTR) {
                continue;
            }
            return false;
        }
        off += static_cast<size_t>(rc);
    }
    return true;
}

Status BuildBindAddr(const std::string &host, uint16_t port, sockaddr_in *addr)
{
    TE_CHECK_PTR_OR_RETURN(addr);
    std::memset(addr, 0, sizeof(*addr));
    addr->sin_family = AF_INET;
    addr->sin_port = htons(port);

    if (host == "0.0.0.0" || host == "*" || host.empty()) {
        addr->sin_addr.s_addr = htonl(INADDR_ANY);
        return Status::OK();
    }
    if (::inet_pton(AF_INET, host.c_str(), &addr->sin_addr) != 1) {
        return TE_MAKE_STATUS(StatusCode::kInvalid, "host should be a valid IPv4 address");
    }
    return Status::OK();
}

}  // namespace

ScopedFd::ScopedFd() = default;

ScopedFd::ScopedFd(int fd) : fd_(fd) {}

ScopedFd::~ScopedFd()
{
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

ScopedFd::ScopedFd(ScopedFd &&other) noexcept : fd_(other.fd_)
{
    other.fd_ = -1;
}

ScopedFd &ScopedFd::operator=(ScopedFd &&other) noexcept
{
    if (this != &other) {
        if (fd_ >= 0) {
            ::close(fd_);
        }
        fd_ = other.fd_;
        other.fd_ = -1;
    }
    return *this;
}

int ScopedFd::Get() const
{
    return fd_;
}

Status ConnectTo(const std::string &host, uint16_t port, int *fd)
{
    TE_CHECK_PTR_OR_RETURN(fd);
    *fd = -1;

    struct addrinfo hints;
    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo *result = nullptr;
    const std::string portStr = std::to_string(port);
    const int gaiRc = getaddrinfo(host.c_str(), portStr.c_str(), &hints, &result);
    if (gaiRc != 0) {
        LOG(WARNING) << "getaddrinfo failed"
                     << ", host=" << host << ", port=" << port << ", gai_rc=" << gaiRc;
        return TE_MAKE_STATUS(StatusCode::kRuntimeError, std::string("getaddrinfo failed: ") + gai_strerror(gaiRc));
    }

    int sock = -1;
    for (auto *it = result; it != nullptr; it = it->ai_next) {
        sock = ::socket(it->ai_family, it->ai_socktype, it->ai_protocol);
        if (sock < 0) {
            continue;
        }
        if (::connect(sock, it->ai_addr, it->ai_addrlen) == 0) {
            *fd = sock;
            freeaddrinfo(result);
            return Status::OK();
        }
        ::close(sock);
        sock = -1;
    }
    freeaddrinfo(result);
    LOG(WARNING) << "connect failed, host=" << host << ", port=" << port;
    return TE_MAKE_STATUS(StatusCode::kRuntimeError, "connect failed");
}

Status CreateListenSocket(const std::string &host, uint16_t port, int backlog, int *listenFd)
{
    TE_CHECK_PTR_OR_RETURN(listenFd);
    *listenFd = -1;
    TE_CHECK_OR_RETURN(backlog > 0, StatusCode::kInvalid, "backlog should be positive");

    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    TE_CHECK_OR_RETURN(fd >= 0, StatusCode::kRuntimeError, "create socket failed");

    int opt = 1;
    (void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr;
    Status addrRc = BuildBindAddr(host, port, &addr);
    if (addrRc.IsError()) {
        ::close(fd);
        return addrRc;
    }
    if (::bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
        LOG(ERROR) << "bind failed, host=" << host << ", port=" << port << ", errno=" << errno;
        ::close(fd);
        return TE_MAKE_STATUS(StatusCode::kRuntimeError, "bind failed");
    }
    if (::listen(fd, backlog) != 0) {
        LOG(ERROR) << "listen failed, host=" << host << ", port=" << port << ", errno=" << errno;
        ::close(fd);
        return TE_MAKE_STATUS(StatusCode::kRuntimeError, "listen failed");
    }
    *listenFd = fd;
    return Status::OK();
}

Status SetSocketTimeoutSec(int fd, int timeoutSec)
{
    TE_CHECK_OR_RETURN(fd >= 0, StatusCode::kInvalid, "invalid fd");
    TE_CHECK_OR_RETURN(timeoutSec > 0, StatusCode::kInvalid, "timeout should be positive");
    timeval timeout {};
    timeout.tv_sec = timeoutSec;
    timeout.tv_usec = 0;
    (void)setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    (void)setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    return Status::OK();
}

Status SendFrame(int fd, RpcMethod method, const std::vector<uint8_t> &payload)
{
    std::vector<uint8_t> frame;
    frame.reserve(4 + 1 + 4 + payload.size());
    WriteU32Be(kMagic, &frame);
    frame.push_back(static_cast<uint8_t>(method));
    WriteU32Be(static_cast<uint32_t>(payload.size()), &frame);
    frame.insert(frame.end(), payload.begin(), payload.end());
    if (!WriteExact(fd, frame.data(), frame.size())) {
        return TE_MAKE_STATUS(StatusCode::kRuntimeError, "send frame failed");
    }
    return Status::OK();
}

Status RecvFrame(int fd, RpcMethod *method, std::vector<uint8_t> *payload)
{
    TE_CHECK_PTR_OR_RETURN(method);
    TE_CHECK_PTR_OR_RETURN(payload);

    uint8_t header[9] = {0};
    if (!ReadExact(fd, header, sizeof(header))) {
        return TE_MAKE_STATUS(StatusCode::kRuntimeError, "read frame header failed");
    }
    const uint32_t magic = ReadU32Be(header);
    if (magic != kMagic) {
        return TE_MAKE_STATUS(StatusCode::kRuntimeError, "invalid frame magic");
    }
    *method = static_cast<RpcMethod>(header[4]);
    const uint32_t payloadLen = ReadU32Be(header + 5);

    payload->assign(payloadLen, 0);
    if (payloadLen > 0 && !ReadExact(fd, payload->data(), payloadLen)) {
        return TE_MAKE_STATUS(StatusCode::kRuntimeError, "read frame payload failed");
    }
    return Status::OK();
}

Status InvokeRpc(const std::string &host, uint16_t port, RpcMethod expectedMethod, const std::vector<uint8_t> &reqPayload,
                 std::vector<uint8_t> *rspPayload)
{
    TE_CHECK_PTR_OR_RETURN(rspPayload);
    int fd = -1;
    TE_RETURN_IF_ERROR(ConnectTo(host, port, &fd));
    ScopedFd scopedFd(fd);

    TE_RETURN_IF_ERROR(SendFrame(scopedFd.Get(), expectedMethod, reqPayload));
    RpcMethod method;
    TE_RETURN_IF_ERROR(RecvFrame(scopedFd.Get(), &method, rspPayload));
    if (method != expectedMethod) {
        LOG(ERROR) << "rpc method mismatch, expected=" << static_cast<int>(expectedMethod)
                   << ", actual=" << static_cast<int>(method);
        return TE_MAKE_STATUS(StatusCode::kRuntimeError, "rpc method mismatch");
    }
    return Status::OK();
}

}  // namespace datasystem
