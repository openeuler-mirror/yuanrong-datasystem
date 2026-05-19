#include "internal/control_plane/socket_rpc_transport.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <memory>

#include "datasystem/transfer_engine/status_helper.h"
#include "internal/log/logging.h"

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

struct AddrInfoDeleter {
    void operator()(addrinfo *info) const
    {
        if (info != nullptr) {
            freeaddrinfo(info);
        }
    }
};

using AddrInfoPtr = std::unique_ptr<addrinfo, AddrInfoDeleter>;

Result ResolveTcpAddr(const std::string &host, uint16_t port, int flags, AddrInfoPtr *out)
{
    TE_CHECK_PTR_OR_RETURN(out);
    struct addrinfo hints;
    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = flags;

    const std::string portStr = std::to_string(port);
    struct addrinfo *result = nullptr;
    const char *node = host.empty() ? nullptr : host.c_str();
    const int gaiRc = getaddrinfo(node, portStr.c_str(), &hints, &result);
    if (gaiRc != 0) {
        TE_LOG_WARNING << "getaddrinfo failed"
                       << ", host=" << host << ", port=" << port << ", gai_rc=" << gaiRc
                       << ", reason=" << gai_strerror(gaiRc);
        return TE_MAKE_STATUS(ErrorCode::kInvalid, std::string("resolve tcp address failed: ") + gai_strerror(gaiRc));
    }

    out->reset(result);
    return Result::OK();
}

std::string NormalizeBindHost(const std::string &host)
{
    if (host.empty() || host == "*") {
        return "";
    }
    return host;
}

void SetCommonSocketOptions(int fd, int family)
{
    int opt = 1;
    (void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    if (family == AF_INET6) {
        (void)setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &opt, sizeof(opt));
    }
}

Result CreateSocketFromAddr(const addrinfo &addr, int &fd)
{
    fd = ::socket(addr.ai_family, addr.ai_socktype, addr.ai_protocol);
    TE_CHECK_OR_RETURN(fd >= 0, ErrorCode::kRuntimeError, "create socket failed");
    SetCommonSocketOptions(fd, addr.ai_family);
    return Result::OK();
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

Result ConnectTo(const std::string &host, uint16_t port, int *fd)
{
    TE_CHECK_PTR_OR_RETURN(fd);
    *fd = -1;

    AddrInfoPtr result;
    TE_RETURN_IF_ERROR(ResolveTcpAddr(host, port, 0, &result));

    int sock = -1;
    for (auto *it = result.get(); it != nullptr; it = it->ai_next) {
        Result socketRc = CreateSocketFromAddr(*it, sock);
        if (socketRc.IsError()) {
            continue;
        }
        if (::connect(sock, it->ai_addr, it->ai_addrlen) == 0) {
            *fd = sock;
            return Result::OK();
        }
        ::close(sock);
        sock = -1;
    }
    TE_LOG_WARNING << "connect failed, host=" << host << ", port=" << port;
    return TE_MAKE_STATUS(ErrorCode::kRuntimeError, "connect failed");
}

namespace {

void LogListenSocketFailure(ListenSocketFailureLogLevel failureLogLevel, const char *operation,
                            const std::string &host, uint16_t port, int errorNo)
{
    if (failureLogLevel == ListenSocketFailureLogLevel::kVlog1) {
        TE_VLOG_1 << operation << " failed, host=" << host << ", port=" << port << ", errno=" << errorNo;
        return;
    }
    TE_LOG_ERROR << operation << " failed, host=" << host << ", port=" << port << ", errno=" << errorNo;
}

}  // namespace

Result CreateListenSocket(const std::string &host, uint16_t port, int backlog, int &listenFd,
                          ListenSocketFailureLogLevel failureLogLevel)
{
    listenFd = -1;
    TE_CHECK_OR_RETURN(backlog > 0, ErrorCode::kInvalid, "backlog should be positive");

    const std::string bindHost = NormalizeBindHost(host);
    AddrInfoPtr result;
    TE_RETURN_IF_ERROR(ResolveTcpAddr(bindHost, port, AI_PASSIVE, &result));

    int lastErrno = 0;
    for (auto *it = result.get(); it != nullptr; it = it->ai_next) {
        int fd = -1;
        Result socketRc = CreateSocketFromAddr(*it, fd);
        if (socketRc.IsError()) {
            lastErrno = errno;
            continue;
        }
        if (::bind(fd, it->ai_addr, it->ai_addrlen) != 0) {
            lastErrno = errno;
            LogListenSocketFailure(failureLogLevel, "bind", bindHost, port, lastErrno);
            ::close(fd);
            continue;
        }
        if (::listen(fd, backlog) != 0) {
            lastErrno = errno;
            LogListenSocketFailure(failureLogLevel, "listen", bindHost, port, lastErrno);
            ::close(fd);
            continue;
        }
        listenFd = fd;
        return Result::OK();
    }

    return TE_MAKE_STATUS(ErrorCode::kRuntimeError, "listen failed");
}

Result SetSocketTimeoutSec(int fd, int timeoutSec)
{
    TE_CHECK_OR_RETURN(fd >= 0, ErrorCode::kInvalid, "invalid fd");
    TE_CHECK_OR_RETURN(timeoutSec > 0, ErrorCode::kInvalid, "timeout should be positive");
    timeval timeout {};
    timeout.tv_sec = timeoutSec;
    timeout.tv_usec = 0;
    (void)setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    (void)setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    return Result::OK();
}

Result SendFrame(int fd, RpcMethod method, const std::vector<uint8_t> &payload)
{
    std::vector<uint8_t> frame;
    frame.reserve(4 + 1 + 4 + payload.size());
    WriteU32Be(kMagic, &frame);
    frame.push_back(static_cast<uint8_t>(method));
    WriteU32Be(static_cast<uint32_t>(payload.size()), &frame);
    frame.insert(frame.end(), payload.begin(), payload.end());
    if (!WriteExact(fd, frame.data(), frame.size())) {
        return TE_MAKE_STATUS(ErrorCode::kRuntimeError, "send frame failed");
    }
    return Result::OK();
}

Result RecvFrame(int fd, RpcMethod *method, std::vector<uint8_t> *payload)
{
    TE_CHECK_PTR_OR_RETURN(method);
    TE_CHECK_PTR_OR_RETURN(payload);

    uint8_t header[9] = {0};
    if (!ReadExact(fd, header, sizeof(header))) {
        return TE_MAKE_STATUS(ErrorCode::kRuntimeError, "read frame header failed");
    }
    const uint32_t magic = ReadU32Be(header);
    if (magic != kMagic) {
        return TE_MAKE_STATUS(ErrorCode::kRuntimeError, "invalid frame magic");
    }
    *method = static_cast<RpcMethod>(header[4]);
    const uint32_t payloadLen = ReadU32Be(header + 5);

    payload->assign(payloadLen, 0);
    if (payloadLen > 0 && !ReadExact(fd, payload->data(), payloadLen)) {
        return TE_MAKE_STATUS(ErrorCode::kRuntimeError, "read frame payload failed");
    }
    return Result::OK();
}

Result InvokeRpc(const std::string &host, uint16_t port, RpcMethod expectedMethod,
                 const std::vector<uint8_t> &reqPayload, std::vector<uint8_t> *rspPayload)
{
    TE_CHECK_PTR_OR_RETURN(rspPayload);
    int fd = -1;
    TE_RETURN_IF_ERROR(ConnectTo(host, port, &fd));
    ScopedFd scopedFd(fd);

    TE_RETURN_IF_ERROR(SendFrame(scopedFd.Get(), expectedMethod, reqPayload));
    RpcMethod method;
    TE_RETURN_IF_ERROR(RecvFrame(scopedFd.Get(), &method, rspPayload));
    if (method != expectedMethod) {
        TE_LOG_ERROR << "rpc method mismatch, expected=" << static_cast<int>(expectedMethod)
                   << ", actual=" << static_cast<int>(method);
        return TE_MAKE_STATUS(ErrorCode::kRuntimeError, "rpc method mismatch");
    }
    return Result::OK();
}

}  // namespace datasystem
