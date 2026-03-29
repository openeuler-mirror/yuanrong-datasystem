#ifndef TRANSFER_ENGINE_INTERNAL_SOCKET_RPC_TRANSPORT_H
#define TRANSFER_ENGINE_INTERNAL_SOCKET_RPC_TRANSPORT_H

#include <cstdint>
#include <string>
#include <vector>

#include "internal/control_plane/control_plane_codec.h"
#include "datasystem/transfer_engine/status.h"

namespace datasystem {

class ScopedFd {
public:
    ScopedFd();
    explicit ScopedFd(int fd);
    ~ScopedFd();

    ScopedFd(const ScopedFd &) = delete;
    ScopedFd &operator=(const ScopedFd &) = delete;
    ScopedFd(ScopedFd &&other) noexcept;
    ScopedFd &operator=(ScopedFd &&other) noexcept;

    int Get() const;

private:
    int fd_ = -1;
};

Result ConnectTo(const std::string &host, uint16_t port, int *fd);
Result CreateListenSocket(const std::string &host, uint16_t port, int backlog, int *listenFd);
Result SetSocketTimeoutSec(int fd, int timeoutSec);

Result SendFrame(int fd, RpcMethod method, const std::vector<uint8_t> &payload);
Result RecvFrame(int fd, RpcMethod *method, std::vector<uint8_t> *payload);
Result InvokeRpc(const std::string &host, uint16_t port, RpcMethod expectedMethod, const std::vector<uint8_t> &reqPayload,
                 std::vector<uint8_t> *rspPayload);

}  // namespace datasystem

#endif  // TRANSFER_ENGINE_INTERNAL_SOCKET_RPC_TRANSPORT_H
