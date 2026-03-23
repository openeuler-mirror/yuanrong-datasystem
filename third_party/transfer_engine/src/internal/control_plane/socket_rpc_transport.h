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

Status ConnectTo(const std::string &host, uint16_t port, int *fd);
Status CreateListenSocket(const std::string &host, uint16_t port, int backlog, int *listenFd);
Status SetSocketTimeoutSec(int fd, int timeoutSec);

Status SendFrame(int fd, RpcMethod method, const std::vector<uint8_t> &payload);
Status RecvFrame(int fd, RpcMethod *method, std::vector<uint8_t> *payload);
Status InvokeRpc(const std::string &host, uint16_t port, RpcMethod expectedMethod, const std::vector<uint8_t> &reqPayload,
                 std::vector<uint8_t> *rspPayload);

}  // namespace datasystem

#endif  // TRANSFER_ENGINE_INTERNAL_SOCKET_RPC_TRANSPORT_H
