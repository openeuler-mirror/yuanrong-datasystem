#ifndef TRANSFER_ENGINE_DATA_PLANE_BACKEND_H
#define TRANSFER_ENGINE_DATA_PLANE_BACKEND_H

#include <cstdint>
#include <string>

#include "datasystem/transfer_engine/status.h"

namespace datasystem {

struct ConnectionSpec {
    std::string localHost;
    uint16_t localPort = 0;
    int32_t localDeviceId = -1;
    std::string peerHost;
    uint16_t peerPort = 0;
    int32_t peerDeviceId = -1;
};

class IDataPlaneBackend {
public:
    virtual ~IDataPlaneBackend() = default;
    virtual bool RequiresAclRuntime() const { return true; }

    virtual Status CreateRootInfo(std::string *rootInfoBytes) = 0;
    virtual Status InitRecv(const ConnectionSpec &spec, const std::string &rootInfoBytes) = 0;
    virtual Status InitSend(const ConnectionSpec &spec, const std::string &rootInfoBytes) = 0;
    virtual Status PostRecv(const ConnectionSpec &spec, uint64_t localAddr, uint64_t length) = 0;
    virtual Status PostSend(const ConnectionSpec &spec, uint64_t remoteAddr, uint64_t length) = 0;
    virtual Status WaitRecv(const ConnectionSpec &spec, uint64_t timeoutMs) = 0;
    virtual void AbortConnection(const ConnectionSpec &spec) { (void)spec; }
};

}  // namespace datasystem

#endif  // TRANSFER_ENGINE_DATA_PLANE_BACKEND_H
