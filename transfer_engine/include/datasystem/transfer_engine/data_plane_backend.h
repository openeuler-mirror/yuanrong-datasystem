#ifndef TRANSFER_ENGINE_DATA_PLANE_BACKEND_H
#define TRANSFER_ENGINE_DATA_PLANE_BACKEND_H

#include <cstdint>
#include <string>
#include <vector>

#include "datasystem/transfer_engine/status.h"

namespace datasystem {

// Owner-side read leases expire after this long unless a backend reports its own validated value.
constexpr uint64_t K_DEFAULT_READ_LEASE_TTL_MS = 30000;

struct ConnectionSpec {
    std::string localHost;
    uint16_t localPort = 0;
    int32_t localDeviceId = -1;
    std::string peerHost;
    uint16_t peerPort = 0;
    int32_t peerDeviceId = -1;
};

struct TransferMemoryRegion {
    uint64_t addr = 0;
    uint64_t length = 0;
};

struct TransferReadOp {
    uint64_t localAddr = 0;
    uint64_t remoteAddr = 0;
    uint64_t length = 0;
};

class IDataPlaneBackend {
public:
    virtual ~IDataPlaneBackend() = default;
    virtual bool RequiresAclRuntime() const { return true; }
    virtual std::string BackendKind() const { return "ascend"; }
    virtual std::string RoutePolicy() const { return ""; }
    virtual uint64_t MemoryGeneration() const { return 0; }
    virtual bool SupportsReceiverDrivenRead() const { return false; }
    // The owner grants read leases with this TTL. A backend that validates the TTL against its own
    // transfer timeout must report the validated value here, so the granted lease and the checked
    // lease can never disagree.
    virtual uint64_t ReadLeaseTtlMs() const { return K_DEFAULT_READ_LEASE_TTL_MS; }
    virtual bool IsConnectionReady(const ConnectionSpec &spec) const
    {
        (void)spec;
        return true;
    }

    virtual Result InitializeLocal(const std::string &localHost, uint16_t localPort, int32_t localDeviceId)
    {
        (void)localHost;
        (void)localPort;
        (void)localDeviceId;
        return Result::OK();
    }

    virtual void FinalizeLocal() {}

    virtual Result RegisterLocalMemory(uint64_t addr, uint64_t length)
    {
        (void)addr;
        (void)length;
        return Result::OK();
    }

    virtual Result UnregisterLocalMemory(uint64_t addr, uint64_t length)
    {
        (void)addr;
        (void)length;
        return Result::OK();
    }

    // Receiver-driven backends may validate local read destinations before transfer.
    // Destination memory is expected to have been registered through RegisterLocalMemory.
    virtual Result PrepareReadDestinations(const std::vector<TransferMemoryRegion> &regions)
    {
        (void)regions;
        return Result::OK();
    }

    virtual Result CreateRootInfo(std::string *rootInfoBytes) = 0;
    virtual Result InitRecv(const ConnectionSpec &spec, const std::string &rootInfoBytes) = 0;
    virtual Result InitSend(const ConnectionSpec &spec, const std::string &rootInfoBytes) = 0;
    // Sender-driven operations are legacy paths; backends without receiver-driven read may
    // override them, the default is "not supported".
    virtual Result PostRecv(const ConnectionSpec &spec, uint64_t localAddr, uint64_t length)
    {
        (void)spec;
        (void)localAddr;
        (void)length;
        return Result(ErrorCode::kNotSupported, "backend does not support sender-driven operations");
    }
    virtual Result PostSend(const ConnectionSpec &spec, uint64_t remoteAddr, uint64_t length)
    {
        (void)spec;
        (void)remoteAddr;
        (void)length;
        return Result(ErrorCode::kNotSupported, "backend does not support sender-driven operations");
    }
    virtual Result WaitRecv(const ConnectionSpec &spec, uint64_t timeoutMs)
    {
        (void)spec;
        (void)timeoutMs;
        return Result(ErrorCode::kNotSupported, "backend does not support sender-driven operations");
    }
    virtual Result TransferSyncRead(const ConnectionSpec &spec, const std::vector<TransferReadOp> &ops,
                                    uint64_t timeoutMs)
    {
        (void)spec;
        (void)ops;
        (void)timeoutMs;
        return Result(ErrorCode::kNotSupported, "backend does not support receiver-driven read");
    }
    virtual void AbortConnection(const ConnectionSpec &spec) { (void)spec; }
    // A peer incarnation or registration change invalidates every alias of its data-plane endpoint.
    virtual void InvalidatePeerConnections(const ConnectionSpec &spec) { AbortConnection(spec); }
};

}  // namespace datasystem

#endif  // TRANSFER_ENGINE_DATA_PLANE_BACKEND_H
