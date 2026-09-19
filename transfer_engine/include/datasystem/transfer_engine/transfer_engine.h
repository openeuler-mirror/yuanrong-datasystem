#ifndef TRANSFER_ENGINE_TRANSFER_ENGINE_H
#define TRANSFER_ENGINE_TRANSFER_ENGINE_H

#include <cstdint>
#include <cstddef>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "datasystem/transfer_engine/control_plane_messages.h"
#include "datasystem/transfer_engine/data_plane_backend.h"
#include "datasystem/transfer_engine/status.h"

namespace datasystem {

class ITransferControlClient;
class ITransferControlService;
class SocketControlServer;
class ConnectionManager;
class RegisteredMemoryTable;
class TransferEngineState;

struct MemoryRegistration {
    MemoryRegistration() = default;
    MemoryRegistration(uintptr_t logicalAddrValue, size_t logicalLengthValue, uintptr_t backingAddrValue,
                       size_t backingLengthValue)
        : logicalAddr(logicalAddrValue),
          logicalLength(logicalLengthValue),
          backingAddr(backingAddrValue),
          backingLength(backingLengthValue)
    {
    }

    // Peer-authorized byte range.
    uintptr_t logicalAddr = 0;
    size_t logicalLength = 0;
    // Caller-owned backend range. It must contain the logical range and remain alive until unregistration.
    uintptr_t backingAddr = 0;
    size_t backingLength = 0;
};

class TransferEngine final {
public:
    TransferEngine();
    explicit TransferEngine(std::shared_ptr<IDataPlaneBackend> backend);
    ~TransferEngine();

    /// @param[in] localHostname Local endpoint in host:port form. Port 0 requests an OS-assigned port, optionally
    ///            constrained to the YR_TE_RPC_PORT_MIN/YR_TE_RPC_PORT_MAX range. Read the bound port via GetRpcPort().
    /// @param[in] protocol The only supported value is "ascend" (case-insensitive).
    Result Initialize(const std::string &localHostname, const std::string &protocol, const std::string &deviceName);
    Result Initialize(const std::string &localHostname, const std::string &metadataServer, const std::string &protocol,
                      const std::string &deviceName);
    int32_t GetRpcPort();
    std::string GetRoutePolicy();
    Result RegisterMemory(uintptr_t bufferAddrRegisrterch, size_t length);
    // Registration batches accept at most 4096 logical ranges.
    Result BatchRegisterMemory(const std::vector<uintptr_t> &bufferAddrs, const std::vector<size_t> &lengths);
    Result RegisterMemoryEx(const MemoryRegistration &registration);
    Result BatchRegisterMemoryEx(const std::vector<MemoryRegistration> &registrations);
    Result UnregisterMemory(uintptr_t bufferAddrRegisrterch);
    // Unregistration batches accept at most 4096 logical ranges.
    Result BatchUnregisterMemory(const std::vector<uintptr_t> &bufferAddrs);
    Result TransferSyncRead(const std::string &targetHostname, uintptr_t buffer, uintptr_t peerBufferAddress,
                            size_t length);
    Result BatchTransferSyncRead(const std::string &targetHostname, const std::vector<uintptr_t> &buffers,
                                 const std::vector<uintptr_t> &peerBufferAddresses, const std::vector<size_t> &lengths);
    Result Finalize();

private:
    struct SyncReadContext {
        std::string targetHostname;
        std::string peerHost;
        uint16_t peerPort = 0;
        std::string localHost;
        uint16_t localPort = 0;
        int32_t deviceId = -1;
        uint64_t requestIdStart = 0;
        const std::vector<uintptr_t> *buffers = nullptr;
        const std::vector<uintptr_t> *peerBufferAddresses = nullptr;
        const std::vector<size_t> *lengths = nullptr;
    };

    enum class ReceiverReadOutcome {
        K_DONE,
        K_RETRY,
        K_FAIL,
    };

    Result BuildConnectionIfNeeded(const std::string &peerHost, uint16_t peerPort, int32_t *ownerDeviceId,
                                   uint64_t *ownerMemGeneration);
    Result BuildConnectionOnce(const std::string &peerHost, uint16_t peerPort, int32_t *ownerDeviceId,
                               uint64_t *ownerMemGeneration);
    Result BindControlPortLocked();
    Result InitializeAscendBackendLocked(const std::string &protocol);
    Result StartControlServerLocked();
    bool TryReuseCachedConnection(const std::string &peerHost, uint16_t peerPort, int32_t cachedOwnerDeviceId,
                                  uint64_t cachedOwnerMemGeneration, int32_t *ownerDeviceId,
                                  uint64_t *ownerMemGeneration);
    Result ExchangeRootInfoForConnection(const std::string &peerHost, uint16_t peerPort, const std::string &rootInfo,
                                         ExchangeRootInfoResponse *exchangeRsp);
    Result InitRequesterRecvForConnection(const std::string &peerHost, uint16_t peerPort, const std::string &rootInfo,
                                          const ExchangeRootInfoResponse &exchangeRsp);
    Result WaitOwnerReadyAndCache(const std::string &peerHost, uint16_t peerPort, int32_t ownerDeviceId,
                                  uint64_t *ownerMemGeneration);
    std::string CreateRootInfo() const;

    Result EnterSyncRead(SyncReadContext *ctx);
    void LeaveSyncRead();
    ConnectionSpec BuildReadConnectionSpec(const SyncReadContext &ctx, int32_t ownerDeviceId) const;
    std::vector<TransferReadOp> BuildReadOps(const SyncReadContext &ctx) const;
    void InvalidateReadRoute(const ConnectionSpec &spec, int32_t ownerDeviceId, bool evictEndpointCache);
    void ReleaseReadLeaseQuietly(const SyncReadContext &ctx, const BatchReadTriggerResponse &rsp);
    Result TriggerBatchReadRpc(const SyncReadContext &ctx, const ConnectionSpec &spec, int32_t ownerDeviceId,
                               BatchReadTriggerResponse *rsp);
    ReceiverReadOutcome AttemptReceiverDrivenRead(const SyncReadContext &ctx, int32_t attempt, Result *failRc);
    Result BatchTransferSyncReadReceiverDriven(const SyncReadContext &ctx);
    Result BatchTransferSyncReadLegacy(const SyncReadContext &ctx);
    void MarkBackendDegraded();
    void TeardownEngineState();

    std::string localHost_;
    uint16_t localPort_ = 0;
    int32_t deviceId_ = -1;
    int32_t rpcThreads_ = 0;
    uint64_t nextRequestId_ = 1;
    bool initialized_ = false;
    bool finalizing_ = false;
    bool backendDegraded_ = false;
    bool backendInjected_ = false;
    uint64_t inFlightSyncReads_ = 0;

    std::mutex apiMutex_;
    std::mutex finalizeMutex_;
    std::condition_variable apiCv_;
    std::mutex chainMutex_;
    std::mutex endpointCacheMutex_;

    std::shared_ptr<ConnectionManager> connMgr_;
    std::shared_ptr<RegisteredMemoryTable> registeredMemory_;
    std::shared_ptr<IDataPlaneBackend> backend_;
    std::shared_ptr<ITransferControlService> controlService_;
    std::shared_ptr<ITransferControlClient> controlClient_;
    std::shared_ptr<SocketControlServer> controlServer_;
    std::unique_ptr<TransferEngineState> state_;
};

}  // namespace datasystem

#endif  // TRANSFER_ENGINE_TRANSFER_ENGINE_H
