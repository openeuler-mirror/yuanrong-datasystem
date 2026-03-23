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

class TransferEngine final {
public:
    TransferEngine();
    explicit TransferEngine(std::shared_ptr<IDataPlaneBackend> backend);
    ~TransferEngine();

    Status Initialize(const std::string &localHostname, const std::string &protocol, const std::string &deviceName);
    int32_t GetRpcPort();
    Status RegisterMemory(uintptr_t bufferAddrRegisrterch, size_t length);
    Status BatchRegisterMemory(const std::vector<uintptr_t> &bufferAddrs, const std::vector<size_t> &lengths);
    Status UnregisterMemory(uintptr_t bufferAddrRegisrterch);
    Status BatchUnregisterMemory(const std::vector<uintptr_t> &bufferAddrs);
    Status TransferSyncRead(const std::string &targetHostname, uintptr_t buffer, uintptr_t peerBufferAddress,
                            size_t length);
    Status BatchTransferSyncRead(const std::string &targetHostname, const std::vector<uintptr_t> &buffers,
                                 const std::vector<uintptr_t> &peerBufferAddresses, const std::vector<size_t> &lengths);
    Status Finalize();

private:
    Status BuildConnectionIfNeeded(const std::string &peerHost, uint16_t peerPort, int32_t *ownerDeviceId);
    Status BuildConnectionOnce(const std::string &peerHost, uint16_t peerPort, int32_t *ownerDeviceId);
    std::string CreateRootInfo() const;

    std::string localHost_;
    uint16_t localPort_ = 0;
    int32_t deviceId_ = -1;
    int32_t rpcThreads_ = 0;
    uint64_t nextRequestId_ = 1;
    bool initialized_ = false;
    bool finalizing_ = false;
    uint64_t inFlightSyncReads_ = 0;

    std::mutex apiMutex_;
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
