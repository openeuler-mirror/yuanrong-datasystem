/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */

#ifndef TRANSFER_ENGINE_INTERNAL_ASCEND_BACKEND_H
#define TRANSFER_ENGINE_INTERNAL_ASCEND_BACKEND_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "internal/backend/ascend/hixl_config.h"
#include "datasystem/transfer_engine/data_plane_backend.h"

namespace datasystem {

class AscendBackend final : public IDataPlaneBackend {
public:
    AscendBackend();
    ~AscendBackend() override;

    bool RequiresAclRuntime() const override { return true; }
    std::string BackendKind() const override { return "ascend"; }
    std::string RoutePolicy() const override;
    uint64_t MemoryGeneration() const override;
    bool SupportsReceiverDrivenRead() const override { return true; }
    uint64_t ReadLeaseTtlMs() const override;
    bool IsConnectionReady(const ConnectionSpec &spec) const override;

    Result InitializeLocal(const std::string &localHost, uint16_t localPort, int32_t localDeviceId) override;
    void FinalizeLocal() override;
    Result RegisterLocalMemory(uint64_t addr, uint64_t length) override;
    Result UnregisterLocalMemory(uint64_t addr, uint64_t length) override;
    Result PrepareReadDestinations(const std::vector<TransferMemoryRegion> &regions) override;

    Result CreateRootInfo(std::string *rootInfoBytes) override;
    Result InitRecv(const ConnectionSpec &spec, const std::string &rootInfoBytes) override;
    Result InitSend(const ConnectionSpec &spec, const std::string &rootInfoBytes) override;
    Result TransferSyncRead(const ConnectionSpec &spec, const std::vector<TransferReadOp> &ops,
                            uint64_t timeoutMs) override;
    void AbortConnection(const ConnectionSpec &spec) override;
    void InvalidatePeerConnections(const ConnectionSpec &spec) override;

private:
    struct Impl;
    struct RegisteredMem {
        uint64_t addr = 0;
        uint64_t length = 0;
        void *handle = nullptr;
    };
    static std::string ConnectionKey(const ConnectionSpec &spec);
    static Result ParseRoutePolicy(std::string *routePolicy);
    static Result BuildEndpoint(const std::string &localHost, int32_t localDeviceId, std::string *endpoint);
    static int32_t GetEnvI32(const char *name, int32_t defaultValue);

    Result RegisterOneLocked(uint64_t addr, uint64_t length, bool *registeredNew);
    Result ValidateBackingAlignmentLocked(uint64_t addr, uint64_t length) const;
    Result UnregisterOneLocked(uint64_t addr, uint64_t length, bool failIfMissing, bool *unregistered = nullptr);
    bool HasEndpointReferenceLocked(const std::string &endpoint) const;
    void EraseEndpointReferencesLocked(const std::string &endpoint);
    bool DisconnectEndpointLocked(const std::string &endpoint, const std::string &where);
    Result CleanupPendingEndpointLocked(const std::string &endpoint);
    Result EnsureEndpointCapacityLocked(const std::string &endpoint);
    Result ConnectEndpointLocked(const std::string &connectionKey, const std::string &endpoint);
    void InvalidateEndpointLocked(const std::string &endpoint, const std::string &where);
    Result DisconnectAllLocked();
    Result ConnectLocked(const std::string &connectionKey, const std::string &endpoint);
    Result TransferReadBatchLocked(const ConnectionSpec &spec, const std::vector<TransferReadOp> &ops, size_t base,
                                   size_t end, const std::string &endpoint, uint64_t timeoutMs);

    std::unique_ptr<Impl> impl_;
    mutable std::mutex mutex_;
    std::unordered_map<uint64_t, RegisteredMem> registeredMems_;
    std::unordered_map<std::string, std::string> peerEndpointByConnection_;
    std::unordered_set<std::string> connectedEndpoints_;
    std::unordered_set<std::string> cleanupPendingEndpoints_;
    int32_t localDeviceId_ = -1;
    std::string hixlEndpoint_;
    std::string routePolicy_ = K_DEFAULT_HIXL_ROUTE;
    HixlEngineMode engineMode_ = HixlEngineMode::kLegacy;
    bool autoConnectEnabled_ = false;
    uint64_t memGeneration_ = 0;
    int32_t connectTimeoutMs_ = 10000;
    int32_t transferTimeoutMs_ = 10000;
    int32_t readLeaseTtlMs_ = static_cast<int32_t>(K_DEFAULT_READ_LEASE_TTL_MS);
};

}  // namespace datasystem

#endif  // TRANSFER_ENGINE_INTERNAL_ASCEND_BACKEND_H
