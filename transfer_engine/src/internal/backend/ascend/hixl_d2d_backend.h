/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */

#ifndef TRANSFER_ENGINE_INTERNAL_HIXL_D2D_BACKEND_H
#define TRANSFER_ENGINE_INTERNAL_HIXL_D2D_BACKEND_H

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "datasystem/transfer_engine/data_plane_backend.h"

namespace datasystem {

class HixlD2DBackend final : public IDataPlaneBackend {
public:
    HixlD2DBackend();
    ~HixlD2DBackend() override;

    bool RequiresAclRuntime() const override { return true; }
    std::string BackendKind() const override { return "hixl"; }
    std::string RoutePolicy() const override;
    uint64_t MemoryGeneration() const override;
    bool SupportsReceiverDrivenRead() const override { return true; }

    Result InitializeLocal(const std::string &localHost, uint16_t localPort, int32_t localDeviceId) override;
    void FinalizeLocal() override;
    Result RegisterLocalMemory(uint64_t addr, uint64_t length) override;
    Result UnregisterLocalMemory(uint64_t addr, uint64_t length) override;
    Result PrepareReadDestinations(const std::vector<TransferMemoryRegion> &regions) override;

    Result CreateRootInfo(std::string *rootInfoBytes) override;
    Result InitRecv(const ConnectionSpec &spec, const std::string &rootInfoBytes) override;
    Result InitSend(const ConnectionSpec &spec, const std::string &rootInfoBytes) override;
    Result PostRecv(const ConnectionSpec &spec, uint64_t localAddr, uint64_t length) override;
    Result PostSend(const ConnectionSpec &spec, uint64_t remoteAddr, uint64_t length) override;
    Result WaitRecv(const ConnectionSpec &spec, uint64_t timeoutMs) override;
    Result TransferSyncRead(const ConnectionSpec &spec, const std::vector<TransferReadOp> &ops,
                            uint64_t timeoutMs) override;
    void AbortConnection(const ConnectionSpec &spec) override;

private:
    struct Impl;
    struct RegisteredMem {
        uint64_t addr = 0;
        uint64_t length = 0;
        void *handle = nullptr;
    };
    struct RootInfo {
        std::string backendKind;
        std::string endpoint;
        std::string routePolicy;
    };

    static std::string ConnectionKey(const ConnectionSpec &spec);
    static Result ParseRootInfo(const std::string &rootInfoBytes, RootInfo *rootInfo);
    static std::string EncodeRootInfo(const RootInfo &rootInfo);
    static Result ParseRoutePolicy(std::string *routePolicy);
    static Result BuildEndpoint(const std::string &localHost, int32_t localDeviceId, std::string *endpoint);
    static int32_t GetEnvI32(const char *name, int32_t defaultValue);

    Result RegisterOneLocked(uint64_t addr, uint64_t length, bool *registeredNew);
    Result UnregisterOneLocked(uint64_t addr, uint64_t length, bool failIfMissing, bool *unregistered = nullptr);
    void DisconnectAllLocked();
    Result ConnectLocked(const std::string &connectionKey, const std::string &endpoint);

    std::unique_ptr<Impl> impl_;
    mutable std::mutex mutex_;
    std::unordered_map<uint64_t, RegisteredMem> registeredMems_;
    std::unordered_map<std::string, std::string> peerEndpointByConnection_;
    std::unordered_set<std::string> connectedEndpoints_;
    int32_t localDeviceId_ = -1;
    std::string hixlEndpoint_;
    std::string routePolicy_ = "auto";
    uint64_t memGeneration_ = 0;
    int32_t connectTimeoutMs_ = 10000;
    int32_t transferTimeoutMs_ = 10000;
};

}  // namespace datasystem

#endif  // TRANSFER_ENGINE_INTERNAL_HIXL_D2D_BACKEND_H
