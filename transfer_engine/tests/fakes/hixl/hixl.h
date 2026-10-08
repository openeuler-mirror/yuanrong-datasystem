/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */
#ifndef TRANSFER_ENGINE_TESTS_FAKE_HIXL_H
#define TRANSFER_ENGINE_TESTS_FAKE_HIXL_H

#include <cstdint>
#include <map>
#include <string>
#include <unordered_set>
#include <vector>

namespace hixl {

enum Status { SUCCESS, PARAM_INVALID, TIMEOUT, NOT_CONNECTED, RESOURCE_EXHAUSTED, UNSUPPORTED, ALREADY_CONNECTED };
enum MemType { MEM_DEVICE };
enum TransferType { READ };
using AscendString = std::string;
using MemHandle = void *;
inline constexpr char OPTION_BUFFER_POOL[] = "BufferPool";
inline constexpr char OPTION_RDMA_TRAFFIC_CLASS[] = "RdmaTrafficClass";
inline constexpr char OPTION_RDMA_SERVICE_LEVEL[] = "RdmaServiceLevel";

struct MemDesc {
    uintptr_t addr = 0;
    size_t len = 0;
};

struct TransferOpDesc {
    uintptr_t local_addr = 0;
    uintptr_t remote_addr = 0;
    size_t len = 0;
};

struct FakeState {
    Status disconnectResult = SUCCESS;
    Status transferResult = SUCCESS;
    Status connectResult = SUCCESS;
    size_t connectCalls = 0;
    size_t disconnectCalls = 0;
    size_t transferCalls = 0;
    std::unordered_set<std::string> connections;
};

inline FakeState fake;

class Hixl {
public:
    Hixl() = default;
    ~Hixl() = default;

    Status Initialize(const AscendString &, const std::map<AscendString, AscendString> &options)
    {
        auto iter = options.find("AutoConnect");
        autoConnect_ = iter != options.end() && iter->second == "1";
        return SUCCESS;
    }

    void Finalize()
    {
        fake.connections.clear();
    }

    Status Connect(const AscendString &endpoint, int32_t)
    {
        ++fake.connectCalls;
        if (fake.connectResult != SUCCESS) {
            return fake.connectResult;
        }
        return fake.connections.insert(endpoint).second ? SUCCESS : ALREADY_CONNECTED;
    }

    Status Disconnect(const AscendString &endpoint, int32_t)
    {
        ++fake.disconnectCalls;
        if (fake.disconnectResult != SUCCESS) {
            return fake.disconnectResult;
        }
        return fake.connections.erase(endpoint) != 0 ? SUCCESS : NOT_CONNECTED;
    }

    Status TransferSync(const AscendString &endpoint, TransferType, const std::vector<TransferOpDesc> &, int32_t)
    {
        ++fake.transferCalls;
        if (autoConnect_) {
            fake.connections.insert(endpoint);
        }
        if (fake.connections.count(endpoint) == 0) {
            return NOT_CONNECTED;
        }
        if (autoConnect_ && fake.transferResult != SUCCESS) {
            fake.connections.erase(endpoint);
        }
        return fake.transferResult;
    }

    Status RegisterMem(const MemDesc &desc, MemType, MemHandle &handle)
    {
        handle = reinterpret_cast<void *>(desc.addr);
        return SUCCESS;
    }

    Status DeregisterMem(MemHandle)
    {
        return SUCCESS;
    }

private:
    bool autoConnect_ = false;
};

}  // namespace hixl

#endif
