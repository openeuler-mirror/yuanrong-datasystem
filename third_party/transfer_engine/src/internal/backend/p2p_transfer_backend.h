#ifndef TRANSFER_ENGINE_INTERNAL_P2P_TRANSFER_BACKEND_H
#define TRANSFER_ENGINE_INTERNAL_P2P_TRANSFER_BACKEND_H

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "datasystem/transfer_engine/data_plane_backend.h"

namespace datasystem {

class P2PTransferBackend final : public IDataPlaneBackend {
public:
    P2PTransferBackend();
    ~P2PTransferBackend() override;

    Status CreateRootInfo(std::string *rootInfoBytes) override;
    Status InitRecv(const ConnectionSpec &spec, const std::string &rootInfoBytes) override;
    Status InitSend(const ConnectionSpec &spec, const std::string &rootInfoBytes) override;
    Status PostRecv(const ConnectionSpec &spec, uint64_t localAddr, uint64_t length) override;
    Status PostSend(const ConnectionSpec &spec, uint64_t remoteAddr, uint64_t length) override;
    Status WaitRecv(const ConnectionSpec &spec, uint64_t timeoutMs) override;
    void AbortConnection(const ConnectionSpec &spec) override;

private:
    struct Impl;
    Status EnsureAclContext(int32_t deviceId);
    std::string Key(const ConnectionSpec &spec) const;
    void ResetConnectionLocked(const std::string &key);
    void EnqueueCleanupLocked(const std::string &key);

    std::shared_ptr<Impl> impl_;
    std::mutex mutex_;
};

}  // namespace datasystem

#endif  // TRANSFER_ENGINE_INTERNAL_P2P_TRANSFER_BACKEND_H
