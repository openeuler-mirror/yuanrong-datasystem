#ifndef TRANSFER_ENGINE_INTERNAL_MOCK_DATA_PLANE_BACKEND_H
#define TRANSFER_ENGINE_INTERNAL_MOCK_DATA_PLANE_BACKEND_H

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "datasystem/transfer_engine/data_plane_backend.h"

namespace datasystem {

class MockDataPlaneBackend final : public IDataPlaneBackend {
public:
    struct PendingRecv {
        uint64_t localAddr = 0;
        uint64_t length = 0;
        bool completed = false;
    };

    struct SharedState {
        std::mutex mutex;
        std::condition_variable cv;
        std::unordered_map<std::string, std::deque<PendingRecv>> pendingRecv;
    };

    MockDataPlaneBackend();
    explicit MockDataPlaneBackend(std::shared_ptr<SharedState> sharedState);

    bool RequiresAclRuntime() const override { return false; }
    Result CreateRootInfo(std::string *rootInfoBytes) override;
    Result InitRecv(const ConnectionSpec &spec, const std::string &rootInfoBytes) override;
    Result InitSend(const ConnectionSpec &spec, const std::string &rootInfoBytes) override;
    Result PostRecv(const ConnectionSpec &spec, uint64_t localAddr, uint64_t length) override;
    Result PostSend(const ConnectionSpec &spec, uint64_t remoteAddr, uint64_t length) override;
    Result WaitRecv(const ConnectionSpec &spec, uint64_t timeoutMs) override;
    void AbortConnection(const ConnectionSpec &spec) override;

private:

    static std::string RecvKey(const ConnectionSpec &spec);
    static std::string SendToRecvKey(const ConnectionSpec &spec);

    std::shared_ptr<SharedState> sharedState_;
};

}  // namespace datasystem

#endif  // TRANSFER_ENGINE_INTERNAL_MOCK_DATA_PLANE_BACKEND_H
