#include "internal/backend/mock_data_plane_backend.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <utility>

#include "datasystem/transfer_engine/status_helper.h"

namespace datasystem {

MockDataPlaneBackend::MockDataPlaneBackend() : sharedState_(std::make_shared<SharedState>()) {}

MockDataPlaneBackend::MockDataPlaneBackend(std::shared_ptr<SharedState> sharedState)
    : sharedState_(std::move(sharedState))
{
    if (sharedState_ == nullptr) {
        sharedState_ = std::make_shared<SharedState>();
    }
}

Status MockDataPlaneBackend::CreateRootInfo(std::string *rootInfoBytes)
{
    TE_CHECK_PTR_OR_RETURN(rootInfoBytes);
    *rootInfoBytes = "mock_root_info";
    return Status::OK();
}

Status MockDataPlaneBackend::InitRecv(const ConnectionSpec &spec, const std::string &rootInfoBytes)
{
    (void)spec;
    TE_CHECK_OR_RETURN(!rootInfoBytes.empty(), StatusCode::kInvalid, "empty root info");
    return Status::OK();
}

Status MockDataPlaneBackend::InitSend(const ConnectionSpec &spec, const std::string &rootInfoBytes)
{
    (void)spec;
    TE_CHECK_OR_RETURN(!rootInfoBytes.empty(), StatusCode::kInvalid, "empty root info");
    return Status::OK();
}

Status MockDataPlaneBackend::PostRecv(const ConnectionSpec &spec, uint64_t localAddr, uint64_t length)
{
    TE_CHECK_OR_RETURN(localAddr > 0 && length > 0, StatusCode::kInvalid, "invalid recv args");
    std::lock_guard<std::mutex> lock(sharedState_->mutex);
    sharedState_->pendingRecv[RecvKey(spec)].push_back(PendingRecv{ localAddr, length, false });
    return Status::OK();
}

Status MockDataPlaneBackend::PostSend(const ConnectionSpec &spec, uint64_t remoteAddr, uint64_t length)
{
    TE_CHECK_OR_RETURN(remoteAddr > 0 && length > 0, StatusCode::kInvalid, "invalid send args");

    std::lock_guard<std::mutex> lock(sharedState_->mutex);
    const std::string key = SendToRecvKey(spec);
    auto iter = sharedState_->pendingRecv.find(key);
    TE_CHECK_OR_RETURN(iter != sharedState_->pendingRecv.end() && !iter->second.empty(),
                       StatusCode::kNotReady, "no pending recv found");
    auto pendingIter = std::find_if(iter->second.begin(), iter->second.end(), [](const PendingRecv &slot) {
        return !slot.completed;
    });
    TE_CHECK_OR_RETURN(pendingIter != iter->second.end(), StatusCode::kNotReady, "all pending recv are already completed");
    auto &pending = *pendingIter;
    TE_CHECK_OR_RETURN(pending.length == length, StatusCode::kInvalid, "length mismatch");

    std::memcpy(reinterpret_cast<void *>(pending.localAddr), reinterpret_cast<void *>(remoteAddr), length);
    pending.completed = true;
    sharedState_->cv.notify_all();
    return Status::OK();
}

Status MockDataPlaneBackend::WaitRecv(const ConnectionSpec &spec, uint64_t timeoutMs)
{
    const std::string key = RecvKey(spec);
    std::unique_lock<std::mutex> lock(sharedState_->mutex);
    auto ready = [&]() {
        auto it = sharedState_->pendingRecv.find(key);
        return it != sharedState_->pendingRecv.end() && !it->second.empty() && it->second.front().completed;
    };

    if (!sharedState_->cv.wait_for(lock, std::chrono::milliseconds(timeoutMs), ready)) {
        return TE_MAKE_STATUS(StatusCode::kNotReady, "wait recv timeout");
    }
    auto it = sharedState_->pendingRecv.find(key);
    if (it != sharedState_->pendingRecv.end() && !it->second.empty()) {
        it->second.pop_front();
        if (it->second.empty()) {
            sharedState_->pendingRecv.erase(it);
        }
    }
    return Status::OK();
}

void MockDataPlaneBackend::AbortConnection(const ConnectionSpec &spec)
{
    std::lock_guard<std::mutex> lock(sharedState_->mutex);
    sharedState_->pendingRecv.erase(RecvKey(spec));
}

std::string MockDataPlaneBackend::RecvKey(const ConnectionSpec &spec)
{
    return spec.localHost + ":" + std::to_string(spec.localPort) + ":" + std::to_string(spec.localDeviceId) + "|" +
           spec.peerHost + ":" + std::to_string(spec.peerPort) + ":" + std::to_string(spec.peerDeviceId);
}

std::string MockDataPlaneBackend::SendToRecvKey(const ConnectionSpec &spec)
{
    return spec.peerHost + ":" + std::to_string(spec.peerPort) + ":" + std::to_string(spec.peerDeviceId) + "|" +
           spec.localHost + ":" + std::to_string(spec.localPort) + ":" + std::to_string(spec.localDeviceId);
}

}  // namespace datasystem
