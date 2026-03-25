#ifndef TRANSFER_ENGINE_CONTROL_PLANE_MESSAGES_H
#define TRANSFER_ENGINE_CONTROL_PLANE_MESSAGES_H

#include <cstdint>
#include <string>
#include <vector>

namespace datasystem {

struct ExchangeRootInfoRequest {
    std::string requesterHost;
    uint32_t requesterPort = 0;
    int32_t requesterDeviceId = -1;
    int32_t ownerDeviceId = -1;
    std::string rootInfo;
};

struct ExchangeRootInfoResponse {
    int32_t code = 0;
    std::string msg;
    int32_t ownerDeviceId = -1;
};

struct QueryConnReadyRequest {
    std::string requesterHost;
    uint32_t requesterPort = 0;
    int32_t requesterDeviceId = -1;
    int32_t ownerDeviceId = -1;
};

struct QueryConnReadyResponse {
    int32_t code = 0;
    std::string msg;
    bool ready = false;
};

struct ReadTriggerRequest {
    uint64_t requestId = 0;
    std::string requesterHost;
    uint32_t requesterPort = 0;
    int32_t requesterDeviceId = -1;
    int32_t ownerDeviceId = -1;
    uint64_t remoteAddr = 0;
    uint64_t length = 0;
};

struct ReadTriggerResponse {
    int32_t code = 0;
    std::string msg;
};

struct BatchReadItem {
    uint64_t requestId = 0;
    uint64_t remoteAddr = 0;
    uint64_t length = 0;
};

struct BatchReadTriggerRequest {
    std::string requesterHost;
    uint32_t requesterPort = 0;
    int32_t requesterDeviceId = -1;
    int32_t ownerDeviceId = -1;
    std::vector<BatchReadItem> items;
};

struct BatchReadTriggerResponse {
    int32_t code = 0;
    std::string msg;
    int32_t failedItemIndex = -1;
};

}  // namespace datasystem

#endif  // TRANSFER_ENGINE_CONTROL_PLANE_MESSAGES_H
