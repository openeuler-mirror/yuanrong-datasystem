#pragma once

#include <gmock/gmock.h>

#include "datasystem/common/rdma/ucp_manager.h"

namespace datasystem {
class MockUcpManager : public UcpManager {
public:
    MockUcpManager() = default;
    virtual ~MockUcpManager() = default;

    MOCK_METHOD(void, InsertSuccessfulEvent, (uint64_t requestId), (override));
    MOCK_METHOD(void, InsertFailedEvent, (uint64_t requestId), (override));
};
}  // namespace datasystem