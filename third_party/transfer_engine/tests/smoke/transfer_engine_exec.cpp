#include <cstdlib>
#include <vector>

#include <glog/logging.h>

#include "internal/backend/mock_data_plane_backend.h"
#include "internal/log/logging.h"
#include "datasystem/transfer_engine/transfer_engine.h"

int main()
{
    datasystem::internal::EnsureGlogInitialized();
    // 中文说明：该可执行用例用于在无 gtest 场景下验证 MockDataPlaneBackend 端到端读流程。
    auto ownerBackend = std::make_shared<datasystem::MockDataPlaneBackend>();
    auto requesterBackend = std::make_shared<datasystem::MockDataPlaneBackend>();

    datasystem::TransferEngine owner(ownerBackend);
    datasystem::TransferEngine requester(requesterBackend);

    auto rc = owner.Initialize("127.0.0.1:63051", "ascend", "npu:0");
    if (rc.IsError()) {
        LOG(ERROR) << "owner initialize failed: " << rc.GetMsg();
        return EXIT_FAILURE;
    }

    rc = requester.Initialize("127.0.0.1:63052", "ascend", "npu:0");
    if (rc.IsError()) {
        LOG(ERROR) << "requester initialize failed: " << rc.GetMsg();
        return EXIT_FAILURE;
    }

    std::vector<uint8_t> src(128);
    std::vector<uint8_t> dst(128, 0);
    for (size_t i = 0; i < src.size(); ++i) {
        src[i] = static_cast<uint8_t>((i * 3) & 0xFF);
    }

    rc = owner.RegisterMemory(reinterpret_cast<uintptr_t>(src.data()), src.size());
    if (rc.IsError()) {
        LOG(ERROR) << "register memory failed: " << rc.GetMsg();
        return EXIT_FAILURE;
    }

    rc = requester.BatchTransferSyncRead(
        "127.0.0.1:63051",
        {reinterpret_cast<uintptr_t>(dst.data())},
        {reinterpret_cast<uintptr_t>(src.data())},
        {dst.size()});
    if (rc.IsError()) {
        LOG(ERROR) << "transfer_sync_read failed: " << rc.GetMsg();
        return EXIT_FAILURE;
    }

    if (src != dst) {
        LOG(ERROR) << "data mismatch after transfer_sync_read";
        return EXIT_FAILURE;
    }

    LOG(INFO) << "transfer_engine_exec: PASS";
    return EXIT_SUCCESS;
}
