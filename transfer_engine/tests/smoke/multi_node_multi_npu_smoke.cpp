#include <cstdlib>
#include <vector>

#include "internal/backend/mock_data_plane_backend.h"
#include "internal/log/logging.h"
#include "datasystem/transfer_engine/transfer_engine.h"

int main()
{
    datasystem::internal::InitializeLogging();

    // 中文说明：该 smoke 用例在“多节点端点”仿真下，验�?owner 同进程注册多 npu_id 内存并被 requester 读取�?
    auto sharedState = std::make_shared<datasystem::MockDataPlaneBackend::SharedState>();
    auto ownerBackend = std::make_shared<datasystem::MockDataPlaneBackend>(sharedState);
    auto requesterBackend = std::make_shared<datasystem::MockDataPlaneBackend>(sharedState);

    datasystem::TransferEngine owner(ownerBackend);
    datasystem::TransferEngine requester(requesterBackend);

    auto rc = owner.Initialize("127.0.0.1:65051", "ascend", "npu:0");
    if (rc.IsError()) {
        TE_LOG_ERROR << "owner initialize failed: " << rc.ToString();
        return EXIT_FAILURE;
    }
    rc = requester.Initialize("127.0.0.1:65052", "ascend", "npu:2");
    if (rc.IsError()) {
        TE_LOG_ERROR << "requester initialize failed: " << rc.ToString();
        return EXIT_FAILURE;
    }

    std::vector<uint8_t> srcNpu0(256, 0x3C);
    std::vector<uint8_t> srcNpu1(256, 0x4D);
    std::vector<uint8_t> dstNpu0(256, 0);
    std::vector<uint8_t> dstNpu1(256, 0);

    rc = owner.RegisterMemory(reinterpret_cast<uintptr_t>(srcNpu0.data()), srcNpu0.size());
    if (rc.IsError()) {
        TE_LOG_ERROR << "register npu0 memory failed: " << rc.ToString();
        return EXIT_FAILURE;
    }
    rc = owner.RegisterMemory(reinterpret_cast<uintptr_t>(srcNpu1.data()), srcNpu1.size());
    if (rc.IsError()) {
        TE_LOG_ERROR << "register npu1 memory failed: " << rc.ToString();
        return EXIT_FAILURE;
    }

    rc = requester.BatchTransferSyncRead(
        "127.0.0.1:65051",
        {reinterpret_cast<uintptr_t>(dstNpu0.data())},
        {reinterpret_cast<uintptr_t>(srcNpu0.data())},
        {dstNpu0.size()});
    if (rc.IsError()) {
        TE_LOG_ERROR << "read npu0 region failed: " << rc.ToString();
        return EXIT_FAILURE;
    }
    rc = requester.BatchTransferSyncRead(
        "127.0.0.1:65051",
        {reinterpret_cast<uintptr_t>(dstNpu1.data())},
        {reinterpret_cast<uintptr_t>(srcNpu1.data())},
        {dstNpu1.size()});
    if (rc.IsError()) {
        TE_LOG_ERROR << "read npu1 region failed: " << rc.ToString();
        return EXIT_FAILURE;
    }

    if (dstNpu0 != srcNpu0 || dstNpu1 != srcNpu1) {
        TE_LOG_ERROR << "data mismatch in multi node multi npu smoke";
        return EXIT_FAILURE;
    }

    (void)requester.Finalize();
    (void)owner.Finalize();
    TE_LOG_INFO << "multi_node_multi_npu_smoke: PASS";
    return EXIT_SUCCESS;
}
