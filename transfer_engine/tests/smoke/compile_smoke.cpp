#include "datasystem/transfer_engine/transfer_engine.h"

int main()
{
    // 中文说明：该冒烟用例仅验证 TransferEngine 公开接口可完成编译与最小调用链路。
    datasystem::TransferEngine engine;
    (void)engine.Initialize("127.0.0.1:60051", "ascend", "npu:0");
    (void)engine.RegisterMemory(0x1000, 256);
    (void)engine.BatchTransferSyncRead("127.0.0.1:60052", {0x2000}, {0x1000}, {128});
    (void)engine.Finalize();
    return 0;
}
