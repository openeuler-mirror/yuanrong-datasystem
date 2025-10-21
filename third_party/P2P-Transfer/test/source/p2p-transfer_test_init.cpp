/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include <string>

#include "p2p.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <chrono>
#include <sys/mman.h>

#include <acl/acl.h>
#include <acl/acl_prof.h>
#include <hccl/hccl.h>

#include <cstring>
#include <cstdio>
#include <iomanip>
#include "test-tools/measurementSeries.h"
#include "test-tools/test-error.h"
#include "test-tools/fifo.h"
#include "test-tools/tools.h"
#include "securec.h"

// HCCL can be used using #define USE_HCCL

bool Verify(float *a, int size)
{
    for (int i = 0; i < size; i++) {
        if (a[i] != i) {
            std::cerr << std::fixed << "Verify failed at index " << i << ": " << a[i] << "!=" << i << std::endl;
            return false;
        }
    }
    return true;
}

int SendDeviceLogic(const char *recvSendFifoPath, uint32_t sendDeviceId, size_t bufferSizeBytes, uint32_t recvRank,
                    uint32_t sendRank, int nSamples, pthread_barrier_t *barrier, int nDevices)
{
    int recvSendFd = open(recvSendFifoPath, O_RDONLY);

    NPU_ERROR(aclInit(nullptr));

    // Send device setup
    NPU_ERROR(aclrtSetDevice(sendDeviceId));
    aclrtContext sendDeviceContext;
    NPU_ERROR(aclrtCreateContext(&sendDeviceContext, sendDeviceId));
    NPU_ERROR(aclrtSetCurrentContext(sendDeviceContext));
    aclrtStream stream;
    NPU_ERROR(aclrtCreateStream(&stream));

    // Create send buffer on device
    float *hostBuff;
    NPU_ERROR(aclrtMallocHost((void **)&hostBuff, bufferSizeBytes));
    memset_s(hostBuff, bufferSizeBytes, 0, bufferSizeBytes);
    for (int i = 0; i < int(bufferSizeBytes / sizeof(float)); i += 1) {
        hostBuff[i] = static_cast<float>(i);
    }

    void *dSendBuff;
    NPU_ERROR(aclrtMalloc((void **)&dSendBuff, bufferSizeBytes, ACL_MEM_MALLOC_HUGE_FIRST));
    NPU_ERROR(aclrtMemcpy(dSendBuff, bufferSizeBytes, hostBuff, bufferSizeBytes, ACL_MEMCPY_HOST_TO_DEVICE));

    for (int i = 0; i < nSamples; i++) {
#ifdef USE_HCCL
        HcclRootInfo rootInfo;
        read(recvSendFd, &rootInfo, sizeof(HcclRootInfo));  // Read from fifo

        pthread_barrier_wait(barrier);
        HcclComm hcclComm;
        MEASURE_TIME(NPU_ERROR(HcclCommInitRootInfo(nDevices, &rootInfo, sendRank, &hcclComm)), "Sender Init RootInfo");
#else
        HcclRootInfo rootInfo;
        read(recvSendFd, &rootInfo, sizeof(HcclRootInfo));  // Read from fifo

        NPU_ERROR(PrewarmHccp());

        pthread_barrier_wait(barrier);
        P2PComm p2pComm;
        MEASURE_TIME(NPU_ERROR(P2PCommInitRootInfo(
                         &rootInfo, P2P_SENDER, P2P_LINK_ROCE, &p2pComm)),
                     "Sender P2PCommInitRootInfo");
#endif

// Dummy send
#ifdef USE_HCCL
        NPU_ERROR(
            HcclSend(dSendBuff, bufferSizeBytes / sizeof(float), HCCL_DATA_TYPE_FP32, recvRank, hcclComm, stream));
#else
        NPU_ERROR(P2PSend(dSendBuff, bufferSizeBytes / sizeof(float), HCCL_DATA_TYPE_FP32, p2pComm, stream));
#endif

        NPU_ERROR(aclrtSynchronizeStream(stream));
    }

    close(recvSendFd);
    NPU_ERROR(aclFinalize());
    return 0;
}

int RecvDeviceLogic(const char *recvSendFifoPath, uint32_t recvDeviceId, size_t bufferSizeBytes, uint32_t recvRank,
                    uint32_t sendRank, int nSamples, pthread_barrier_t *barrier, int nDevices)
{
    int recvSendFd = open(recvSendFifoPath, O_WRONLY);

    aclInit(nullptr);

    // Receive device setup
    NPU_ERROR(aclrtSetDevice(recvDeviceId));
    aclrtContext recvDeviceContext;
    NPU_ERROR(aclrtCreateContext(&recvDeviceContext, recvDeviceId));
    NPU_ERROR(aclrtSetCurrentContext(recvDeviceContext));
    aclrtStream stream;
    NPU_ERROR(aclrtCreateStream(&stream));

    float *hostBuff;
    NPU_ERROR(aclrtMallocHost((void **)&hostBuff, bufferSizeBytes));

    void *dRecvBuff;
    NPU_ERROR(aclrtMalloc((void **)&dRecvBuff, bufferSizeBytes, ACL_MEM_MALLOC_HUGE_FIRST));

    for (int i = 0; i < nSamples; i++) {
#ifdef USE_HCCL
        HcclRootInfo rootInfo;
        MEASURE_TIME(NPU_ERROR(HcclGetRootInfo(&rootInfo)), "Receiver HcclGetRootInfo");
        write(recvSendFd, &rootInfo, sizeof(HcclRootInfo));  // Send message to FIFO
#else
        HcclRootInfo rootInfo;
        MEASURE_TIME(NPU_ERROR(P2PGetRootInfo(&rootInfo)), "Receiver P2PGetRootInfo");
        write(recvSendFd, &rootInfo, sizeof(HcclRootInfo));  // Send message to FIFO
#endif

        NPU_ERROR(PrewarmHccp());

        pthread_barrier_wait(barrier);
#ifdef USE_HCCL
        HcclComm hcclComm;
        MEASURE_TIME(NPU_ERROR(HcclCommInitRootInfo(nDevices, &rootInfo, recvRank, &hcclComm)),
                     "Receiver HcclCommInitRootInfo");
#else
        P2PComm p2pComm;
        MEASURE_TIME(NPU_ERROR(P2PCommInitRootInfo(
                         &rootInfo, P2P_RECEIVER, P2P_LINK_ROCE, &p2pComm)),
                     "Receiver P2PCommInitRootInfo");
#endif

// Warmup
#ifdef USE_HCCL
        NPU_ERROR(
            HcclRecv(dRecvBuff, bufferSizeBytes / sizeof(float), HCCL_DATA_TYPE_FP32, sendRank, hcclComm, stream));
#else
        NPU_ERROR(P2PRecv(dRecvBuff, bufferSizeBytes / sizeof(float), HCCL_DATA_TYPE_FP32, p2pComm, stream));
#endif

        NPU_ERROR(aclrtSynchronizeStream(stream));
    }

    close(recvSendFd);
    NPU_ERROR(aclFinalize());
    return 0;
}

void *CreateSharedMemory(size_t size)
{
    // Our memory buffer will be readable and writable:
    int protection = PROT_READ | PROT_WRITE;

    // The buffer will be shared (meaning other processes can access it), but
    // anonymous (meaning third-party processes cannot obtain an address for it),
    // so only this process and its children will be able to use it:
    int visibility = MAP_SHARED | MAP_ANONYMOUS;

    // The remaining parameters to `mmap()` are not important for this use case,
    // but the manpage for `mmap` explains their purpose.
    return mmap(nullptr, size, protection, visibility, -1, 0);
}

int Benchmark()
{
    int nSamples = 1;
    uint32_t sendDeviceId = 3;
    uint32_t recvDeviceId = 4;
    int nDevices = 2;
    size_t bufferSizeBytes = static_cast<size_t>(16) * 1024 * 1024;
    unsigned int modeRw = 0666;

    // Initialize barrier in shared memory
    pthread_barrier_t *barrier = (pthread_barrier_t *)CreateSharedMemory(sizeof(pthread_barrier_t));
    pthread_barrierattr_t barrierAttr;
    pthread_barrierattr_setpshared(&barrierAttr, PTHREAD_PROCESS_SHARED);
    pthread_barrier_init(barrier, &barrierAttr, nDevices);

    const char *recvSendFifoPath = "p2ptest-recvSendFifo";
    Fifo fifo(recvSendFifoPath, modeRw);

    pid_t pid = fork();
    if (pid < 0) {
        std::cerr << "Fork failed!" << std::endl;
        return 1;
    }

    if (pid == 0) {  // Child
        std::cout << "Process " << pid << std::endl;
        SendDeviceLogic(recvSendFifoPath, sendDeviceId, bufferSizeBytes, 0, 1, nSamples, barrier, nDevices);

        return 0;
    } else {  // Parent
        std::cout << "Process " << pid << std::endl;
        RecvDeviceLogic(recvSendFifoPath, recvDeviceId, bufferSizeBytes, 0, 1, nSamples, barrier, nDevices);

        int status;
        waitpid(pid, &status, 0);  // Wait for the child to terminate

        if (WIFEXITED(status)) {
            std::cout << "Child process exited with status " << WEXITSTATUS(status) << std::endl;
        } else {
            std::cerr << "Child process did not terminate normally!" << std::endl;
            return 1;
        }
        return 0;
    }
}

// Note: for now send syncstream returns before receiver has received all data, so best to measure at receiver

auto main() -> int
{
    return Benchmark();
}
