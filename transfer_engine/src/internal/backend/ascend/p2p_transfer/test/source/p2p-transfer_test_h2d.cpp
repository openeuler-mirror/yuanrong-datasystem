/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
#include <runtime/stream.h>
#include <cstring>
#include <cstdio>
#include <iomanip>
#include "test-tools/measurementSeries.h"
#include "test-tools/test-error.h"
#include "test-tools/fifo.h"
#include "test-tools/tools.h"
#include "securec.h"
#include <chrono>
#include <thread>

constexpr size_t BLOCK_SIZE_MULT = 2;
constexpr size_t NUM_MS_IN_SEC = 1000;
constexpr int FRAC_2_PERC = 100;
constexpr int KEEP_0_DECIMAL_PACES = 0;
constexpr int KEEP_2_DECIMAL_PACES = 2;
constexpr int OUTPUT_WIDTH_10 = 10;
constexpr int OUTPUT_WIDTH_7 = 7;
constexpr int SHIFT_BYTES_TO_KB = 10;
constexpr P2pLink LINK_TYPE = P2P_LINK_ROCE;

// define VERIFY
#define USE_BATCH

bool Verify(float *a, int size, float start)
{
    for (int i = 0; i < size; i++) {
        if (a[i] != i + start) {
            std::cerr << std::fixed << "Verify failed at index " << i << ": " << a[i] << "!=" << i + start << std::endl;
            return false;
        }
    }
    return true;
}

int SendDeviceLogic(const char *recvSendFifoPath, uint32_t sendDeviceId, size_t bufferSizeBytes, uint32_t recvRank,
                    uint32_t sendRank, int nSamples, int batchSize, pthread_barrier_t *barrier, int nDevices)
{
    int recvSendFd = open(recvSendFifoPath, O_WRONLY);

    NPU_ERROR(aclInit(nullptr));

    // Send device setup
    NPU_ERROR(aclrtSetDevice(sendDeviceId));
    aclrtContext sendDeviceContext;
    NPU_ERROR(aclrtCreateContext(&sendDeviceContext, sendDeviceId));
    NPU_ERROR(aclrtSetCurrentContext(sendDeviceContext));
    aclrtStream stream;
    NPU_ERROR(rtStreamCreateWithFlags(&stream, 0, RT_STREAM_FAST_SYNC));

    // Create host buffer for remote to read
    float *hostBuff;
    // NPU_ERROR(aclrtMallocHost((void **)&hostBuff, batchSize * bufferSizeBytes)); // might want to test mmap etc.
    hostBuff = (float *)mmap(nullptr, batchSize * bufferSizeBytes, PROT_READ | PROT_WRITE,
                             MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    if (hostBuff == MAP_FAILED) {
        std::cerr << "Mmap failed" << std::endl;
        exit(1);
    }

    memset_s(hostBuff, batchSize * bufferSizeBytes, 0, batchSize * bufferSizeBytes);

    for (int i = 0; i < (batchSize * bufferSizeBytes) / sizeof(float); i++) {
        hostBuff[i] = static_cast<float>(i);
    }

    // Register host memory to NPU for remote reading (can be done anytime)
    HcclRootInfo rootInfo;
    MEASURE_TIME(NPU_ERROR(P2PGetRootInfo(&rootInfo)), "Receiver P2PGetRootInfo");
    write(recvSendFd, &rootInfo, sizeof(HcclRootInfo));  // Send message to FIFO

    P2pSegmentInfo hostSegmentInfo;
    NPU_ERROR(P2PRegisterHostMem(hostBuff, batchSize * bufferSizeBytes, &hostSegmentInfo, P2P_SEGMENT_READ_ONLY));
    write(recvSendFd, &hostSegmentInfo, sizeof(P2pSegmentInfo));  // Send message to FIFO
    write(recvSendFd, &hostBuff, sizeof(float *));                // Send message to FIFO

    pthread_barrier_wait(barrier);
    P2PComm p2pComm;
    MEASURE_TIME(NPU_ERROR(P2PCommInitRootInfo(&rootInfo, P2P_SENDER, LINK_TYPE, &p2pComm)),
                 "Sender P2PCommInitRootInfo");

    // Wait until receiver done reading before exit process
    pthread_barrier_wait(barrier);
    NPU_ERROR(P2PCommDestroy(p2pComm));

    close(recvSendFd);
    NPU_ERROR(aclFinalize());
    return 0;
}

int RecvDeviceLogic(const char *recvSendFifoPath, uint32_t recvDeviceId, size_t bufferSizeBytes, uint32_t recvRank,
                    uint32_t sendRank, int nSamples, int batchSize, pthread_barrier_t *barrier, int nDevices)
{
    int recvSendFd = open(recvSendFifoPath, O_RDONLY);

    NPU_ERROR(aclInit(nullptr));

    // Receive device setup
    NPU_ERROR(aclrtSetDevice(recvDeviceId));

    aclrtContext recvDeviceContext;
    NPU_ERROR(aclrtCreateContext(&recvDeviceContext, recvDeviceId));
    NPU_ERROR(aclrtSetCurrentContext(recvDeviceContext));
    aclrtStream stream;
    NPU_ERROR(rtStreamCreateWithFlags(&stream, 0, RT_STREAM_FAST_SYNC));

    float **hostBuffs = (float **)malloc(batchSize * sizeof(float *));
    for (int i = 0; i < batchSize; i++) {
        NPU_ERROR(aclrtMallocHost((void **)&hostBuffs[i], bufferSizeBytes));
    }

    void **dRecvBuffs = (void **)malloc(batchSize * sizeof(void *));
    for (int i = 0; i < batchSize; i++) {
        NPU_ERROR(aclrtMalloc((void **)&dRecvBuffs[i], bufferSizeBytes, ACL_MEM_MALLOC_HUGE_FIRST));
    }

    HcclRootInfo rootInfo;
    read(recvSendFd, &rootInfo, sizeof(HcclRootInfo));  // Read from fifo

    float *remoteHostBuff;  // might want to allocate multiple hostBuffs, for all
    P2pSegmentInfo remoteHostSegmentInfo;
    read(recvSendFd, &remoteHostSegmentInfo, sizeof(P2pSegmentInfo));  // Read from fifo
    read(recvSendFd, &remoteHostBuff, sizeof(float *));                // Send message to FIFO
    // Register remote host memory to NPU for remote reading (can be done anytime)
    NPU_ERROR(P2PImportHostSegment(remoteHostSegmentInfo));

    pthread_barrier_wait(barrier);

    P2PComm p2pComm;
    std::cout << "Init receiver" << std::endl;
    MEASURE_TIME(NPU_ERROR(P2PCommInitRootInfo(&rootInfo, P2P_RECEIVER, LINK_TYPE, &p2pComm)),
                 "Receiver P2PCommInitRootInfo");

    // Warmup
    for (int i = 0; i < batchSize; i++) {
        NPU_ERROR(P2PGetRemoteHostMem(&remoteHostBuff[i * (bufferSizeBytes / sizeof(float))], dRecvBuffs[i],
                                      bufferSizeBytes / sizeof(float), HCCL_DATA_TYPE_FP32, p2pComm, stream));
    }

    NPU_ERROR(aclrtSynchronizeStream(stream));

    for (int i = 0; i < batchSize; i++) {
        NPU_ERROR(aclrtMemset(dRecvBuffs[i], bufferSizeBytes, 0, bufferSizeBytes));
    }

    // Read
    for (size_t transferSizeBytes = 13 * 1024; transferSizeBytes <= bufferSizeBytes;
         transferSizeBytes *= BLOCK_SIZE_MULT) {
        MeasurementSeries time;

#ifdef USE_BATCH
        uint64_t transferSizes[batchSize];
        for (int i = 0; i < batchSize; i++) {
            transferSizes[i] = transferSizeBytes / sizeof(float);
        }
#endif

        for (int sample = 0; sample < nSamples; sample++) {
            auto t1 = std::chrono::high_resolution_clock::now();

#ifdef USE_BATCH
            NPU_ERROR(P2PScatterFromRemoteHostMem((void *)remoteHostBuff, dRecvBuffs, transferSizes,
                                                  HCCL_DATA_TYPE_FP32, batchSize, p2pComm, stream));
#else
            for (int i = 0; i < batchSize; i++) {
                NPU_ERROR(P2PGetRemoteHostMem(&remoteHostBuff[i * (transferSizeBytes / sizeof(float))], dRecvBuffs[i],
                                              transferSizeBytes / sizeof(float), HCCL_DATA_TYPE_FP32, p2pComm, stream));
            }
#endif

            NPU_ERROR(aclrtSynchronizeStream(stream));

            auto t2 = std::chrono::high_resolution_clock::now();
            const std::chrono::duration<double, std::milli> ms = t2 - t1;
            time.Add(ms.count() / (NUM_MS_IN_SEC));

#ifdef VERIFY
            for (int i = 0; i < batchSize; i++) {
                NPU_ERROR(aclrtMemcpy(hostBuffs[i], bufferSizeBytes, dRecvBuffs[i], bufferSizeBytes,
                                      ACL_MEMCPY_DEVICE_TO_HOST));
                NPU_ERROR(aclrtMemset(dRecvBuffs[i], bufferSizeBytes, 0, bufferSizeBytes));
                Verify(hostBuffs[i], transferSizeBytes / sizeof(float), i * (transferSizeBytes / sizeof(float)));
            }
#endif
        }

        double bw = (batchSize * transferSizeBytes) / time.Value();
        std::cout << std::fixed << "Recv Device: " << recvDeviceId << "   " << std::setw(OUTPUT_WIDTH_10)
                  << std::setprecision(KEEP_0_DECIMAL_PACES) << (transferSizeBytes >> SHIFT_BYTES_TO_KB) << "kB  "
                  << std::setprecision(KEEP_2_DECIMAL_PACES) << std::setw(OUTPUT_WIDTH_7)
                  << time.Value() * NUM_MS_IN_SEC << "ms " << std::setprecision(KEEP_2_DECIMAL_PACES)
                  << std::setw(OUTPUT_WIDTH_7) << bw * 1e-9 << "GB/s   " << time.Spread() * FRAC_2_PERC << "%\n";
    }

    // Signal to sender that can exit
    pthread_barrier_wait(barrier);

    NPU_ERROR(P2PCommDestroy(p2pComm));

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
    int batchSize = 32;
    int nSamples = 1;  // ra_send_wr fails when using 1000 samples
    uint32_t sendDeviceId = 1;
    uint32_t recvDeviceId = 3;
    uint32_t nDevices = 2;
    size_t bufferSizeBytes = static_cast<size_t>(8 * 1024) * 1024;
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
        SendDeviceLogic(recvSendFifoPath, sendDeviceId, bufferSizeBytes, 0, 1, nSamples, batchSize, barrier, nDevices);

        return 0;
    } else {  // Parent
        std::cout << "Process " << pid << std::endl;
        RecvDeviceLogic(recvSendFifoPath, recvDeviceId, bufferSizeBytes, 0, 1, nSamples, batchSize, barrier, nDevices);

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
