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
#include "test-tools/barrier.h"
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

int SendDeviceLogic(std::string recvSendFifoPath, uint32_t sendDeviceId, size_t bufferSizeBytes, uint32_t recvRank,
                    uint32_t sendRank, int nSamples, int batchSize, int keyNum, Barrier &bar, int nDevices)
{
    int recvSendFd = open(recvSendFifoPath.c_str(), O_WRONLY);

    // Send device setup
    NPU_ERROR(aclrtSetDevice(sendDeviceId));
    aclrtContext sendDeviceContext;
    NPU_ERROR(aclrtCreateContext(&sendDeviceContext, sendDeviceId));
    NPU_ERROR(aclrtSetCurrentContext(sendDeviceContext));
    aclrtStream stream;
    NPU_ERROR(rtStreamCreateWithFlags(&stream, 0, RT_STREAM_FAST_SYNC));

    // Create host buffer for remote to read
    float *hostBuff = (float *)mmap(nullptr, keyNum * batchSize * bufferSizeBytes, PROT_READ | PROT_WRITE,
                                    MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    if (hostBuff == MAP_FAILED) {
        std::cerr << "Mmap failed" << std::endl;
        exit(1);
    }
    memset_s(hostBuff, keyNum * batchSize * bufferSizeBytes, 0, batchSize * bufferSizeBytes);

    // NPU_ERROR(aclrtMallocHost((void **)&hostBuff, batchSize * bufferSizeBytes)); // might want to test mmap etc.

    for (int k = 0; k < keyNum; k++) {
        for (int i = 0; i < (batchSize * bufferSizeBytes) / sizeof(float); i++) {
            hostBuff[k * ((batchSize * bufferSizeBytes) / sizeof(float)) + i] =
                static_cast<float>(k * ((batchSize * bufferSizeBytes) / sizeof(float)) + i);
        }
    }

    // Register host memory to NPU for remote reading (can be done anytime)
    HcclRootInfo rootInfo;
    MEASURE_TIME(NPU_ERROR(P2PGetRootInfo(&rootInfo)), "Receiver P2PGetRootInfo");
    write(recvSendFd, &rootInfo, sizeof(HcclRootInfo));  // Send message to FIFO

    P2pSegmentInfo hostSegmentInfo;
    NPU_ERROR(
        P2PRegisterHostMem(hostBuff, keyNum * batchSize * bufferSizeBytes, &hostSegmentInfo, P2P_SEGMENT_READ_ONLY));
    write(recvSendFd, &hostSegmentInfo, sizeof(P2pSegmentInfo));  // Send message to FIFO
    write(recvSendFd, &hostBuff, sizeof(float *));                // Send message to FIFO

    bar.Wait();
    P2PComm p2pComm;
    MEASURE_TIME(NPU_ERROR(P2PCommInitRootInfo(&rootInfo, P2P_SENDER, LINK_TYPE, &p2pComm)),
                 "Sender P2PCommInitRootInfo");

    // Wait until receiver done reading before exit process
    bar.Wait();
    NPU_ERROR(P2PCommDestroy(p2pComm));

    close(recvSendFd);
    return 0;
}

int RecvDeviceLogic(std::string recvSendFifoPath, uint32_t recvDeviceId, size_t bufferSizeBytes, uint32_t recvRank,
                    uint32_t sendRank, int nSamples, int batchSize, int keyNum, Barrier &bar, Barrier &bar2,
                    int nDevices)
{
    int recvSendFd = open(recvSendFifoPath.c_str(), O_RDONLY);

    // std::vector<size_t> numObjChoices = { 1, 5, 20u, 50u };
    std::vector<size_t> numObjChoices = { 1, 5, 20u, 50u };

    // Receive device setup
    NPU_ERROR(aclrtSetDevice(recvDeviceId));
    aclrtContext recvDeviceContext;
    NPU_ERROR(aclrtCreateContext(&recvDeviceContext, recvDeviceId));
    NPU_ERROR(aclrtSetCurrentContext(recvDeviceContext));

    aclrtStream stream;
    NPU_ERROR(rtStreamCreateWithFlags(&stream, 0, RT_STREAM_FAST_SYNC));

    float **verifyBuffs = (float **)malloc(keyNum * sizeof(float *));
    for (int k = 0; k < keyNum; k++) {
        NPU_ERROR(aclrtMallocHost((void **)&verifyBuffs[k], batchSize * bufferSizeBytes));
    }

    float **hostBuffs = (float **)malloc(batchSize * sizeof(float *));
    for (int i = 0; i < batchSize; i++) {
        NPU_ERROR(aclrtMallocHost((void **)&hostBuffs[i], bufferSizeBytes));
    }

    void *dRecvBuffs[keyNum][batchSize];
    for (int k = 0; k < keyNum; k++) {
        for (int i = 0; i < batchSize; i++) {
            NPU_ERROR(aclrtMalloc((void **)&dRecvBuffs[k][i], bufferSizeBytes, ACL_MEM_MALLOC_HUGE_FIRST));
        }
    }

    HcclRootInfo rootInfo;
    read(recvSendFd, &rootInfo, sizeof(HcclRootInfo));  // Read from fifo

    float *remoteHostBuff;
    P2pSegmentInfo remoteHostSegmentInfo;
    read(recvSendFd, &remoteHostSegmentInfo, sizeof(P2pSegmentInfo));  // Read from fifo
    read(recvSendFd, &remoteHostBuff, sizeof(float *));                // Send message to FIFO
    // Register remote host memory to NPU for remote reading (can be done anytime)
    NPU_ERROR(P2PImportHostSegment(remoteHostSegmentInfo));

    bar.Wait();

    P2PComm p2pComm;
    std::cout << "Init receiver" << std::endl;
    MEASURE_TIME(NPU_ERROR(P2PCommInitRootInfo(&rootInfo, P2P_RECEIVER, LINK_TYPE, &p2pComm)),
                 "Receiver P2PCommInitRootInfo");

    NPU_ERROR(aclrtSynchronizeStream(stream));

    NPU_ERROR(aclrtSynchronizeStream(stream));

    aclrtEvent timingEventStart;
    aclrtEvent timingEventEnd;

    NPU_ERROR(aclrtCreateEvent(&timingEventStart));
    NPU_ERROR(aclrtCreateEvent(&timingEventEnd));

    // Read
    for (int sample = 0; sample < nSamples; sample++) {
        for (int i = 0; i < numObjChoices.size(); i++) {
            int keyNum = numObjChoices[i];
            MeasurementSeries time;

            bar2.Wait();

            NPU_ERROR(aclrtRecordEvent(timingEventStart, stream));

            P2pScatterEntry entries[keyNum];
            std::vector<std::vector<uint64_t>> entryCounts(keyNum);

            for (int k = 0; k < keyNum; k++) {
                float *srcBuf = remoteHostBuff + k * ((batchSize * bufferSizeBytes) / sizeof(float));

                entries[k].ddrBuf = (void *)srcBuf;
                entries[k].dstBufs = dRecvBuffs[k];

                entryCounts[k].resize(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    entryCounts[k][i] = bufferSizeBytes / sizeof(float);
                }

                entries[k].counts = entryCounts[k].data();
                entries[k].dataType = HCCL_DATA_TYPE_FP32;
                entries[k].numEl = batchSize;
            }

            NPU_ERROR(P2PScatterBatchFromRemoteHostMem(entries, keyNum, p2pComm, stream));

            NPU_ERROR(aclrtRecordEvent(timingEventEnd, stream));
            NPU_ERROR(aclrtSynchronizeStream(stream));
            float timingMs;
            NPU_ERROR(aclrtEventElapsedTime(&timingMs, timingEventStart, timingEventEnd));
            time.Add(timingMs / (NUM_MS_IN_SEC));

#ifdef VERIFY
            for (int k = 0; k < keyNum; k++) {
                for (int i = 0; i < batchSize; i++) {
                    NPU_ERROR(aclrtMemcpy(&verifyBuffs[k][i * (bufferSizeBytes / sizeof(float))], bufferSizeBytes,
                                          dRecvBuffs[k][i], bufferSizeBytes, ACL_MEMCPY_DEVICE_TO_HOST));
                    NPU_ERROR(aclrtMemset(dRecvBuffs[k][i], bufferSizeBytes, 0, bufferSizeBytes));
                }

                Verify(verifyBuffs[k], batchSize * bufferSizeBytes / sizeof(float),
                       k * ((batchSize * bufferSizeBytes) / sizeof(float)));
                // std::cout << "Verified " << k << std::endl;
            }
#endif

            double bw = (batchSize * keyNum * bufferSizeBytes) / time.Value();
            std::cout << std::fixed << "Recv Device: " << recvDeviceId << "   " << std::setw(OUTPUT_WIDTH_10)
                      << std::setprecision(KEEP_0_DECIMAL_PACES) << (bufferSizeBytes >> SHIFT_BYTES_TO_KB) << "kB  "
                      << std::setprecision(KEEP_2_DECIMAL_PACES) << std::setw(OUTPUT_WIDTH_7)
                      << time.Value() * NUM_MS_IN_SEC << "ms " << std::setprecision(KEEP_2_DECIMAL_PACES)
                      << std::setw(OUTPUT_WIDTH_7) << bw * 1e-9 << "GB/s   " << time.Spread() * FRAC_2_PERC << "%\n";
        }
    }

    // Signal to sender that can exit
    bar.Wait();

    NPU_ERROR(P2PCommDestroy(p2pComm));

    close(recvSendFd);
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
    size_t blksPerObj = 31;
    size_t maxKeyNum = 50;
    size_t bufferSizeBytes = static_cast<size_t>(73 * 1024);

    int nSamples = 5;  // ra_send_wr fails when using 1000 samples
    uint32_t sendDeviceId = 1;
    uint32_t recvDeviceId = 3;
    uint32_t nDevices = 2;
    unsigned int modeRw = 0666;

    Barrier bar(nDevices * 2 / 2);  // / 2 for only 2 threads
    Barrier bar2(nDevices / 2);     // / 2 for only 2 threads

    std::string recvSendFifoPath = "p2ptest-recvSendFifo";
    Fifo fifo(recvSendFifoPath.c_str(), modeRw);

    std::string recvSendFifoPath2 = "p2ptest-recvSendFifo2";
    Fifo fifo2(recvSendFifoPath2.c_str(), modeRw);

    NPU_ERROR(aclInit(nullptr));

    std::thread t1(SendDeviceLogic, recvSendFifoPath, sendDeviceId, bufferSizeBytes, 0, 1, nSamples, blksPerObj,
                   maxKeyNum, std::ref(bar), nDevices);
    std::thread t2(RecvDeviceLogic, recvSendFifoPath, recvDeviceId, bufferSizeBytes, 0, 1, nSamples, blksPerObj,
                   maxKeyNum, std::ref(bar), std::ref(bar2), nDevices);

    t1.join();
    t2.join();

    NPU_ERROR(aclFinalize());
    return 0;
}

// Note: for now send syncstream returns before receiver has received all data, so best to measure at receiver

auto main() -> int
{
    return Benchmark();
}
