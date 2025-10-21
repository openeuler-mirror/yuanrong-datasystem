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
#include <runtime/stream.h>

#include <cstring>
#include <cstdio>
#include <iomanip>
#include "test-tools/measurementSeries.h"
#include "test-tools/test-error.h"
#include "test-tools/fifo.h"
#include "test-tools/tools.h"
#include "securec.h"

#include <errno.h>   // For errno
#include <string.h>  // For strerror

#include <arpa/inet.h>

// HCCL can be used using #define USE_HCCL
// define USE_HCCL

constexpr size_t BLOCK_SIZE_MULT = 2;
constexpr size_t NUM_MS_IN_SEC = 1000;
constexpr int FRAC_2_PERC = 100;
constexpr int KEEP_0_DECIMAL_PACES = 0;
constexpr int KEEP_2_DECIMAL_PACES = 2;
constexpr int OUTPUT_WIDTH_10 = 10;
constexpr int OUTPUT_WIDTH_7 = 7;
constexpr int SHIFT_BYTES_TO_KB = 10;
constexpr P2pLink LINK_TYPE = P2P_LINK_ROCE;

int createClientSocket(std::string serverIp, int serverPort)
{
    // Try to connect repeatedly
    int attempts = 0;
    const int max_attempts = 30;
    while (attempts < max_attempts) {
        // a. Create a socket
        int client_socket = socket(AF_INET, SOCK_STREAM, 0);
        if (client_socket == -1) {
            std::cerr << "Error creating socket" << std::endl;
            return -1;
        }

        // b. Set up server address structure
        sockaddr_in server_addr;
        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(serverPort);
        if (inet_pton(AF_INET, serverIp.c_str(), &server_addr.sin_addr) <= 0) {
            std::cerr << "Invalid address/ Address not supported" << std::endl;
            close(client_socket);
            return -1;
        }

        // c. Try to connect to the server
        if (connect(client_socket, (sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
            std::cerr << "Connection Failed (attempt " << (attempts + 1) << " of " << max_attempts
                      << "): " << strerror(errno) << " (errno: " << errno << ")" << std::endl;
            close(client_socket);

            // d. Wait for 1 second before the next attempt
            sleep(1);
            attempts++;
            continue;  // Go to the next iteration of the loop
        }

        return client_socket;
    }

    return -1;
}

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

// Pseudo barrier to synchronize both processes at least somewhat
void pseudoClientBarrier(int clientSocket)
{
    uint64_t barSendData = 1;
    uint64_t barRecvData;
    recv(clientSocket, &barRecvData, sizeof(barRecvData), 0);
    send(clientSocket, &barSendData, sizeof(barSendData), 0);
}

int RecvDeviceLogic(int clientSocket, uint32_t recvDeviceId, size_t bufferSizeBytes, uint32_t recvRank,
                    uint32_t sendRank, int nSamples, int batchSize, int nDevices)
{
    aclInit(nullptr);

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

#ifdef USE_HCCL
    HcclRootInfo rootInfo;
    MEASURE_TIME(NPU_ERROR(HcclGetRootInfo(&rootInfo)), "Receiver HcclGetRootInfo");
    send(clientSocket, &rootInfo, sizeof(HcclRootInfo), 0);
#else
    HcclRootInfo rootInfo;
    MEASURE_TIME(NPU_ERROR(P2PGetRootInfo(&rootInfo)), "Receiver P2PGetRootInfo");
    std::cout << "Start write" << std::endl;
    send(clientSocket, &rootInfo, sizeof(HcclRootInfo), 0);
    std::cout << "End write" << std::endl;
#endif

    pseudoClientBarrier(clientSocket);
    std::cout << "Comm Init Rootinfo" << std::endl;
#ifdef USE_HCCL
    HcclComm hcclComm;
    MEASURE_TIME(NPU_ERROR(HcclCommInitRootInfo(nDevices, &rootInfo, recvRank, &hcclComm)),
                 "Receiver HcclCommInitRootInfo");
#else
    P2PComm p2pComm;
    std::cout << "Init receiver" << std::endl;
    MEASURE_TIME(NPU_ERROR(P2PCommInitRootInfo(&rootInfo, P2P_RECEIVER, LINK_TYPE, &p2pComm)),
                 "Receiver P2PCommInitRootInfo");
#endif
    std::cout << "Comm Init Rootinfo finish" << std::endl;

    for (int i = 0; i < batchSize; i++) {
#ifdef USE_HCCL
        NPU_ERROR(
            HcclRecv(dRecvBuffs[i], bufferSizeBytes / sizeof(float), HCCL_DATA_TYPE_FP32, sendRank, hcclComm, stream));
#else
        NPU_ERROR(P2PRecv(dRecvBuffs[i], bufferSizeBytes / sizeof(float), HCCL_DATA_TYPE_FP32, p2pComm, stream));
#endif
    }

    NPU_ERROR(aclrtSynchronizeStream(stream));

    // Recv
    for (size_t transferSizeBytes = 128 * 1024; transferSizeBytes <= bufferSizeBytes;
         transferSizeBytes *= BLOCK_SIZE_MULT) {
        MeasurementSeries time;
        pseudoClientBarrier(clientSocket);

        for (int sample = 0; sample < nSamples; sample++) {
            auto t1 = std::chrono::high_resolution_clock::now();
            for (int i = 0; i < batchSize; i++) {
#ifdef USE_HCCL
                NPU_ERROR(HcclRecv(dRecvBuffs[i], transferSizeBytes / sizeof(float), HCCL_DATA_TYPE_FP32, sendRank,
                                   hcclComm, stream));
#else
                NPU_ERROR(
                    P2PRecv(dRecvBuffs[i], transferSizeBytes / sizeof(float), HCCL_DATA_TYPE_FP32, p2pComm, stream));
#endif
            }
            NPU_ERROR(aclrtSynchronizeStream(stream));
            auto t2 = std::chrono::high_resolution_clock::now();
            const std::chrono::duration<double, std::milli> ms = t2 - t1;
            time.Add(ms.count() / (NUM_MS_IN_SEC));
        }

        for (int i = 0; i < batchSize; i++) {
            NPU_ERROR(
                aclrtMemcpy(hostBuffs[i], bufferSizeBytes, dRecvBuffs[i], bufferSizeBytes, ACL_MEMCPY_DEVICE_TO_HOST));
            Verify(hostBuffs[i], transferSizeBytes / sizeof(float));
        }

        double bw = (batchSize * transferSizeBytes) / time.Value();
        std::cout << std::fixed << "Recv Device: " << recvDeviceId << "   " << std::setw(OUTPUT_WIDTH_10)
                  << std::setprecision(KEEP_0_DECIMAL_PACES) << (transferSizeBytes >> SHIFT_BYTES_TO_KB) << "kB  "
                  << std::setprecision(KEEP_2_DECIMAL_PACES) << std::setw(OUTPUT_WIDTH_7)
                  << time.Value() * NUM_MS_IN_SEC << "ms " << std::setprecision(KEEP_2_DECIMAL_PACES)
                  << std::setw(OUTPUT_WIDTH_7) << bw * 1e-9 << "GB/s   " << time.Spread() * FRAC_2_PERC << "%\n";
    }

    close(clientSocket);
    NPU_ERROR(aclFinalize());
    return 0;
}

int Benchmark()
{
    int batchSize = 512;
    int nSamples = 100;  // todo: ra_send_wr fails when using 1000 samples
    uint32_t recvDeviceId = 4;
    uint32_t nDevices = 2;
    size_t bufferSizeBytes = static_cast<size_t>(4) * 1024 * 1024;
    unsigned int modeRw = 0666;

    std::string server_ip = "10.170.27.163";  // Replace with receiver's IP
    int server_port = 8085;
    int send_socket = createClientSocket(server_ip, server_port);

    RecvDeviceLogic(send_socket, recvDeviceId, bufferSizeBytes, 0, 1, nSamples, batchSize, nDevices);
    return 0;
}

// Note: for now send syncstream returns before receiver has received all data, so best to measure at receiver

auto main() -> int
{
    return Benchmark();
}
