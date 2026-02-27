/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

/**
 * Description: CUDA device manager implementation for GPU support.
 */
#include "datasystem/common/device/nvidia/cuda_device_manager.h"

#include <cstdint>
#include <iomanip>
#include <libgen.h>
#include <link.h>
#include <memory>
#include <mutex>
#include <unordered_set>

#include "datasystem/common/ak_sk/hasher.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/util/dlutils.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/utils/status.h"

#ifndef USE_GPU
#define CUDA_PLUGIN_SHA256 "Unknown"
#else
#include "datasystem/common/device/nvidia/plugin/cuda_plugin_sha256.h"
#endif

namespace datasystem {
namespace cuda {

constexpr auto CudaPluginLibrary = "libcuda_plugin.so";
constexpr int CUDAPLUGIN_SO_MAX_LIMIT = 10 * 1024 * 1024;
constexpr size_t MAX_DEVICE_MALLOC_SIZE = 12UL * 1024 * 1024 * 1024;

std::once_flag CudaDeviceManager::init_;
std::once_flag CudaDeviceManager::hasLoadPlugin_;
std::unique_ptr<CudaDeviceManager> CudaDeviceManager::instance_ = nullptr;

CudaDeviceManager::CudaDeviceManager()
{
    waitPost_ = std::make_unique<WaitPost>();
}

CudaDeviceManager::~CudaDeviceManager()
{
    Shutdown();
}

CudaDeviceManager *CudaDeviceManager::Instance()
{
    std::call_once(init_, []() { instance_ = std::make_unique<CudaDeviceManager>(); });
    return instance_.get();
}

void CudaDeviceManager::Init()
{
#ifdef USE_GPU
    auto traceId = Trace::Instance().GetTraceID();
    loadPluginThread_ = std::make_unique<Thread>([this, traceId]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        Status loadStatus = this->LoadPlugin();
        waitPost_->SetWithStatus(loadStatus);
    });
#else
    waitPost_->SetWithStatus(
        Status(K_RUNTIME_ERROR,
               "Heterogeneous api is currently unavailable. Ensure that the compilation switch '-X on' is enabled when "
               "building datasystem."));
#endif
}

Status CudaDeviceManager::LoadPlugin()
{
    Dl_info dlInfo;
    Status lastRc = Status::OK();
    Raii raii([this]() { waitPost_->Set(); });
    if (dladdr(reinterpret_cast<void *>(CudaDeviceManager::Instance), &dlInfo) == 0) {
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR,
                                FormatString("Load CUDA plugin failed, get dladdr error: %s", GetDlErrorMsg()));
    }
    std::string curSoPath = dlInfo.dli_fname;
    std::string cudaPluginPath = std::string(dirname(curSoPath.data())) + "/" + CudaPluginLibrary;
    // If the plugin library cannot be found with the symbols,
    // try traverse all the lib paths and see if it can be found.
    // Also avoid error logging.
    bool logError = false;
    if (FileSize(cudaPluginPath, logError) < 0) {
        std::unordered_set<std::string> soPaths{ dirname(curSoPath.data()) };
        auto pathCollector = [](struct dl_phdr_info *info, size_t size, void *data) {
            (void)size;
            auto soPaths = reinterpret_cast<std::unordered_set<std::string> *>(data);
            std::string temp(info->dlpi_name);
            soPaths->emplace(dirname(temp.data()));
            return 0;
        };
        CHECK_FAIL_RETURN_STATUS(dl_iterate_phdr(pathCollector, reinterpret_cast<void *>(&soPaths)) == 0,
                                 K_RUNTIME_ERROR, "Walk through list of shared objects failed");
        for (auto &path : soPaths) {
            if (FileSize(path + "/" + CudaPluginLibrary, logError) >= 0) {
                cudaPluginPath = path + "/" + CudaPluginLibrary;
                break;
            }
        }
    }
    LOG(INFO) << "cudaPluginPath is " << cudaPluginPath;
    RETURN_IF_NOT_OK(VerifyingSha256(cudaPluginPath));
    pluginHandle_ = dlopen(cudaPluginPath.c_str(), RTLD_LAZY | RTLD_LOCAL);
    if (pluginHandle_ == nullptr) {
        RETURN_STATUS_LOG_ERROR(K_INVALID,
                                FormatString("Load CUDA plugin failed, dlopen error: %s", GetDlErrorMsg()));
    } else {
        DlsymFuncObj();
    }
    return Status::OK();
}

void CudaDeviceManager::DlsymFuncObj()
{
    DLSYM_FUNC_OBJ(MallocDeviceMemory, pluginHandle_);
    DLSYM_FUNC_OBJ(FreeDeviceMemory, pluginHandle_);
    DLSYM_FUNC_OBJ(MemCopyD2D, pluginHandle_);
    DLSYM_FUNC_OBJ(MemCopyD2H, pluginHandle_);
    DLSYM_FUNC_OBJ(MemCopyH2D, pluginHandle_);

    DLSYM_FUNC_OBJ(GetDeviceIdx, pluginHandle_);
    DLSYM_FUNC_OBJ(SetDeviceIdx, pluginHandle_);

    DLSYM_FUNC_OBJ(RtCreateStream, pluginHandle_);
    DLSYM_FUNC_OBJ(RtSynchronizeStream, pluginHandle_);
    DLSYM_FUNC_OBJ(RtSynchronizeStreamWithTimeout, pluginHandle_);
    DLSYM_FUNC_OBJ(RtDestroyStream, pluginHandle_);
    DLSYM_FUNC_OBJ(RtDestroyStreamForce, pluginHandle_);

    DLSYM_FUNC_OBJ(DSNcclGetRootInfo, pluginHandle_);
    DLSYM_FUNC_OBJ(DSNcclCommInitRootInfo, pluginHandle_);
    DLSYM_FUNC_OBJ(DSNcclSend, pluginHandle_);
    DLSYM_FUNC_OBJ(DSNcclRecv, pluginHandle_);
    DLSYM_FUNC_OBJ(DSNcclCommDestroy, pluginHandle_);
    DLSYM_FUNC_OBJ(DSNcclGetCommAsyncError, pluginHandle_);

    DLSYM_FUNC_OBJ(DSCudartCreateEvent, pluginHandle_);
    DLSYM_FUNC_OBJ(DSCudartRecordEvent, pluginHandle_);
    DLSYM_FUNC_OBJ(DSCudartSynchronizeEvent, pluginHandle_);
    DLSYM_FUNC_OBJ(DSCudartSynchronizeEventWithTimeout, pluginHandle_);
    DLSYM_FUNC_OBJ(DSCudartDestroyEvent, pluginHandle_);
    DLSYM_FUNC_OBJ(DSCudartQueryEventStatus, pluginHandle_);

    DLSYM_FUNC_OBJ(DSCudaInit, pluginHandle_);
    DLSYM_FUNC_OBJ(DSCudaFinalize, pluginHandle_);
    DLSYM_FUNC_OBJ(DSCudartSetDevice, pluginHandle_);
    DLSYM_FUNC_OBJ(DSCudartGetDeviceCount, pluginHandle_);
    DLSYM_FUNC_OBJ(DSCudartQueryDeviceStatus, pluginHandle_);
    DLSYM_FUNC_OBJ(DSCudartMemcpyAsync, pluginHandle_);
    DLSYM_FUNC_OBJ(DSCudartResetDevice, pluginHandle_);
    DLSYM_FUNC_OBJ(DSCudartMalloc, pluginHandle_);
    DLSYM_FUNC_OBJ(DSCudartFree, pluginHandle_);
    DLSYM_FUNC_OBJ(DSCudartMallocHost, pluginHandle_);
    DLSYM_FUNC_OBJ(DSCudartFreeHost, pluginHandle_);
}

Status CudaDeviceManager::CheckPluginOk()
{
    std::call_once(hasLoadPlugin_, []() { Instance()->Init(); });
    Status loadStatus = waitPost_->WaitAndGetStatus();
    if (!loadStatus.IsOk()) {
        return loadStatus;
    }
    return Status::OK();
}

Status CudaDeviceManager::VerifyingSha256(const std::string &cudaPluginPath)
{
    // Step 1: Verifying the File Size.
    auto fileSize = FileSize(cudaPluginPath);
    CHECK_FAIL_RETURN_STATUS(fileSize >= 0, K_RUNTIME_ERROR, "Get file size failed");
    if (fileSize > CUDAPLUGIN_SO_MAX_LIMIT) {
        RETURN_STATUS_LOG_ERROR(
            K_NOT_AUTHORIZED,
            FormatString("Load CUDA plugin failed. The size of the plugin file is %lld Byte, which exceeds the max "
                         "limit of %lld Byte.",
                         fileSize, CUDAPLUGIN_SO_MAX_LIMIT));
    }
    // Step 2: Read the file.
    std::string fileContext;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ReadFileToString(cudaPluginPath, fileContext),
                                     "Failed to read the libcuda_plugin.so");
    // Step 3: Calculate the hash value.
    std::unique_ptr<unsigned char[]> outHashData;
    unsigned int outHashSize;
    Hasher hasher;
    RETURN_IF_NOT_OK(hasher.HashSHA256(fileContext.c_str(), fileContext.size(), outHashData, outHashSize));
    std::stringstream ss;
    int fieldWidth = 2;
    for (size_t i = 0; i < outHashSize; ++i) {
        ss << std::hex << std::setfill('0') << std::setw(fieldWidth) << static_cast<int>(outHashData[i]);
    }
    // Step 4: Check whether the hash values are consistent.
    if (ss.str() != CUDA_PLUGIN_SHA256) {
        RETURN_STATUS_LOG_ERROR(
            K_NOT_AUTHORIZED,
            "Load CUDA plugin failed, which fails to pass the integrity check. "
            "Possible causes and solutions: "
            "1.This usually occurs when libcuda_plugin.so and libdatasystem.so are from different versions. Ensure "
            "both libraries are from the same version.");
    }
    return Status::OK();
}

Status CudaDeviceManager::VerifyDeviceId(std::vector<uint32_t> deviceIds)
{
    for (const auto devId : deviceIds) {
        RETURN_IF_NOT_OK(QueryDeviceStatus(devId));
    }
    return Status::OK();
}

void CudaDeviceManager::Shutdown()
{
    if (loadPluginThread_ != nullptr) {
        loadPluginThread_->join();
        loadPluginThread_.reset();
    }
}

Status CudaDeviceManager::HandleNcclResult(int ncclResult)
{
    constexpr int ncclInvalidUsage = 5;     // ncclInvalidUsage
    constexpr int ncclRemoteError = 6;      // ncclRemoteError
    constexpr int ncclSystemError = 2;      // ncclSystemError
    switch (ncclResult) {
        case ncclInvalidUsage:
            return Status(StatusCode::K_NCCL_ERROR, __LINE__, __FILE__,
                          "NCCL api operation failed with error code: 5. "
                          "The call to NCCL is incorrect. This is usually reflecting a programming error.");

        case ncclRemoteError:
            return Status(StatusCode::K_NCCL_ERROR, __LINE__, __FILE__,
                          "NCCL api operation failed with error code: 6. "
                          "A call failed possibly due to a network error or a remote process exiting prematurely.");

        case ncclSystemError:
            return Status(StatusCode::K_NCCL_ERROR, __LINE__, __FILE__,
                          "NCCL api operation failed with error code: 2. "
                          "A call to the system failed.");
        default:
            RETURN_NCCL_RESULT(ncclResult);
    }
}

// ==================== Lifecycle Management ====================

Status CudaDeviceManager::cudaInit()
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSCudaInitFunc_);
    RETURN_CUDART_RESULT(DSCudaInitFunc_());
}

Status CudaDeviceManager::cudaFinalize()
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSCudaFinalizeFunc_);
    RETURN_CUDART_RESULT(DSCudaFinalizeFunc_());
}

// ==================== Device Management ====================

Status CudaDeviceManager::MallocDeviceMemory(size_t dataSize, void *&deviceData)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    CHECK_FAIL_RETURN_STATUS(dataSize <= MAX_DEVICE_MALLOC_SIZE, K_INVALID, "The dataSize can't be greater then 12GB.");
    RETURN_RUNTIME_ERROR_IF_NULL(MallocDeviceMemoryFunc_);
    RETURN_CUDART_RESULT(MallocDeviceMemoryFunc_(dataSize, deviceData));
}

Status CudaDeviceManager::FreeDeviceMemory(void *deviceData)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(FreeDeviceMemoryFunc_);
    RETURN_CUDART_RESULT(FreeDeviceMemoryFunc_(deviceData));
}

Status CudaDeviceManager::GetDeviceIdx(int32_t &deviceIdx)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(GetDeviceIdxFunc_);
    int cudaRet = GetDeviceIdxFunc_(deviceIdx);
    constexpr int normalCode = 0;  // cudaSuccess
    if (cudaRet != normalCode) {
        RETURN_STATUS_LOG_ERROR(
            K_INVALID,
            FormatString(
                "May not create context or set device in this thread. Detail: cuda api failed with error code %d",
                cudaRet));
    }
    return Status::OK();
}

Status CudaDeviceManager::SetDeviceIdx(int32_t deviceId)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(SetDeviceIdxFunc_);
    RETURN_CUDART_RESULT(SetDeviceIdxFunc_(deviceId));
}

Status CudaDeviceManager::cudartSetDevice(int deviceId)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSCudartSetDeviceFunc_);
    RETURN_CUDART_RESULT(DSCudartSetDeviceFunc_(deviceId));
}

Status CudaDeviceManager::cudartGetDeviceCount(int *count)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSCudartGetDeviceCountFunc_);
    RETURN_CUDART_RESULT(DSCudartGetDeviceCountFunc_(count));
}

Status CudaDeviceManager::cudartQueryDeviceStatus(uint32_t deviceId)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSCudartQueryDeviceStatusFunc_);
    int32_t deviceStatus;
    int cudaRet = DSCudartQueryDeviceStatusFunc_(deviceId, &deviceStatus);
    constexpr int normalCode = 0;  // cudaSuccess
    if (cudaRet != normalCode) {
        RETURN_STATUS_LOG_ERROR(
            K_INVALID,
            FormatString(
                "Got Error/ABNORMAL device, deviceId: %d. Detail: cuda api failed with error code %d, deviceStatus: %d",
                deviceId, cudaRet, deviceStatus));
    }
    return Status::OK();
}

Status CudaDeviceManager::cudartResetDevice(int deviceId)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSCudartResetDeviceFunc_);
    RETURN_CUDART_RESULT(DSCudartResetDeviceFunc_(deviceId));
}

// ==================== Memory Management ====================

Status CudaDeviceManager::cudartMalloc(void **devPtr, size_t size)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSCudartMallocFunc_);
    RETURN_CUDART_RESULT(DSCudartMallocFunc_(devPtr, size));
}

Status CudaDeviceManager::cudartFree(void *devPtr)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSCudartFreeFunc_);
    RETURN_CUDART_RESULT(DSCudartFreeFunc_(devPtr));
}

Status CudaDeviceManager::cudartMallocHost(void **devPtr, size_t size)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSCudartMallocHostFunc_);
    RETURN_CUDART_RESULT(DSCudartMallocHostFunc_(devPtr, size));
}

Status CudaDeviceManager::cudartFreeHost(void *devPtr)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSCudartFreeHostFunc_);
    RETURN_CUDART_RESULT(DSCudartFreeHostFunc_(devPtr));
}

// ==================== Memory Copy ====================

Status CudaDeviceManager::MemCopyD2H(void *hostDst, size_t dstMaxSize, const void *devSrc, size_t srcSize)
{
    (void)dstMaxSize;
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(MemCopyD2HFunc_);
    RETURN_CUDART_RESULT(MemCopyD2HFunc_(hostDst, devSrc, srcSize));
}

Status CudaDeviceManager::MemCopyH2D(void *devDst, size_t dstMaxSize, const void *hostSrc, size_t srcSize)
{
    (void)dstMaxSize;
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(MemCopyH2DFunc_);
    RETURN_CUDART_RESULT(MemCopyH2DFunc_(devDst, hostSrc, srcSize));
}

Status CudaDeviceManager::MemCopyD2D(void *dst, size_t dstMaxSize, const void *src, size_t srcSize)
{
    (void)dstMaxSize;
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(MemCopyD2DFunc_);
    RETURN_CUDART_RESULT(MemCopyD2DFunc_(dst, src, srcSize));
}

Status CudaDeviceManager::cudartMemcpyAsync(void *dst, const void *src, size_t count,
                                            cudaMemcpyKind kind, cudaStream_t stream)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSCudartMemcpyAsyncFunc_);
    RETURN_CUDART_RESULT(DSCudartMemcpyAsyncFunc_(dst, src, count, kind, stream));
}

// ==================== Stream Management ====================

Status CudaDeviceManager::RtCreateStream(cudaStream_t *stream)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(RtCreateStreamFunc_);
    RETURN_CUDART_RESULT(RtCreateStreamFunc_(stream));
}

Status CudaDeviceManager::RtSynchronizeStream(cudaStream_t stream)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(RtSynchronizeStreamFunc_);
    RETURN_CUDART_RESULT(RtSynchronizeStreamFunc_(stream));
}

Status CudaDeviceManager::RtSynchronizeStreamWithTimeout(cudaStream_t stream, int32_t timeoutMs)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(RtSynchronizeStreamWithTimeoutFunc_);
    RETURN_CUDART_RESULT(RtSynchronizeStreamWithTimeoutFunc_(stream, timeoutMs));
}

Status CudaDeviceManager::RtDestroyStream(cudaStream_t stream)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(RtDestroyStreamFunc_);
    RETURN_CUDART_RESULT(RtDestroyStreamFunc_(stream));
}

Status CudaDeviceManager::RtDestroyStreamForce(cudaStream_t stream)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(RtDestroyStreamForceFunc_);
    RETURN_CUDART_RESULT(RtDestroyStreamForceFunc_(stream));
}

// ==================== NCCL Communication ====================

Status CudaDeviceManager::DSNcclGetRootInfo(ncclUniqueId *rootInfo)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSNcclGetRootInfoFunc_);
    RETURN_NCCL_RESULT(DSNcclGetRootInfoFunc_(rootInfo));
}

Status CudaDeviceManager::DSNcclCommInitRootInfo(int nRanks, const ncclUniqueId *rootInfo, int rank, ncclComm_t *comm)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSNcclCommInitRootInfoFunc_);
    return HandleNcclResult(DSNcclCommInitRootInfoFunc_(nRanks, rootInfo, rank, comm));
}

Status CudaDeviceManager::DSNcclSend(void *sendBuf, size_t count, ncclDataType_t dataType, int destRank,
                                     ncclComm_t comm, cudaStream_t stream)
{
    PerfPoint point(PerfKey::DS_NCCL_SEND);
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSNcclSendFunc_);
    return HandleNcclResult(DSNcclSendFunc_(sendBuf, count, dataType, destRank, comm, stream));
}

Status CudaDeviceManager::DSNcclRecv(void *recvBuf, size_t count, ncclDataType_t dataType, int srcRank,
                                     ncclComm_t comm, cudaStream_t stream)
{
    PerfPoint point(PerfKey::DS_NCCL_RECEIVE);
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSNcclRecvFunc_);
    return HandleNcclResult(DSNcclRecvFunc_(recvBuf, count, dataType, srcRank, comm, stream));
}

Status CudaDeviceManager::DSNcclCommDestroy(ncclComm_t comm)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSNcclCommDestroyFunc_);
    RETURN_NCCL_RESULT(DSNcclCommDestroyFunc_(comm));
}

Status CudaDeviceManager::DSNcclGetCommAsyncError(ncclComm_t comm)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSNcclGetCommAsyncErrorFunc_);
    ncclResult_t asyncError;
    HandleNcclResult(DSNcclGetCommAsyncErrorFunc_(comm, &asyncError));
    return HandleNcclResult(asyncError);
}

// ==================== Event Management ====================

Status CudaDeviceManager::DSCudartCreateEvent(cudaEvent_t *event)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSCudartCreateEventFunc_);
    RETURN_CUDART_RESULT(DSCudartCreateEventFunc_(event));
}

Status CudaDeviceManager::DSCudartRecordEvent(cudaEvent_t event, cudaStream_t stream)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSCudartRecordEventFunc_);
    RETURN_CUDART_RESULT(DSCudartRecordEventFunc_(event, stream));
}

Status CudaDeviceManager::DSCudartSynchronizeEvent(cudaEvent_t event)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSCudartSynchronizeEventFunc_);
    RETURN_CUDART_RESULT(DSCudartSynchronizeEventFunc_(event));
}

Status CudaDeviceManager::DSCudartSynchronizeEventWithTimeout(cudaEvent_t event, int32_t timeoutMs)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSCudartSynchronizeEventWithTimeoutFunc_);
    RETURN_CUDART_RESULT(DSCudartSynchronizeEventWithTimeoutFunc_(event, timeoutMs));
}

Status CudaDeviceManager::DSCudartDestroyEvent(cudaEvent_t event)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSCudartDestroyEventFunc_);
    RETURN_CUDART_RESULT(DSCudartDestroyEventFunc_(event));
}

Status CudaDeviceManager::DSCudartQueryEventStatus(cudaEvent_t event)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSCudartQueryEventStatusFunc_);
    CHECK_FAIL_RETURN_STATUS(event != nullptr, K_RUNTIME_ERROR, "Event is nullptr");
    cudaEventRecordedStatus recordedStatus;
    auto cudaResult = DSCudartQueryEventStatusFunc_(event, &recordedStatus);
    if (cudaResult != 0) {
        return Status(K_RUNTIME_ERROR, FormatString("QueryEventStatus failed with error code: %d", cudaResult));
    }
    CHECK_FAIL_RETURN_STATUS(
        recordedStatus == cudaEventRecordedStatus::CUDA_EVENT_RECORDED_STATUS_COMPLETE, K_CUDA_ERROR,
        "The event is not recorded in the stream, or the event recorded in the stream is not executed or fails "
        "to be executed.");
    return Status::OK();
}

// ==================== DeviceManagerBase Interface Implementation ====================

Status CudaDeviceManager::Init(const char *configPath)
{
    (void)configPath;
    return cudaInit();
}

Status CudaDeviceManager::Finalize()
{
    return cudaFinalize();
}

Status CudaDeviceManager::GetDeviceCount(uint32_t *count)
{
    return cudartGetDeviceCount(reinterpret_cast<int *>(count));
}

Status CudaDeviceManager::QueryDeviceStatus(uint32_t deviceId)
{
    return cudartQueryDeviceStatus(deviceId);
}

Status CudaDeviceManager::SetDevice(int32_t deviceId)
{
    return cudartSetDevice(static_cast<int>(deviceId));
}

Status CudaDeviceManager::ResetDevice(int32_t deviceId)
{
    return cudartResetDevice(static_cast<int>(deviceId));
}

Status CudaDeviceManager::Malloc(void **devPtr, size_t size, MemMallocPolicy policy)
{
    (void)policy;
    return cudartMalloc(devPtr, size);
}

Status CudaDeviceManager::Free(void *devPtr)
{
    return cudartFree(devPtr);
}

Status CudaDeviceManager::MallocHost(void **hostPtr, size_t size)
{
    return cudartMallocHost(hostPtr, size);
}

Status CudaDeviceManager::FreeHost(void *hostPtr)
{
    return cudartFreeHost(hostPtr);
}

Status CudaDeviceManager::MemcpyAsync(void *dst, size_t dstMaxSize, const void *src, size_t count,
                                      MemcpyKind kind, void *stream)
{
    (void)dstMaxSize;
    cudaMemcpyKind cudaKind = {};
    RETURN_IF_NOT_OK(ToCudaMemcpyKind(kind, cudaKind));
    return cudartMemcpyAsync(dst, src, count, cudaKind, static_cast<cudaStream_t>(stream));
}

Status CudaDeviceManager::MemcpyBatch(void **dsts, size_t *destMax, void **srcs, size_t *sizes, size_t numBatches,
                                      MemcpyKind kind, uint32_t deviceIdx, size_t *failIndex)
{
    (void)dsts;
    (void)destMax;
    (void)srcs;
    (void)sizes;
    (void)kind;
    (void)numBatches;
    (void)kind;
    (void)deviceIdx;
    (void)failIndex;
    return Status(K_NOT_SUPPORTED, "Batch memory copy is not supported on CUDA");
};
// ==================== Stream Management ====================

Status CudaDeviceManager::CreateStream(void **stream)
{
    return RtCreateStream(reinterpret_cast<cudaStream_t *>(stream));
}

Status CudaDeviceManager::SynchronizeStream(void *stream)
{
    return RtSynchronizeStream(static_cast<cudaStream_t>(stream));
}

Status CudaDeviceManager::SynchronizeStreamWithTimeout(void *stream, int32_t timeoutMs)
{
    return RtSynchronizeStreamWithTimeout(static_cast<cudaStream_t>(stream), timeoutMs);
}

Status CudaDeviceManager::DestroyStream(void *stream)
{
    return RtDestroyStream(static_cast<cudaStream_t>(stream));
}

Status CudaDeviceManager::DestroyStreamForce(void *stream)
{
    return RtDestroyStreamForce(static_cast<cudaStream_t>(stream));
}

Status CudaDeviceManager::CreateEvent(void **event)
{
    return DSCudartCreateEvent(reinterpret_cast<cudaEvent_t *>(event));
}

Status CudaDeviceManager::RecordEvent(void *event, void *stream)
{
    return DSCudartRecordEvent(static_cast<cudaEvent_t>(event), static_cast<cudaStream_t>(stream));
}

Status CudaDeviceManager::SynchronizeEvent(void *event)
{
    return DSCudartSynchronizeEvent(static_cast<cudaEvent_t>(event));
}

Status CudaDeviceManager::SynchronizeEventWithTimeout(void *event, int32_t timeoutMs)
{
    return DSCudartSynchronizeEventWithTimeout(static_cast<cudaEvent_t>(event), timeoutMs);
}

Status CudaDeviceManager::DestroyEvent(void *event)
{
    return DSCudartDestroyEvent(static_cast<cudaEvent_t>(event));
}

Status CudaDeviceManager::QueryEventStatus(void *event)
{
    return DSCudartQueryEventStatus(static_cast<cudaEvent_t>(event));
}

Status CudaDeviceManager::CommGetRootInfo(CommRootInfo *rootInfo)
{
    ncclUniqueId *ncclRootInfo = nullptr;
    RETURN_IF_NOT_OK(ToNcclRootInfo(rootInfo, ncclRootInfo));
    return DSNcclGetRootInfo(ncclRootInfo);
}

Status CudaDeviceManager::CommInitRootInfo(uint32_t nRanks, const CommRootInfo *rootInfo, uint32_t rank, void **comm)
{
    const ncclUniqueId *ncclRootInfo = nullptr;
    RETURN_IF_NOT_OK(ToNcclRootInfo(rootInfo, ncclRootInfo));
    return DSNcclCommInitRootInfo(static_cast<int>(nRanks), ncclRootInfo, static_cast<int>(rank),
                                  reinterpret_cast<ncclComm_t *>(comm));
}

Status CudaDeviceManager::CommSend(void *sendBuf, uint64_t count, CommDataType dataType, uint32_t destRank,
                                   void *comm, void *stream)
{
    ncclDataType_t ncclDataType = {};
    RETURN_IF_NOT_OK(ToNcclDataType(dataType, ncclDataType));
    return DSNcclSend(sendBuf, static_cast<size_t>(count), ncclDataType, static_cast<int>(destRank),
                      static_cast<ncclComm_t>(comm), static_cast<cudaStream_t>(stream));
}

Status CudaDeviceManager::CommRecv(void *recvBuf, uint64_t count, CommDataType dataType, uint32_t srcRank,
                                   void *comm, void *stream)
{
    ncclDataType_t ncclDataType = {};
    RETURN_IF_NOT_OK(ToNcclDataType(dataType, ncclDataType));
    return DSNcclRecv(recvBuf, static_cast<size_t>(count), ncclDataType, static_cast<int>(srcRank),
                      static_cast<ncclComm_t>(comm), static_cast<cudaStream_t>(stream));
}

Status CudaDeviceManager::CommDestroy(void *comm)
{
    return DSNcclCommDestroy(static_cast<ncclComm_t>(comm));
}

Status CudaDeviceManager::CommGetAsyncError(void *comm)
{
    return DSNcclGetCommAsyncError(static_cast<ncclComm_t>(comm));
}

// ==================== P2P Communication (Not Supported on CUDA) ====================

Status CudaDeviceManager::P2PGetRootInfo(CommRootInfo *rootInfo)
{
    (void)rootInfo;
    return Status(K_NOT_SUPPORTED, "P2P operations are not supported on CUDA");
}

Status CudaDeviceManager::P2PCommInitRootInfo(const CommRootInfo *rootInfo, P2pKindBase kind, P2pLinkBase link,
                                              void **comm)
{
    (void)rootInfo;
    (void)kind;
    (void)link;
    (void)comm;
    return Status(K_NOT_SUPPORTED, "P2P operations are not supported on CUDA");
}

Status CudaDeviceManager::P2PCommDestroy(void *comm)
{
    (void)comm;
    return Status(K_NOT_SUPPORTED, "P2P operations are not supported on CUDA");
}

Status CudaDeviceManager::P2PSend(void *sendBuf, uint64_t count, CommDataType dataType, void *comm, void *stream)
{
    (void)sendBuf;
    (void)count;
    (void)dataType;
    (void)comm;
    (void)stream;
    return Status(K_NOT_SUPPORTED, "P2P operations are not supported on CUDA");
}

Status CudaDeviceManager::P2PRecv(void *recvBuf, uint64_t count, CommDataType dataType, void *comm, void *stream)
{
    (void)recvBuf;
    (void)count;
    (void)dataType;
    (void)comm;
    (void)stream;
    return Status(K_NOT_SUPPORTED, "P2P operations are not supported on CUDA");
}

Status CudaDeviceManager::P2PGetCommAsyncError(void *comm)
{
    (void)comm;
    return Status(K_NOT_SUPPORTED, "P2P operations are not supported on CUDA");
}

// ==================== NPU-Specific Interfaces (Not Supported on CUDA) ====================

Status CudaDeviceManager::NotifyCreate(int32_t deviceId, void **notify)
{
    (void)deviceId;
    (void)notify;
    return Status(K_NOT_SUPPORTED, "Notify operations are NPU-specific and not supported on CUDA");
}

Status CudaDeviceManager::NotifyDestroy(void *notify)
{
    (void)notify;
    return Status(K_NOT_SUPPORTED, "Notify operations are NPU-specific and not supported on CUDA");
}

Status CudaDeviceManager::NotifyRecord(void *notify, void *stream)
{
    (void)notify;
    (void)stream;
    return Status(K_NOT_SUPPORTED, "Notify operations are NPU-specific and not supported on CUDA");
}

Status CudaDeviceManager::NotifyWait(void *notify, void *stream)
{
    (void)notify;
    (void)stream;
    return Status(K_NOT_SUPPORTED, "Notify operations are NPU-specific and not supported on CUDA");
}

Status CudaDeviceManager::LaunchCallback(StreamCallback fn, void *userData, CallbackBlockType blockType, void *stream)
{
    (void)fn;
    (void)userData;
    (void)blockType;
    (void)stream;
    return Status(K_NOT_SUPPORTED, "LaunchCallback is NPU-specific and not supported on CUDA");
}

Status CudaDeviceManager::ProcessReport(int32_t timeout)
{
    (void)timeout;
    return Status(K_NOT_SUPPORTED, "ProcessReport is NPU-specific and not supported on CUDA");
}

Status CudaDeviceManager::SubscribeReport(uint64_t threadId, void *stream)
{
    (void)threadId;
    (void)stream;
    return Status(K_NOT_SUPPORTED, "SubscribeReport is NPU-specific and not supported on CUDA");
}

Status CudaDeviceManager::UnSubscribeReport(uint64_t threadId, void *stream)
{
    (void)threadId;
    (void)stream;
    return Status(K_NOT_SUPPORTED, "UnSubscribeReport is NPU-specific and not supported on CUDA");
}

Status CudaDeviceManager::GeneralCtrl(uintptr_t *ctrl, uint32_t num, uint32_t type)
{
    (void)ctrl;
    (void)num;
    (void)type;
    return Status(K_NOT_SUPPORTED, "GeneralCtrl is NPU-specific and not supported on CUDA");
}

Status CudaDeviceManager::GetDeviceInfo(uint32_t deviceId, int32_t moduleType, int32_t infoType, int64_t *val)
{
    (void)deviceId;
    (void)moduleType;
    (void)infoType;
    (void)val;
    return Status(K_NOT_SUPPORTED, "GetDeviceInfo is NPU-specific and not supported on CUDA");
}

// ==================== P2P Host Memory Registration (Not Supported on CUDA) ====================

Status CudaDeviceManager::P2PRegisterHostMem(void *hostBuf, uint64_t size, P2pSegmentBase *segmentInfo,
                                             P2pSegmentPermBase permissions)
{
    (void)hostBuf;
    (void)size;
    (void)segmentInfo;
    (void)permissions;
    return Status(K_NOT_SUPPORTED, "P2PRegisterHostMem is NPU-specific and not supported on CUDA");
}

Status CudaDeviceManager::P2PImportHostSegment(P2pSegmentBase segmentInfo)
{
    (void)segmentInfo;
    return Status(K_NOT_SUPPORTED, "P2PImportHostSegment is NPU-specific and not supported on CUDA");
}

Status CudaDeviceManager::P2PScatterBatchFromRemoteHostMem(P2pScatterBase *entries, uint32_t batchSize,
                                                           void *comm, void *stream)
{
    (void)entries;
    (void)batchSize;
    (void)comm;
    (void)stream;
    return Status(K_NOT_SUPPORTED, "P2PScatterBatchFromRemoteHostMem is NPU-specific and not supported on CUDA");
}

}  // namespace cuda
}  // namespace datasystem
