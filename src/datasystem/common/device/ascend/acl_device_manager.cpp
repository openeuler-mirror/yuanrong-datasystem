/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: The AscendCL device manager.
 */
#include "datasystem/common/device/ascend/acl_device_manager.h"

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

#ifndef BUILD_HETERO
#define ACL_PLUGIN_SHA256 "Unknown"
#else
#include "datasystem/common/device/ascend/plugin/acl_plugin_sha256.h"
#endif

namespace datasystem {
namespace acl {

constexpr auto AclPluginLibrary = "libacl_plugin.so";
constexpr int ACLPLUGIN_SO_MAX_LIMIT = 10 * 1024 * 1024;
constexpr size_t MAX_DEVICE_MALLOC_SIZE = 12ul * 1024 * 1024 * 1024;

std::once_flag AclDeviceManager::init_;
std::once_flag AclDeviceManager::hasLoadPlugin_;
std::unique_ptr<AclDeviceManager> AclDeviceManager::instance_ = nullptr;

AclDeviceManager::AclDeviceManager()
{
    waitPost_ = std::make_unique<WaitPost>();
};

AclDeviceManager *AclDeviceManager::Instance()
{
    std::call_once(init_, []() { instance_ = std::make_unique<AclDeviceManager>(); });
    return instance_.get();
}

void AclDeviceManager::Init()
{
#ifdef BUILD_HETERO
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

Status AclDeviceManager::LoadPlugin()
{
    Dl_info dlInfo;
    Status lastRc = Status::OK();
    Raii raii([this]() { waitPost_->Set(); });
    if (dladdr(reinterpret_cast<void *>(AclDeviceManager::Instance), &dlInfo) == 0) {
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR,
                                FormatString("Load Ascend plugin failed, get dladdr error: %s", GetDlErrorMsg()));
    }
    std::string curSoPath = dlInfo.dli_fname;
    std::string aclPluginPath = std::string(dirname(curSoPath.data())) + "/" + AclPluginLibrary;
    // If the plugin library cannot be found with the symbols,
    // try traverse all the lib paths and see if it can be found.
    // Also avoid error logging.
    bool logError = false;
    if (FileSize(aclPluginPath, logError) < 0) {
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
            if (FileSize(path + "/" + AclPluginLibrary, logError) >= 0) {
                aclPluginPath = path + "/" + AclPluginLibrary;
                break;
            }
        }
    }
    LOG(INFO) << "aclPluginPath is " << aclPluginPath;
    RETURN_IF_NOT_OK(VerifyingSha256(aclPluginPath));
    pluginHandle_ = dlopen(aclPluginPath.c_str(), RTLD_LAZY | RTLD_LOCAL);
    if (pluginHandle_ == nullptr) {
        RETURN_STATUS_LOG_ERROR(K_INVALID,
                                FormatString("Load Ascend plugin failed, dlopen error: %s", GetDlErrorMsg()));
    } else {
        DlsymFuncObj();
    }
    return Status::OK();
}

void AclDeviceManager::DlsymFuncObj()
{
    DLSYM_FUNC_OBJ(MallocDeviceMemory, pluginHandle_);
    DLSYM_FUNC_OBJ(FreeDeviceMemory, pluginHandle_);
    DLSYM_FUNC_OBJ(MemCopyD2H, pluginHandle_);
    DLSYM_FUNC_OBJ(MemCopyH2D, pluginHandle_);
    DLSYM_FUNC_OBJ(MemCopyD2D, pluginHandle_);

    DLSYM_FUNC_OBJ(GetDeviceIdx, pluginHandle_);
    DLSYM_FUNC_OBJ(SetDeviceIdx, pluginHandle_);
    DLSYM_FUNC_OBJ(RtCreateStream, pluginHandle_);
    DLSYM_FUNC_OBJ(RtSynchronizeStream, pluginHandle_);
    DLSYM_FUNC_OBJ(RtSynchronizeStreamWithTimeout, pluginHandle_);
    DLSYM_FUNC_OBJ(RtDestroyStream, pluginHandle_);
    DLSYM_FUNC_OBJ(RtDestroyStreamForce, pluginHandle_);

    DLSYM_FUNC_OBJ(DSHcclGetRootInfo, pluginHandle_);
    DLSYM_FUNC_OBJ(DSHcclCommInitRootInfo, pluginHandle_);
    DLSYM_FUNC_OBJ(DSHcclSend, pluginHandle_);
    DLSYM_FUNC_OBJ(DSHcclRecv, pluginHandle_);
    DLSYM_FUNC_OBJ(DSHcclCommDestroy, pluginHandle_);
    DLSYM_FUNC_OBJ(DSAclrtRecordEvent, pluginHandle_);
    DLSYM_FUNC_OBJ(DSAclrtCreateEvent, pluginHandle_);
    DLSYM_FUNC_OBJ(DSAclrtSynchronizeEvent, pluginHandle_);
    DLSYM_FUNC_OBJ(DSAclrtSynchronizeEventWithTimeout, pluginHandle_);
    DLSYM_FUNC_OBJ(DSAclrtDestroyEvent, pluginHandle_);
    DLSYM_FUNC_OBJ(DSAclrtQueryEventStatus, pluginHandle_);
    DLSYM_FUNC_OBJ(DSHcclGetCommAsyncError, pluginHandle_);
    DLSYM_FUNC_OBJ(DSAclInit, pluginHandle_);
    DLSYM_FUNC_OBJ(DSAclrtSetDevice, pluginHandle_);
    DLSYM_FUNC_OBJ(DSAclFinalize, pluginHandle_);
    DLSYM_FUNC_OBJ(DSAclrtMemcpyAsync, pluginHandle_);
    DLSYM_FUNC_OBJ(DSAclrtResetDevice, pluginHandle_);
    DLSYM_FUNC_OBJ(DSAclrtMalloc, pluginHandle_);
    DLSYM_FUNC_OBJ(DSAclrtFree, pluginHandle_);
    DLSYM_FUNC_OBJ(DSAclrtGetDeviceCount, pluginHandle_);
    DLSYM_FUNC_OBJ(DSAclrtQueryDeviceStatus, pluginHandle_);
    DLSYM_FUNC_OBJ(DSAclrtMallocHost, pluginHandle_);
    DLSYM_FUNC_OBJ(DSAclrtFreeHost, pluginHandle_);

    DLSYM_FUNC_OBJ(DSP2PGetRootInfo, pluginHandle_);
    DLSYM_FUNC_OBJ(DSP2PCommInitRootInfo, pluginHandle_);
    DLSYM_FUNC_OBJ(DSP2PCommDestroy, pluginHandle_);
    DLSYM_FUNC_OBJ(DSP2PSend, pluginHandle_);
    DLSYM_FUNC_OBJ(DSP2PRecv, pluginHandle_);
    DLSYM_FUNC_OBJ(DSP2PGetCommAsyncError, pluginHandle_);

    DLSYM_FUNC_OBJ(DSRtNotifyCreate, pluginHandle_);
    DLSYM_FUNC_OBJ(DSRtNotifyDestroy, pluginHandle_);
    DLSYM_FUNC_OBJ(DSRtNotifyRecord, pluginHandle_);
    DLSYM_FUNC_OBJ(DSRtNotifyWait, pluginHandle_);

    DLSYM_FUNC_OBJ(DSRtGeneralCtrl, pluginHandle_);
    DLSYM_FUNC_OBJ(DSRtGetDeviceInfo, pluginHandle_);

    DLSYM_FUNC_OBJ(DSAclrtLaunchCallback, pluginHandle_);
    DLSYM_FUNC_OBJ(DSAclrtProcessReport, pluginHandle_);
    DLSYM_FUNC_OBJ(DSAclrtSubscribeReport, pluginHandle_);
    DLSYM_FUNC_OBJ(DSAclrtUnSubscribeReport, pluginHandle_);

    DLSYM_FUNC_OBJ(DSP2PRegisterHostMem, pluginHandle_);
    DLSYM_FUNC_OBJ(DSP2PImportHostSegment, pluginHandle_);
    DLSYM_FUNC_OBJ(DSP2PScatterBatchFromRemoteHostMem, pluginHandle_);
}

Status AclDeviceManager::CheckPluginOk()
{
    std::call_once(hasLoadPlugin_, []() { instance_->Init(); });
    Status loadStatus = waitPost_->WaitAndGetStatus();
    if (!loadStatus.IsOk()) {
        return loadStatus;
    }
    return Status::OK();
}

Status AclDeviceManager::VerifyDeviceId(std::vector<uint32_t> deviceIds)
{
    for (const auto devId : deviceIds) {
        RETURN_IF_NOT_OK(aclrtQueryDeviceStatus(devId));
    }
    return Status::OK();
}

Status AclDeviceManager::VerifyingSha256(const std::string &aclPluginPath)
{
    // Step 1: Verifying the File Size.
    auto fileSize = FileSize(aclPluginPath);
    CHECK_FAIL_RETURN_STATUS(fileSize >= 0, K_RUNTIME_ERROR, "Get file size failed");
    if (fileSize > ACLPLUGIN_SO_MAX_LIMIT) {
        RETURN_STATUS_LOG_ERROR(
            K_NOT_AUTHORIZED,
            FormatString("Load Ascend plugin failed. The size of the plugin file is %lld Byte, which exceeds the max "
                         "limit of %lld Byte.",
                         fileSize, ACLPLUGIN_SO_MAX_LIMIT));
    }
    // Step 2: Read the file.
    std::string fileContext;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ReadFileToString(aclPluginPath, fileContext),
                                     "Failed to read the libacl_plugin.so");
    // Step 3: Calculate the hash value.
    std::unique_ptr<unsigned char[]> outHashData;
    unsigned int outHashSize;
    Hasher hasher;
    RETURN_IF_NOT_OK(hasher.HashSHA256(fileContext.c_str(), fileContext.size(), outHashData, outHashSize));
    std::stringstream ss;
    int fieldWidth = 2;
    for (size_t i = 0; i < outHashSize; ++i) {
        ss << std::hex << std::setw(fieldWidth) << std::setfill('0') << (int)outHashData[i];
    }
    // Step 4: Check whether the hash values are consistent.
    if (ss.str() != ACL_PLUGIN_SHA256) {
        RETURN_STATUS_LOG_ERROR(
            K_NOT_AUTHORIZED,
            "Load Ascend plugin failed, which fails to pass the integrity check. "
            "Possible causes and solutions: "
            "1.This usually occurs when libacl_plugin.so and libdatasystem.so are from different versions. Ensure "
            "both libraries are from the same version.");
    }
    return Status::OK();
}

Status AclDeviceManager::MallocDeviceMemory(size_t dataSize, void *&deviceData)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    CHECK_FAIL_RETURN_STATUS(dataSize <= MAX_DEVICE_MALLOC_SIZE, K_INVALID, "The dataSize can't be greater then 12GB.");
    RETURN_RUNTIME_ERROR_IF_NULL(MallocDeviceMemoryFunc_);
    RETURN_ACL_RESULT(MallocDeviceMemoryFunc_(dataSize, deviceData));
}

Status AclDeviceManager::FreeDeviceMemory(void *deviceData)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(FreeDeviceMemoryFunc_);
    RETURN_ACL_RESULT(FreeDeviceMemoryFunc_(deviceData));
}

Status AclDeviceManager::GetDeviceIdx(int32_t &deviceIdx)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(GetDeviceIdxFunc_);
    int aclRet = GetDeviceIdxFunc_(deviceIdx);
    constexpr int normalCode = 0;  // ACL_RT_DEVICE_STATUS_NORMAL
    if (aclRet != normalCode) {
        RETURN_STATUS_LOG_ERROR(
            K_INVALID,
            FormatString(
                "May not create context or set device in this thread. Detail: acl api failed with error code %d",
                aclRet));
    }
    return Status::OK();
}

Status AclDeviceManager::SetDeviceIdx(int32_t deviceId)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(SetDeviceIdxFunc_);
    RETURN_ACL_RESULT(SetDeviceIdxFunc_(deviceId));
}

Status AclDeviceManager::MemCopyD2H(void *hostDst, size_t dstMaxSize, const void *devSrc, size_t srcSize)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(MemCopyD2HFunc_);
    RETURN_ACL_RESULT(MemCopyD2HFunc_(hostDst, dstMaxSize, devSrc, srcSize));
}

Status AclDeviceManager::MemCopyH2D(void *devDst, size_t dstMaxSize, const void *hostSrc, size_t srcSize)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(MemCopyH2DFunc_);
    RETURN_ACL_RESULT(MemCopyH2DFunc_(devDst, dstMaxSize, hostSrc, srcSize));
}

Status AclDeviceManager::MemCopyD2D(void *dst, size_t dstMaxSize, const void *src, size_t srcSize)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(MemCopyD2HFunc_);
    RETURN_ACL_RESULT(MemCopyD2DFunc_(dst, dstMaxSize, src, srcSize));
}

Status AclDeviceManager::RtCreateStream(aclrtStream *stream)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(RtCreateStreamFunc_);
    RETURN_ACL_RESULT(RtCreateStreamFunc_(stream));
}

Status AclDeviceManager::RtSynchronizeStream(aclrtStream stream)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(RtSynchronizeStreamFunc_);
    RETURN_ACL_RESULT(RtSynchronizeStreamFunc_(stream));
}

Status AclDeviceManager::RtSynchronizeStreamWithTimeout(aclrtStream stream, int32_t timeoutMs)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(RtSynchronizeStreamWithTimeoutFunc_);
    RETURN_ACL_RESULT(RtSynchronizeStreamWithTimeoutFunc_(stream, timeoutMs));
}

Status AclDeviceManager::RtDestroyStream(aclrtStream stream)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(RtDestroyStreamFunc_);
    RETURN_ACL_RESULT(RtDestroyStreamFunc_(stream));
}

Status AclDeviceManager::RtDestroyStreamForce(aclrtStream stream)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(RtDestroyStreamForceFunc_);
    RETURN_ACL_RESULT(RtDestroyStreamForceFunc_(stream));
}

Status AclDeviceManager::DSHcclGetRootInfo(HcclRootInfo *rootInfo)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSHcclGetRootInfoFunc_);
    int hcclRet = DSHcclGetRootInfoFunc_(rootInfo);
    if (hcclRet == 1) {  // HCCL_E_PARA = 1
        RETURN_STATUS(K_HCCL_ERROR,
            "HcclGetRootInfoapi failed with error code 1 (parameter error). Possible cause: HCCL failed to obtain IP "
            "address (null). Please check Ascend logs for detailed error information. "
            "Solution: If IP address acquisition failed, configure environment variable "
            "HCCL_IF_IP with the current host IP address.");
    }
    RETURN_HCCL_RESULT(hcclRet);
}

Status AclDeviceManager::DSHcclCommInitRootInfo(uint32_t nRanks, const HcclRootInfo *rootInfo, uint32_t rank,
                                                HcclComm *comm)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSHcclCommInitRootInfoFunc_);
    return HandleHcclResult(DSHcclCommInitRootInfoFunc_(nRanks, rootInfo, rank, comm));
}

Status AclDeviceManager::DSHcclSend(void *sendBuf, uint64_t count, HcclDataType dataType, uint32_t destRank,
                                    HcclComm comm, aclrtStream stream)
{
    PerfPoint point(PerfKey::DS_HCCL_SEND);
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSHcclSendFunc_);
    return HandleHcclResult(DSHcclSendFunc_(sendBuf, count, dataType, destRank, comm, stream));
}

Status AclDeviceManager::DSHcclRecv(void *recvBuf, uint64_t count, HcclDataType dataType, uint32_t srcRank,
                                    HcclComm comm, aclrtStream stream)
{
    PerfPoint point(PerfKey::DS_HCCl_RECEIVE);
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSHcclRecvFunc_);
    return HandleHcclResult(DSHcclRecvFunc_(recvBuf, count, dataType, srcRank, comm, stream));
}

Status AclDeviceManager::DSHcclCommDestroy(HcclComm comm)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSHcclCommDestroyFunc_);
    RETURN_HCCL_RESULT(DSHcclCommDestroyFunc_(comm));
}

Status AclDeviceManager::DSAclrtCreateEvent(aclrtEvent *event)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSAclrtCreateEventFunc_);
    RETURN_ACL_RESULT(DSAclrtCreateEventFunc_(event));
}

Status AclDeviceManager::DSAclrtRecordEvent(aclrtEvent event, aclrtStream stream)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSAclrtRecordEventFunc_);
    RETURN_ACL_RESULT(DSAclrtRecordEventFunc_(event, stream));
}

Status AclDeviceManager::DSAclrtSynchronizeEvent(aclrtEvent event)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSAclrtSynchronizeEventFunc_);
    RETURN_ACL_RESULT(DSAclrtSynchronizeEventFunc_(event));
}

Status AclDeviceManager::DSAclrtSynchronizeEventWithTimeout(aclrtEvent event, int32_t timeoutMs)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSAclrtSynchronizeEventWithTimeoutFunc_);
    RETURN_ACL_RESULT(DSAclrtSynchronizeEventWithTimeoutFunc_(event, timeoutMs));
}

Status AclDeviceManager::DSAclrtDestroyEvent(aclrtEvent event)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSAclrtDestroyEventFunc_);
    RETURN_ACL_RESULT(DSAclrtDestroyEventFunc_(event));
}

Status AclDeviceManager::DSHcclGetCommAsyncError(HcclComm comm)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSHcclGetCommAsyncErrorFunc_);
    HcclResult asyncError;
    HandleHcclResult(DSHcclGetCommAsyncErrorFunc_(comm, &asyncError));
    return HandleHcclResult(asyncError);
}

Status AclDeviceManager::aclInit(const char *configPath)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSAclInitFunc_);
    RETURN_ACL_RESULT(DSAclInitFunc_(configPath));
}

Status AclDeviceManager::aclrtSetDevice(int deviceId)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSAclrtSetDeviceFunc_);
    RETURN_ACL_RESULT(DSAclrtSetDeviceFunc_(deviceId));
}
Status AclDeviceManager::aclFinalize()
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSAclFinalizeFunc_);
    RETURN_ACL_RESULT(DSAclFinalizeFunc_());
}

Status AclDeviceManager::aclrtMemcpyAsync(void *dst, size_t destMax, const void *src, size_t count,
                                          aclrtMemcpyKind kind, aclrtStream stream)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSAclrtMemcpyAsyncFunc_);
    RETURN_ACL_RESULT(DSAclrtMemcpyAsyncFunc_(dst, destMax, src, count, kind, stream));
}

Status AclDeviceManager::aclrtResetDevice(int32_t deviceId)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSAclrtMemcpyAsyncFunc_);
    RETURN_ACL_RESULT(DSAclrtResetDeviceFunc_(deviceId));
}

Status AclDeviceManager::aclrtMalloc(void **devPtr, size_t size, aclrtMemMallocPolicy policy)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSAclrtMallocFunc_);
    RETURN_ACL_RESULT(DSAclrtMallocFunc_(devPtr, size, policy));
}

Status AclDeviceManager::aclrtFree(void *devPtr)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSAclrtFreeFunc_);
    RETURN_ACL_RESULT(DSAclrtFreeFunc_(devPtr));
}

Status AclDeviceManager::aclrtMallocHost(void **devPtr, size_t size)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSAclrtMallocHostFunc_);
    RETURN_ACL_RESULT(DSAclrtMallocHostFunc_(devPtr, size));
}

Status AclDeviceManager::aclrtFreeHost(void *devPtr)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSAclrtFreeHostFunc_);
    RETURN_ACL_RESULT(DSAclrtFreeHostFunc_(devPtr));
}

Status AclDeviceManager::aclrtGetDeviceCount(uint32_t *count)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSAclrtGetDeviceCountFunc_);
    RETURN_ACL_RESULT(DSAclrtGetDeviceCountFunc_(count));
}

Status AclDeviceManager::aclrtQueryDeviceStatus(uint32_t deviceId)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSAclrtQueryDeviceStatusFunc_);
    int32_t deviceStatus;
    int aclRet = DSAclrtQueryDeviceStatusFunc_(deviceId, &deviceStatus);
    constexpr int normalCode = 0;  // ACL_RT_DEVICE_STATUS_NORMAL
    if (aclRet != normalCode) {
        RETURN_STATUS_LOG_ERROR(
            K_INVALID,
            FormatString(
                "Got Error/ABNORMAL device, deviceId: %d. Detail: acl api failed with error code %d, deviceStatus: %d",
                static_cast<int32_t>(deviceId), aclRet, deviceStatus));
    }
    return Status::OK();
}

AclDeviceManager::~AclDeviceManager()
{
    Shutdown();
}

void AclDeviceManager::Shutdown()
{
    if (loadPluginThread_ != nullptr) {
        loadPluginThread_->join();
        loadPluginThread_.reset();
    }
}

Status AclDeviceManager::DSAclrtQueryEventStatus(aclrtEvent event)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSAclrtQueryEventStatusFunc_);
    CHECK_FAIL_RETURN_STATUS(event != nullptr, K_RUNTIME_ERROR, "Event is nullptr");
    aclrtEventRecordedStatus recordedStatus;
    auto aclResult = DSAclrtQueryEventStatusFunc_(event, &recordedStatus);
    if (aclResult != 0) {
        RETURN_ACL_RESULT(aclResult);
    }
    CHECK_FAIL_RETURN_STATUS(
        recordedStatus == aclrtEventRecordedStatus::ACL_EVENT_RECORDED_STATUS_COMPLETE, K_ACL_ERROR,
        "The event is not recorded in the stream, or the event recorded in the stream is not executed or fails "
        "to be executed.");
    return Status::OK();
}

Status AclDeviceManager::DSP2PGetRootInfo(HcclRootInfo *rootInfo)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSP2PGetRootInfoFunc_);
    RETURN_HCCL_RESULT(DSP2PGetRootInfoFunc_(rootInfo));
}

Status AclDeviceManager::DSP2PCommInitRootInfo(const HcclRootInfo *rootInfo, P2pKind kind, P2pLink link, P2PComm *comm)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSP2PCommInitRootInfoFunc_);
    RETURN_HCCL_RESULT(DSP2PCommInitRootInfoFunc_(rootInfo, kind, link, comm));
}

Status AclDeviceManager::DSP2PCommDestroy(P2PComm comm)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSP2PCommDestroyFunc_);
    RETURN_HCCL_RESULT(DSP2PCommDestroyFunc_(comm));
}

Status AclDeviceManager::DSP2PSend(void *sendBuf, uint64_t count, HcclDataType dataType, P2PComm comm,
                                   aclrtStream stream)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSP2PSendFunc_);
    RETURN_HCCL_RESULT(DSP2PSendFunc_(sendBuf, count, dataType, comm, stream));
}

Status AclDeviceManager::DSP2PRecv(void *recvBuf, uint64_t count, HcclDataType dataType, P2PComm comm,
                                   aclrtStream stream)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSP2PRecvFunc_);
    RETURN_HCCL_RESULT(DSP2PRecvFunc_(recvBuf, count, dataType, comm, stream));
}

Status AclDeviceManager::DSP2PGetCommAsyncError(P2PComm comm)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSP2PGetCommAsyncErrorFunc_);
    HcclResult asyncError;
    HandleHcclResult(DSP2PGetCommAsyncErrorFunc_(comm, &asyncError));
    return HandleHcclResult(asyncError);
}

Status AclDeviceManager::RtNotifyCreate(int32_t deviceId, void **notify)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSRtNotifyCreateFunc_);
    RETURN_ACL_RESULT(DSRtNotifyCreateFunc_(deviceId, notify));
}

Status AclDeviceManager::RtNotifyDestroy(void *notify)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSRtNotifyDestroyFunc_);
    RETURN_ACL_RESULT(DSRtNotifyDestroyFunc_(notify));
}

Status AclDeviceManager::RtNotifyRecord(void *notify, void *stream)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSRtNotifyRecordFunc_);
    RETURN_ACL_RESULT(DSRtNotifyRecordFunc_(notify, stream));
}
Status AclDeviceManager::RtNotifyWait(void *notify, void *stream)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSRtNotifyWaitFunc_);
    RETURN_ACL_RESULT(DSRtNotifyWaitFunc_(notify, stream));
}

Status AclDeviceManager::RtGeneralCtrl(uintptr_t *ctrl, uint32_t num, uint32_t type)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSRtGeneralCtrlFunc_);
    RETURN_ACL_RESULT(DSRtGeneralCtrlFunc_(ctrl, num, type));
}

Status AclDeviceManager::RtGetDeviceInfo(uint32_t deviceId, int32_t moduleType, int32_t infoType, int64_t *val)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSRtGetDeviceInfoFunc_);
    RETURN_ACL_RESULT(DSRtGetDeviceInfoFunc_(deviceId, moduleType, infoType, val));
}

Status AclDeviceManager::AclrtLaunchCallback(aclrtCallback fn, void *userData, aclrtCallbackBlockType blockType,
                                             aclrtStream stream)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSAclrtLaunchCallbackFunc_);
    RETURN_ACL_RESULT(DSAclrtLaunchCallbackFunc_(fn, userData, blockType, stream));
}

Status AclDeviceManager::AclrtProcessReport(int32_t timeout)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSAclrtProcessReportFunc_);
    RETURN_ACL_RESULT(DSAclrtProcessReportFunc_(timeout));
}

Status AclDeviceManager::AclrtSubscribeReport(uint64_t threadId, aclrtStream stream)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSAclrtSubscribeReportFunc_);
    RETURN_ACL_RESULT(DSAclrtSubscribeReportFunc_(threadId, stream));
}

Status AclDeviceManager::AclrtUnSubscribeReport(uint64_t threadId, aclrtStream stream)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSAclrtUnSubscribeReportFunc_);
    RETURN_ACL_RESULT(DSAclrtUnSubscribeReportFunc_(threadId, stream));
}

Status AclDeviceManager::HandleHcclResult(int hcclResult)
{
    constexpr int hcclEUnavail = 7;      // HCCL_E_UNAVAIL
    constexpr int hcclERemote = 21;      // HCCL_E_REMOTE
    constexpr int hcclESuspending = 22;  // HCCL_E_SUSPENDING
    switch (hcclResult) {
        case hcclEUnavail:
            return Status(StatusCode::K_HCCL_ERROR, __LINE__, __FILE__,
                          "HCCL api operation failed with error code: 7 (resource unavailable). "
                          "Possible causes:"
                          "1. NPU are occupied or device unavailability.");

        case hcclERemote:
            return Status(StatusCode::K_HCCL_ERROR, __LINE__, __FILE__,
                          "HCCL api operation failed with error code: 21 (error cqe). Indicates that an 'RDMA ERROR "
                          "CQE' error has occurred within this communication domain.");

        case hcclESuspending:
            return Status(StatusCode::K_HCCL_ERROR, __LINE__, __FILE__,
                          "HCCL api operation failed with error code: 22 (error communicator suspending). "
                          "This usually occurs when the device state was reset unexpectedly, "
                          "causing the communication domain to be destroyed. "
                          "Possible causes:"
                          "1. Device reset operation after HCCL communicator initialization.");

        default:
            RETURN_HCCL_RESULT(hcclResult);
    }
}

Status AclDeviceManager::DSP2PRegisterHostMem(void *hostBuf, uint64_t size, P2pSegmentInfo *segmentInfo,
                                              P2pSegmentPermissions permissions)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSP2PRegisterHostMemFunc_);
    RETURN_ACL_RESULT(DSP2PRegisterHostMemFunc_(hostBuf, size, segmentInfo, permissions));
}

Status AclDeviceManager::DSP2PImportHostSegment(P2pSegmentInfo segmentInfo)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSP2PRegisterHostMemFunc_);
    RETURN_ACL_RESULT(DSP2PImportHostSegmentFunc_(segmentInfo));
}

Status AclDeviceManager::DSP2PScatterBatchFromRemoteHostMem(P2pScatterEntry *entries, uint32_t batchSize, P2PComm comm,
                                                            aclrtStream stream)
{
    RETURN_IF_NOT_OK(CheckPluginOk());
    RETURN_RUNTIME_ERROR_IF_NULL(DSP2PScatterBatchFromRemoteHostMemFunc_);
    RETURN_ACL_RESULT(DSP2PScatterBatchFromRemoteHostMemFunc_(entries, batchSize, comm, stream));
}

// ==================== DeviceManagerBase Interface Implementation ====================

Status AclDeviceManager::Init(const char *configPath)
{
    return aclInit(configPath);
}

Status AclDeviceManager::Finalize()
{
    return aclFinalize();
}

Status AclDeviceManager::GetDeviceCount(uint32_t *count)
{
    return aclrtGetDeviceCount(count);
}

Status AclDeviceManager::QueryDeviceStatus(uint32_t deviceId)
{
    return aclrtQueryDeviceStatus(deviceId);
}

Status AclDeviceManager::SetDevice(int32_t deviceId)
{
    return SetDeviceIdx(deviceId);
}

Status AclDeviceManager::ResetDevice(int32_t deviceId)
{
    return aclrtResetDevice(deviceId);
}

Status AclDeviceManager::Malloc(void **devPtr, size_t size, MemMallocPolicy policy)
{
    aclrtMemMallocPolicy aclPolicy = {};
    RETURN_IF_NOT_OK(ToAclMemMallocPolicy(policy, aclPolicy));
    return aclrtMalloc(devPtr, size, aclPolicy);
}

Status AclDeviceManager::Free(void *devPtr)
{
    return aclrtFree(devPtr);
}

Status AclDeviceManager::MallocHost(void **hostPtr, size_t size)
{
    return aclrtMallocHost(hostPtr, size);
}

Status AclDeviceManager::FreeHost(void *hostPtr)
{
    return aclrtFreeHost(hostPtr);
}

Status AclDeviceManager::MemcpyAsync(void *dst, size_t dstMaxSize, const void *src, size_t count,
                                     MemcpyKind kind, void *stream)
{
    aclrtMemcpyKind aclKind = {};
    RETURN_IF_NOT_OK(ToAclMemcpyKind(kind, aclKind));
    return aclrtMemcpyAsync(dst, dstMaxSize, src, count, aclKind,
                           static_cast<aclrtStream>(stream));
}

Status AclDeviceManager::CreateStream(void **stream)
{
    return RtCreateStream(reinterpret_cast<aclrtStream *>(stream));
}

Status AclDeviceManager::SynchronizeStream(void *stream)
{
    return RtSynchronizeStream(static_cast<aclrtStream>(stream));
}

Status AclDeviceManager::SynchronizeStreamWithTimeout(void *stream, int32_t timeoutMs)
{
    return RtSynchronizeStreamWithTimeout(static_cast<aclrtStream>(stream), timeoutMs);
}

Status AclDeviceManager::DestroyStream(void *stream)
{
    return RtDestroyStream(static_cast<aclrtStream>(stream));
}

Status AclDeviceManager::DestroyStreamForce(void *stream)
{
    return RtDestroyStreamForce(static_cast<aclrtStream>(stream));
}

Status AclDeviceManager::CreateEvent(void **event)
{
    return DSAclrtCreateEvent(reinterpret_cast<aclrtEvent *>(event));
}

Status AclDeviceManager::RecordEvent(void *event, void *stream)
{
    return DSAclrtRecordEvent(static_cast<aclrtEvent>(event), static_cast<aclrtStream>(stream));
}

Status AclDeviceManager::SynchronizeEvent(void *event)
{
    return DSAclrtSynchronizeEvent(static_cast<aclrtEvent>(event));
}

Status AclDeviceManager::SynchronizeEventWithTimeout(void *event, int32_t timeoutMs)
{
    return DSAclrtSynchronizeEventWithTimeout(static_cast<aclrtEvent>(event), timeoutMs);
}

Status AclDeviceManager::DestroyEvent(void *event)
{
    return DSAclrtDestroyEvent(static_cast<aclrtEvent>(event));
}

Status AclDeviceManager::QueryEventStatus(void *event)
{
    return DSAclrtQueryEventStatus(static_cast<aclrtEvent>(event));
}

Status AclDeviceManager::CommGetRootInfo(CommRootInfo *rootInfo)
{
    HcclRootInfo *hcclRootInfo = nullptr;
    RETURN_IF_NOT_OK(ToHcclRootInfo(rootInfo, hcclRootInfo));
    return DSHcclGetRootInfo(hcclRootInfo);
}

Status AclDeviceManager::CommInitRootInfo(uint32_t nRanks, const CommRootInfo *rootInfo, uint32_t rank, void **comm)
{
    const HcclRootInfo *hcclRootInfo = nullptr;
    RETURN_IF_NOT_OK(ToHcclRootInfo(rootInfo, hcclRootInfo));
    return DSHcclCommInitRootInfo(nRanks, hcclRootInfo, rank, reinterpret_cast<HcclComm *>(comm));
}

Status AclDeviceManager::CommSend(void *sendBuf, uint64_t count, CommDataType dataType, uint32_t destRank,
                                  void *comm, void *stream)
{
    HcclDataType hcclDataType = {};
    RETURN_IF_NOT_OK(ToHcclDataType(dataType, hcclDataType));
    return DSHcclSend(sendBuf, count, hcclDataType, destRank,
                      static_cast<HcclComm>(comm), static_cast<aclrtStream>(stream));
}

Status AclDeviceManager::CommRecv(void *recvBuf, uint64_t count, CommDataType dataType, uint32_t srcRank,
                                  void *comm, void *stream)
{
    HcclDataType hcclDataType = {};
    RETURN_IF_NOT_OK(ToHcclDataType(dataType, hcclDataType));
    return DSHcclRecv(recvBuf, count, hcclDataType, srcRank,
                      static_cast<HcclComm>(comm), static_cast<aclrtStream>(stream));
}

Status AclDeviceManager::CommDestroy(void *comm)
{
    return DSHcclCommDestroy(static_cast<HcclComm>(comm));
}

Status AclDeviceManager::CommGetAsyncError(void *comm)
{
    return DSHcclGetCommAsyncError(static_cast<HcclComm>(comm));
}

Status AclDeviceManager::P2PGetRootInfo(CommRootInfo *rootInfo)
{
    return DSP2PGetRootInfo(reinterpret_cast<HcclRootInfo *>(rootInfo));
}

Status AclDeviceManager::P2PCommInitRootInfo(const CommRootInfo *rootInfo, P2pKindBase kind, P2pLinkBase link,
                                             void **comm)
{
    P2pKind p2pKind = {};
    RETURN_IF_NOT_OK(ToP2pKind(kind, p2pKind));
    
    P2pLink p2pLink = {};
    RETURN_IF_NOT_OK(ToP2pLink(link, p2pLink));
    
    return DSP2PCommInitRootInfo(reinterpret_cast<const HcclRootInfo *>(rootInfo),
                                 p2pKind,
                                 p2pLink,
                                 reinterpret_cast<P2PComm *>(comm));
}

Status AclDeviceManager::P2PCommDestroy(void *comm)
{
    return DSP2PCommDestroy(static_cast<P2PComm>(comm));
}

Status AclDeviceManager::P2PSend(void *sendBuf, uint64_t count, CommDataType dataType, void *comm, void *stream)
{
    HcclDataType hcclDataType = {};
    RETURN_IF_NOT_OK(ToHcclDataType(dataType, hcclDataType));
    return DSP2PSend(sendBuf, count, hcclDataType,
                     static_cast<P2PComm>(comm), static_cast<aclrtStream>(stream));
}

Status AclDeviceManager::P2PRecv(void *recvBuf, uint64_t count, CommDataType dataType, void *comm, void *stream)
{
    HcclDataType hcclDataType = {};
    RETURN_IF_NOT_OK(ToHcclDataType(dataType, hcclDataType));
    return DSP2PRecv(recvBuf, count, hcclDataType,
                     static_cast<P2PComm>(comm), static_cast<aclrtStream>(stream));
}

Status AclDeviceManager::P2PGetCommAsyncError(void *comm)
{
    return DSP2PGetCommAsyncError(static_cast<P2PComm>(comm));
}

Status AclDeviceManager::NotifyCreate(int32_t deviceId, void **notify)
{
    return RtNotifyCreate(deviceId, notify);
}

Status AclDeviceManager::NotifyDestroy(void *notify)
{
    return RtNotifyDestroy(notify);
}

Status AclDeviceManager::NotifyRecord(void *notify, void *stream)
{
    return RtNotifyRecord(notify, stream);
}

Status AclDeviceManager::NotifyWait(void *notify, void *stream)
{
    return RtNotifyWait(notify, stream);
}

Status AclDeviceManager::LaunchCallback(StreamCallback fn, void *userData, CallbackBlockType blockType, void *stream)
{
    aclrtCallbackBlockType aclBlockType = {};
    RETURN_IF_NOT_OK(ToAclCallbackBlockType(blockType, aclBlockType));
    return AclrtLaunchCallback(reinterpret_cast<aclrtCallback>(fn), userData,
                               aclBlockType, static_cast<aclrtStream>(stream));
}

Status AclDeviceManager::ProcessReport(int32_t timeout)
{
    return AclrtProcessReport(timeout);
}

Status AclDeviceManager::SubscribeReport(uint64_t threadId, void *stream)
{
    return AclrtSubscribeReport(threadId, static_cast<aclrtStream>(stream));
}

Status AclDeviceManager::UnSubscribeReport(uint64_t threadId, void *stream)
{
    return AclrtUnSubscribeReport(threadId, static_cast<aclrtStream>(stream));
}

Status AclDeviceManager::GeneralCtrl(uintptr_t *ctrl, uint32_t num, uint32_t type)
{
    return RtGeneralCtrl(ctrl, num, type);
}

Status AclDeviceManager::GetDeviceInfo(uint32_t deviceId, int32_t moduleType, int32_t infoType, int64_t *val)
{
    return RtGetDeviceInfo(deviceId, moduleType, infoType, val);
}

Status AclDeviceManager::P2PRegisterHostMem(void *hostBuf, uint64_t size, P2pSegmentBase *segmentInfo,
                                            P2pSegmentPermBase permissions)
{
    P2pSegmentInfo *p2pSegmentInfo = nullptr;
    RETURN_IF_NOT_OK(ToP2pSegmentInfo(segmentInfo, p2pSegmentInfo));
    
    P2pSegmentPermissions p2pPermissions = {};
    RETURN_IF_NOT_OK(ToP2pSegmentPermissions(permissions, p2pPermissions));
    
    return DSP2PRegisterHostMem(hostBuf, size, p2pSegmentInfo, p2pPermissions);
}

Status AclDeviceManager::P2PImportHostSegment(P2pSegmentBase segmentInfo)
{
    P2pSegmentInfo p2pSegmentInfo = {};
    RETURN_IF_NOT_OK(ToP2pSegmentInfo(segmentInfo, p2pSegmentInfo));
    return DSP2PImportHostSegment(p2pSegmentInfo);
}

Status AclDeviceManager::P2PScatterBatchFromRemoteHostMem(P2pScatterBase *entries, uint32_t batchSize,
                                                          void *comm, void *stream)
{
    // Convert P2pScatterBase array to P2pScatterEntry array
    std::vector<P2pScatterEntry> nativeEntries(batchSize);
    for (uint32_t i = 0; i < batchSize; ++i) {
        RETURN_IF_NOT_OK(ToP2pScatterEntry(entries[i], nativeEntries[i]));
    }
    return DSP2PScatterBatchFromRemoteHostMem(nativeEntries.data(), batchSize,
                                              static_cast<P2PComm>(comm), static_cast<aclrtStream>(stream));
}
}  // namespace acl
}  // namespace datasystem
