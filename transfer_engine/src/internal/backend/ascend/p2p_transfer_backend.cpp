#include "internal/backend/ascend/p2p_transfer_backend.h"

#include <dlfcn.h>
#include <limits.h>
#include <unistd.h>

#include <cstdlib>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <filesystem>
#include <thread>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "internal/log/environment_dump.h"
#include "internal/log/logging.h"
#include "p2p.h"

#include "datasystem/transfer_engine/status_helper.h"

namespace datasystem {

namespace {

void *TryOpenLib(const std::vector<std::string> &candidates, std::string *debugErr)
{
    std::string errors;
    for (const auto &path : candidates) {
        if (path.empty()) {
            continue;
        }
        void *handle = dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
        if (handle != nullptr) {
            if (debugErr != nullptr) {
                *debugErr = "";
            }
            return handle;
        }
        const char *err = dlerror();
        errors += "[" + path + "] ";
        errors += (err == nullptr ? "unknown dlopen error" : err);
        errors += "; ";
    }
    if (debugErr != nullptr) {
        *debugErr = errors;
    }
    return nullptr;
}

std::vector<std::string> BuildP2PLoadCandidates()
{
    std::vector<std::string> candidates;
    if (const char *envPath = std::getenv("TRANSFER_ENGINE_P2P_SO_PATH"); envPath != nullptr && envPath[0] != '\0') {
        candidates.emplace_back(envPath);
    }

    candidates.emplace_back("libp2p_transfer.so");
    candidates.emplace_back("libp2p_transfer.so.0");

    char exeBuf[PATH_MAX] = {0};
    const ssize_t len = readlink("/proc/self/exe", exeBuf, sizeof(exeBuf) - 1);
    if (len > 0) {
        std::filesystem::path exePath(std::string(exeBuf, static_cast<size_t>(len)));
        std::filesystem::path exeDir = exePath.parent_path();
        candidates.emplace_back((exeDir / "libp2p_transfer.so").string());
        candidates.emplace_back((exeDir / "lib" / "libp2p_transfer.so").string());
        candidates.emplace_back((exeDir / "ascend_p2p_transfer_build" / "libp2p_transfer.so").string());
        candidates.emplace_back((exeDir / "ascend_p2p_transfer_build" / "libp2p_transfer.so.0").string());
        candidates.emplace_back((exeDir.parent_path() / "lib" / "libp2p_transfer.so").string());
        candidates.emplace_back((exeDir.parent_path() / "ascend_p2p_transfer_build" / "libp2p_transfer.so").string());
        candidates.emplace_back(
            (exeDir.parent_path() / "ascend_p2p_transfer_build" / "libp2p_transfer.so.0").string());
    }
    return candidates;
}

template <typename T>
T LoadSymbol(void *handle, const char *name)
{
    return reinterpret_cast<T>(dlsym(handle, name));
}

Result ToStatus(HcclResult rc, const std::string &where)
{
    if (rc == HCCL_SUCCESS) {
        return Result::OK();
    }
    return TE_MAKE_STATUS(ErrorCode::kRuntimeError, where + " failed, hccl rc=" + std::to_string(static_cast<int>(rc)));
}

Result ToStatusAcl(aclError rc, const std::string &where)
{
    if (rc == ACL_ERROR_NONE) {
        return Result::OK();
    }
    return TE_MAKE_STATUS(ErrorCode::kRuntimeError, where + " failed, acl rc=" + std::to_string(static_cast<int>(rc)));
}

}  // namespace

struct P2PTransferBackend::Impl {
    using FnGetRootInfo = HcclResult (*)(HcclRootInfo *);
    using FnCommInit = HcclResult (*)(const HcclRootInfo *, P2pKind, P2pLink, P2PComm *);
    using FnCommDestroy = HcclResult (*)(P2PComm);
    using FnSend = HcclResult (*)(void *, uint64_t, HcclDataType, P2PComm, aclrtStream);
    using FnRecv = HcclResult (*)(void *, uint64_t, HcclDataType, P2PComm, aclrtStream);
    using FnGetCommAsyncError = HcclResult (*)(P2PComm, HcclResult *);

    using FnAclCreateStream = aclError (*)(aclrtStream *);
    using FnAclDestroyStream = aclError (*)(aclrtStream);
    using FnAclSynchronizeStream = aclError (*)(aclrtStream);
    using FnAclCreateEvent = aclError (*)(aclrtEvent *);
    using FnAclDestroyEvent = aclError (*)(aclrtEvent);
    using FnAclRecordEvent = aclError (*)(aclrtEvent, aclrtStream);
    using FnAclSynchronizeEvent = aclError (*)(aclrtEvent);
    using FnAclSynchronizeEventWithTimeout = aclError (*)(aclrtEvent, int32_t);
    using FnAclGetCurrentContext = aclError (*)(aclrtContext *);
    using FnAclSetDevice = aclError (*)(int32_t);

    struct CommPair {
        P2PComm sendComm = nullptr;
        P2PComm recvComm = nullptr;
        aclrtStream sendStream = nullptr;
        aclrtStream recvStream = nullptr;
        aclrtEvent recvEvent = nullptr;
        int32_t localDeviceId = -1;
        uint32_t recvPostedCount = 0;
    };

    void *p2pLibHandle = nullptr;
    void *aclLibHandle = nullptr;

    FnGetRootInfo getRootInfo = nullptr;
    FnCommInit commInit = nullptr;
    FnCommDestroy commDestroy = nullptr;
    FnSend send = nullptr;
    FnRecv recv = nullptr;
    FnGetCommAsyncError getCommAsyncError = nullptr;

    FnAclCreateStream aclCreateStream = nullptr;
    FnAclDestroyStream aclDestroyStream = nullptr;
    FnAclSynchronizeStream aclSynchronizeStream = nullptr;
    FnAclCreateEvent aclCreateEvent = nullptr;
    FnAclDestroyEvent aclDestroyEvent = nullptr;
    FnAclRecordEvent aclRecordEvent = nullptr;
    FnAclSynchronizeEvent aclSynchronizeEvent = nullptr;
    FnAclSynchronizeEventWithTimeout aclSynchronizeEventWithTimeout = nullptr;
    FnAclGetCurrentContext aclGetCurrentContext = nullptr;
    FnAclSetDevice aclSetDevice = nullptr;

    std::unordered_map<std::string, CommPair> comms;
    std::mutex cleanupMutex;
    std::condition_variable cleanupCv;
    std::condition_variable cleanupDoneCv;
    std::deque<CommPair> cleanupQueue;
    size_t cleanupInFlight = 0;
    bool cleanupWorkerExited = false;
    bool cleanupStop = false;
    std::thread cleanupThread;

    std::string p2pLoadError;
    std::string p2pSymbolError;

    ~Impl()
    {
        if (p2pLibHandle != nullptr) {
            dlclose(p2pLibHandle);
        }
        if (aclLibHandle != nullptr) {
            dlclose(aclLibHandle);
        }
    }
};

P2PTransferBackend::P2PTransferBackend() : impl_(std::make_shared<Impl>())
{
    internal::EnsureGlogInitialized();
    LOG(INFO) << "p2p backend initialize begin";
    internal::DumpProcessEnvironment("p2p_backend_ctor_begin");
    const std::vector<std::string> candidates = BuildP2PLoadCandidates();
    for (size_t i = 0; i < candidates.size(); ++i) {
        LOG(INFO) << "p2p backend dlopen candidate[" << i << "]=" << candidates[i];
    }
    impl_->p2pLibHandle = TryOpenLib(candidates, &impl_->p2pLoadError);
    impl_->aclLibHandle = dlopen("libascendcl.so", RTLD_NOW | RTLD_LOCAL);

    if (impl_->p2pLibHandle != nullptr) {
        impl_->getRootInfo = LoadSymbol<Impl::FnGetRootInfo>(impl_->p2pLibHandle, "P2PGetRootInfo");
        impl_->commInit = LoadSymbol<Impl::FnCommInit>(impl_->p2pLibHandle, "P2PCommInitRootInfo");
        impl_->commDestroy = LoadSymbol<Impl::FnCommDestroy>(impl_->p2pLibHandle, "P2PCommDestroy");
        impl_->send = LoadSymbol<Impl::FnSend>(impl_->p2pLibHandle, "P2PSend");
        impl_->recv = LoadSymbol<Impl::FnRecv>(impl_->p2pLibHandle, "P2PRecv");
        impl_->getCommAsyncError = LoadSymbol<Impl::FnGetCommAsyncError>(impl_->p2pLibHandle, "P2PGetCommAsyncError");

        if (impl_->getRootInfo == nullptr) {
            impl_->p2pSymbolError += "missing symbol: P2PGetRootInfo; ";
        }
        if (impl_->commInit == nullptr) {
            impl_->p2pSymbolError += "missing symbol: P2PCommInitRootInfo; ";
        }
        if (impl_->commDestroy == nullptr) {
            impl_->p2pSymbolError += "missing symbol: P2PCommDestroy; ";
        }
        if (impl_->send == nullptr) {
            impl_->p2pSymbolError += "missing symbol: P2PSend; ";
        }
        if (impl_->recv == nullptr) {
            impl_->p2pSymbolError += "missing symbol: P2PRecv; ";
        }
        if (impl_->getCommAsyncError == nullptr) {
            impl_->p2pSymbolError += "missing symbol: P2PGetCommAsyncError; ";
        }
    }
    LOG(INFO) << "p2p backend p2p library load result, loaded=" << (impl_->p2pLibHandle != nullptr)
              << ", symbol_error=" << (impl_->p2pSymbolError.empty() ? "none" : impl_->p2pSymbolError)
              << ", load_error=" << (impl_->p2pLoadError.empty() ? "none" : impl_->p2pLoadError);

    if (impl_->aclLibHandle != nullptr) {
        impl_->aclCreateStream = LoadSymbol<Impl::FnAclCreateStream>(impl_->aclLibHandle, "aclrtCreateStream");
        impl_->aclDestroyStream = LoadSymbol<Impl::FnAclDestroyStream>(impl_->aclLibHandle, "aclrtDestroyStream");
        impl_->aclSynchronizeStream = LoadSymbol<Impl::FnAclSynchronizeStream>(impl_->aclLibHandle, "aclrtSynchronizeStream");
        impl_->aclCreateEvent = LoadSymbol<Impl::FnAclCreateEvent>(impl_->aclLibHandle, "aclrtCreateEvent");
        impl_->aclDestroyEvent = LoadSymbol<Impl::FnAclDestroyEvent>(impl_->aclLibHandle, "aclrtDestroyEvent");
        impl_->aclRecordEvent = LoadSymbol<Impl::FnAclRecordEvent>(impl_->aclLibHandle, "aclrtRecordEvent");
        impl_->aclSynchronizeEvent = LoadSymbol<Impl::FnAclSynchronizeEvent>(impl_->aclLibHandle, "aclrtSynchronizeEvent");
        impl_->aclSynchronizeEventWithTimeout =
            LoadSymbol<Impl::FnAclSynchronizeEventWithTimeout>(impl_->aclLibHandle, "aclrtSynchronizeEventWithTimeout");
        impl_->aclGetCurrentContext = LoadSymbol<Impl::FnAclGetCurrentContext>(impl_->aclLibHandle, "aclrtGetCurrentContext");
        impl_->aclSetDevice = LoadSymbol<Impl::FnAclSetDevice>(impl_->aclLibHandle, "aclrtSetDevice");
    }
    LOG(INFO) << "p2p backend acl library load result, loaded=" << (impl_->aclLibHandle != nullptr);

    std::shared_ptr<Impl> workerImpl = impl_;
    impl_->cleanupThread = std::thread([workerImpl]() {
        auto destroyPair = [workerImpl](Impl::CommPair &pair) {
            bool aclContextReady = true;
            if (pair.localDeviceId >= 0 && workerImpl->aclSetDevice != nullptr) {
                aclContextReady = (workerImpl->aclSetDevice(pair.localDeviceId) == ACL_ERROR_NONE);
            }
            if (workerImpl->commDestroy != nullptr) {
                if (pair.sendComm != nullptr) {
                    (void)workerImpl->commDestroy(pair.sendComm);
                    pair.sendComm = nullptr;
                }
                if (pair.recvComm != nullptr) {
                    (void)workerImpl->commDestroy(pair.recvComm);
                    pair.recvComm = nullptr;
                }
            }
            if (aclContextReady && workerImpl->aclDestroyEvent != nullptr && pair.recvEvent != nullptr) {
                (void)workerImpl->aclDestroyEvent(pair.recvEvent);
                pair.recvEvent = nullptr;
            }
            if (aclContextReady && workerImpl->aclDestroyStream != nullptr) {
                if (pair.sendStream != nullptr) {
                    (void)workerImpl->aclDestroyStream(pair.sendStream);
                    pair.sendStream = nullptr;
                }
                if (pair.recvStream != nullptr) {
                    (void)workerImpl->aclDestroyStream(pair.recvStream);
                    pair.recvStream = nullptr;
                }
            }
            pair.recvPostedCount = 0;
            pair.localDeviceId = -1;
        };
        for (;;) {
            Impl::CommPair pair;
            {
                std::unique_lock<std::mutex> lock(workerImpl->cleanupMutex);
                workerImpl->cleanupCv.wait(lock, [workerImpl]() {
                    return workerImpl->cleanupStop || !workerImpl->cleanupQueue.empty();
                });
                if (workerImpl->cleanupQueue.empty()) {
                    if (workerImpl->cleanupStop) {
                        workerImpl->cleanupWorkerExited = true;
                        workerImpl->cleanupDoneCv.notify_all();
                        break;
                    }
                    continue;
                }
                pair = std::move(workerImpl->cleanupQueue.front());
                workerImpl->cleanupQueue.pop_front();
                ++workerImpl->cleanupInFlight;
            }
            destroyPair(pair);
            {
                std::lock_guard<std::mutex> lock(workerImpl->cleanupMutex);
                if (workerImpl->cleanupInFlight > 0) {
                    --workerImpl->cleanupInFlight;
                }
                workerImpl->cleanupDoneCv.notify_all();
            }
        }
    });
    LOG(INFO) << "p2p backend initialize success";
}

P2PTransferBackend::~P2PTransferBackend()
{
    auto impl = impl_;
    if (impl == nullptr) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto &it : impl->comms) {
            std::lock_guard<std::mutex> cleanupLock(impl->cleanupMutex);
            impl->cleanupQueue.emplace_back(std::move(it.second));
        }
        impl->comms.clear();
    }
    {
        std::lock_guard<std::mutex> lock(impl->cleanupMutex);
        impl->cleanupStop = true;
    }
    impl->cleanupCv.notify_all();

    constexpr auto kCleanupWaitTimeout = std::chrono::seconds(2);
    {
        std::unique_lock<std::mutex> lock(impl->cleanupMutex);
        impl->cleanupDoneCv.wait_for(lock, kCleanupWaitTimeout, [impl]() {
            return impl->cleanupWorkerExited || (impl->cleanupQueue.empty() && impl->cleanupInFlight == 0);
        });
    }
    if (impl->cleanupThread.joinable()) {
        bool shouldJoin = false;
        {
            std::lock_guard<std::mutex> lock(impl->cleanupMutex);
            shouldJoin = (impl->cleanupWorkerExited || (impl->cleanupQueue.empty() && impl->cleanupInFlight == 0));
        }
        if (shouldJoin) {
            impl->cleanupThread.join();
        } else {
            impl->cleanupThread.detach();
        }
    }
    impl_.reset();
}

Result P2PTransferBackend::CreateRootInfo(std::string *rootInfoBytes)
{
    LOG(INFO) << "p2p backend create root info begin";
    internal::DumpProcessEnvironment("p2p_backend_create_root_info");
    TE_CHECK_PTR_OR_RETURN(rootInfoBytes);
    if (impl_->getRootInfo == nullptr) {
        std::string msg = "p2p_transfer library not loaded";
        if (!impl_->p2pLoadError.empty()) {
            msg += ": " + impl_->p2pLoadError;
        }
        if (!impl_->p2pSymbolError.empty()) {
            msg += " symbols: " + impl_->p2pSymbolError;
        }
        return TE_MAKE_STATUS(ErrorCode::kNotReady, msg);
    }

    HcclRootInfo rootInfo;
    TE_RETURN_IF_ERROR(ToStatus(impl_->getRootInfo(&rootInfo), "P2PGetRootInfo"));
    rootInfoBytes->assign(reinterpret_cast<const char *>(&rootInfo), sizeof(HcclRootInfo));
    LOG(INFO) << "p2p backend create root info success";
    return Result::OK();
}

Result P2PTransferBackend::InitRecv(const ConnectionSpec &spec, const std::string &rootInfoBytes)
{
    LOG(INFO) << "p2p backend init recv begin, local=" << spec.localHost << ":" << spec.localPort
              << ", local_device_id=" << spec.localDeviceId
              << ", peer=" << spec.peerHost << ":" << spec.peerPort
              << ", peer_device_id=" << spec.peerDeviceId;
    internal::DumpProcessEnvironment("p2p_backend_init_recv");
    TE_CHECK_OR_RETURN(rootInfoBytes.size() == sizeof(HcclRootInfo), ErrorCode::kInvalid, "invalid root info size");
    if (impl_->commInit == nullptr) {
        std::string msg = "p2p_transfer library not loaded";
        if (!impl_->p2pLoadError.empty()) {
            msg += ": " + impl_->p2pLoadError;
        }
        if (!impl_->p2pSymbolError.empty()) {
            msg += " symbols: " + impl_->p2pSymbolError;
        }
        return TE_MAKE_STATUS(ErrorCode::kNotReady, msg);
    }
    TE_CHECK_OR_RETURN(impl_->aclCreateStream != nullptr && impl_->aclCreateEvent != nullptr,
                       ErrorCode::kNotReady, "acl runtime symbols not loaded");
    TE_RETURN_IF_ERROR(EnsureAclContext(spec.localDeviceId));

    HcclRootInfo rootInfo;
    std::memcpy(&rootInfo, rootInfoBytes.data(), sizeof(HcclRootInfo));

    aclrtStream recvStream = nullptr;
    TE_RETURN_IF_ERROR(ToStatusAcl(impl_->aclCreateStream(&recvStream), "aclrtCreateStream(recv)"));

    aclrtEvent recvEvent = nullptr;
    Result eventRc = ToStatusAcl(impl_->aclCreateEvent(&recvEvent), "aclrtCreateEvent(recv)");
    if (eventRc.IsError()) {
        if (impl_->aclDestroyStream != nullptr) {
            (void)impl_->aclDestroyStream(recvStream);
        }
        return eventRc;
    }

    P2PComm recvComm = nullptr;
    Result initRc = ToStatus(impl_->commInit(&rootInfo, P2P_RECEIVER, P2P_LINK_ROCE, &recvComm), "P2PCommInitRootInfo(RECV)");
    if (initRc.IsError()) {
        if (impl_->aclDestroyEvent != nullptr) {
            (void)impl_->aclDestroyEvent(recvEvent);
        }
        if (impl_->aclDestroyStream != nullptr) {
            (void)impl_->aclDestroyStream(recvStream);
        }
        return initRc;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    const std::string key = Key(spec);
    auto iter = impl_->comms.find(key);
    if (iter != impl_->comms.end()) {
        {
            std::lock_guard<std::mutex> cleanupLock(impl_->cleanupMutex);
            impl_->cleanupQueue.emplace_back(std::move(iter->second));
        }
        impl_->comms.erase(iter);
        impl_->cleanupCv.notify_one();
    }
    auto &slot = impl_->comms[key];
    slot.recvComm = recvComm;
    slot.recvStream = recvStream;
    slot.recvEvent = recvEvent;
    slot.localDeviceId = spec.localDeviceId;
    slot.recvPostedCount = 0;
    LOG(INFO) << "p2p backend init recv success, key=" << key;
    return Result::OK();
}

Result P2PTransferBackend::InitSend(const ConnectionSpec &spec, const std::string &rootInfoBytes)
{
    LOG(INFO) << "p2p backend init send begin, local=" << spec.localHost << ":" << spec.localPort
              << ", local_device_id=" << spec.localDeviceId
              << ", peer=" << spec.peerHost << ":" << spec.peerPort
              << ", peer_device_id=" << spec.peerDeviceId;
    internal::DumpProcessEnvironment("p2p_backend_init_send");
    TE_CHECK_OR_RETURN(rootInfoBytes.size() == sizeof(HcclRootInfo), ErrorCode::kInvalid, "invalid root info size");
    if (impl_->commInit == nullptr) {
        std::string msg = "p2p_transfer library not loaded";
        if (!impl_->p2pLoadError.empty()) {
            msg += ": " + impl_->p2pLoadError;
        }
        if (!impl_->p2pSymbolError.empty()) {
            msg += " symbols: " + impl_->p2pSymbolError;
        }
        return TE_MAKE_STATUS(ErrorCode::kNotReady, msg);
    }
    TE_CHECK_OR_RETURN(impl_->aclCreateStream != nullptr, ErrorCode::kNotReady, "acl runtime symbols not loaded");
    TE_RETURN_IF_ERROR(EnsureAclContext(spec.localDeviceId));

    HcclRootInfo rootInfo;
    std::memcpy(&rootInfo, rootInfoBytes.data(), sizeof(HcclRootInfo));

    aclrtStream sendStream = nullptr;
    TE_RETURN_IF_ERROR(ToStatusAcl(impl_->aclCreateStream(&sendStream), "aclrtCreateStream(send)"));

    P2PComm sendComm = nullptr;
    Result initRc = ToStatus(impl_->commInit(&rootInfo, P2P_SENDER, P2P_LINK_ROCE, &sendComm), "P2PCommInitRootInfo(SEND)");
    if (initRc.IsError()) {
        if (impl_->aclDestroyStream != nullptr) {
            (void)impl_->aclDestroyStream(sendStream);
        }
        return initRc;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    const std::string key = Key(spec);
    auto iter = impl_->comms.find(key);
    if (iter != impl_->comms.end()) {
        {
            std::lock_guard<std::mutex> cleanupLock(impl_->cleanupMutex);
            impl_->cleanupQueue.emplace_back(std::move(iter->second));
        }
        impl_->comms.erase(iter);
        impl_->cleanupCv.notify_one();
    }
    auto &slot = impl_->comms[key];
    slot.sendComm = sendComm;
    slot.sendStream = sendStream;
    slot.localDeviceId = spec.localDeviceId;
    LOG(INFO) << "p2p backend init send success, key=" << key;
    return Result::OK();
}

Result P2PTransferBackend::EnsureAclContext(int32_t deviceId)
{
    (void)deviceId;
    TE_CHECK_OR_RETURN(impl_->aclGetCurrentContext != nullptr,
                       ErrorCode::kNotReady, "acl context symbols not loaded");

    aclrtContext current = nullptr;
    aclError getCtxRc = impl_->aclGetCurrentContext(&current);
    if (getCtxRc != ACL_ERROR_NONE) {
        return TE_MAKE_STATUS(ErrorCode::kRuntimeError,
                      ToStatusAcl(getCtxRc, "aclrtGetCurrentContext").GetMsg() +
                          ", hint: please call aclInit(nullptr) and aclrtSetDevice(device_id) before using transfer_engine.");
    }
    if (current == nullptr) {
        return TE_MAKE_STATUS(ErrorCode::kNotReady,
                      "acl context is null, hint: please call aclInit(nullptr) and aclrtSetDevice(device_id) before using transfer_engine.");
    }
    return Result::OK();
}

Result P2PTransferBackend::PostRecv(const ConnectionSpec &spec, uint64_t localAddr, uint64_t length)
{
    TE_CHECK_OR_RETURN(impl_->recv != nullptr && impl_->aclRecordEvent != nullptr,
                       ErrorCode::kNotReady, "p2p/acl symbols not loaded");

    std::lock_guard<std::mutex> lock(mutex_);
    const std::string key = Key(spec);
    auto iter = impl_->comms.find(key);
    TE_CHECK_OR_RETURN(iter != impl_->comms.end() && iter->second.recvComm != nullptr && iter->second.recvStream != nullptr &&
                           iter->second.recvEvent != nullptr,
                       ErrorCode::kNotReady, "recv side is not ready");
    if (impl_->aclSetDevice != nullptr && iter->second.localDeviceId >= 0) {
        TE_RETURN_IF_ERROR(ToStatusAcl(impl_->aclSetDevice(iter->second.localDeviceId), "aclrtSetDevice(recv)"));
    }

    Result recvRc = ToStatus(
        impl_->recv(reinterpret_cast<void *>(localAddr), length, HCCL_DATA_TYPE_INT8, iter->second.recvComm,
                    iter->second.recvStream),
        "P2PRecv");
    if (recvRc.IsError()) {
        EnqueueCleanupLocked(key);
        return recvRc;
    }
    Result eventRc = ToStatusAcl(impl_->aclRecordEvent(iter->second.recvEvent, iter->second.recvStream), "aclrtRecordEvent(recv)");
    if (eventRc.IsError()) {
        EnqueueCleanupLocked(key);
        return eventRc;
    }
    ++iter->second.recvPostedCount;
    return Result::OK();
}

Result P2PTransferBackend::PostSend(const ConnectionSpec &spec, uint64_t remoteAddr, uint64_t length)
{
    TE_CHECK_OR_RETURN(impl_->send != nullptr, ErrorCode::kNotReady, "p2p symbols not loaded");

    std::lock_guard<std::mutex> lock(mutex_);
    const std::string key = Key(spec);
    auto iter = impl_->comms.find(key);
    TE_CHECK_OR_RETURN(iter != impl_->comms.end() && iter->second.sendComm != nullptr && iter->second.sendStream != nullptr,
                       ErrorCode::kNotReady, "send side is not ready");
    if (impl_->aclSetDevice != nullptr && iter->second.localDeviceId >= 0) {
        TE_RETURN_IF_ERROR(ToStatusAcl(impl_->aclSetDevice(iter->second.localDeviceId), "aclrtSetDevice(send)"));
    }

    Result sendRc = ToStatus(
        impl_->send(reinterpret_cast<void *>(remoteAddr), length, HCCL_DATA_TYPE_INT8, iter->second.sendComm,
                    iter->second.sendStream),
        "P2PSend");
    if (sendRc.IsError()) {
        EnqueueCleanupLocked(key);
        return sendRc;
    }
    return Result::OK();
}

Result P2PTransferBackend::WaitRecv(const ConnectionSpec &spec, uint64_t timeoutMs)
{
    TE_CHECK_OR_RETURN(impl_->aclSynchronizeEvent != nullptr || impl_->aclSynchronizeStream != nullptr,
                       ErrorCode::kNotReady, "acl wait symbols not loaded");

    std::lock_guard<std::mutex> lock(mutex_);
    const std::string key = Key(spec);
    auto iter = impl_->comms.find(key);
    TE_CHECK_OR_RETURN(iter != impl_->comms.end(), ErrorCode::kNotReady, "connection not found");
    TE_CHECK_OR_RETURN(iter->second.recvPostedCount > 0, ErrorCode::kNotReady, "recv was not posted");
    if (impl_->aclSetDevice != nullptr && iter->second.localDeviceId >= 0) {
        TE_RETURN_IF_ERROR(ToStatusAcl(impl_->aclSetDevice(iter->second.localDeviceId), "aclrtSetDevice(wait_recv)"));
    }

    Result waitRc = Result::OK();
    if (impl_->aclSynchronizeEventWithTimeout != nullptr && iter->second.recvEvent != nullptr) {
        waitRc = ToStatusAcl(impl_->aclSynchronizeEventWithTimeout(iter->second.recvEvent, static_cast<int32_t>(timeoutMs)),
                             "aclrtSynchronizeEventWithTimeout");
    } else if (impl_->aclSynchronizeEvent != nullptr && iter->second.recvEvent != nullptr) {
        waitRc = ToStatusAcl(impl_->aclSynchronizeEvent(iter->second.recvEvent), "aclrtSynchronizeEvent");
    } else {
        waitRc = ToStatusAcl(impl_->aclSynchronizeStream(iter->second.recvStream), "aclrtSynchronizeStream(recv)");
    }

    if (waitRc.IsError()) {
        EnqueueCleanupLocked(key);
        return waitRc;
    }

    if (impl_->getCommAsyncError != nullptr && iter->second.recvComm != nullptr) {
        HcclResult asyncErr = HCCL_SUCCESS;
        Result asyncRc = ToStatus(impl_->getCommAsyncError(iter->second.recvComm, &asyncErr), "P2PGetCommAsyncError");
        if (asyncRc.IsError() || asyncErr != HCCL_SUCCESS) {
            EnqueueCleanupLocked(key);
            if (asyncRc.IsError()) {
                return asyncRc;
            }
            return TE_MAKE_STATUS(ErrorCode::kRuntimeError,
                          "p2p async error=" + std::to_string(static_cast<int>(asyncErr)));
        }
    }

    --iter->second.recvPostedCount;
    return Result::OK();
}

void P2PTransferBackend::AbortConnection(const ConnectionSpec &spec)
{
    std::lock_guard<std::mutex> lock(mutex_);
    EnqueueCleanupLocked(Key(spec));
}

std::string P2PTransferBackend::Key(const ConnectionSpec &spec) const
{
    return spec.localHost + ":" + std::to_string(spec.localPort) + ":" + std::to_string(spec.localDeviceId) + "|" +
           spec.peerHost + ":" + std::to_string(spec.peerPort) + ":" + std::to_string(spec.peerDeviceId);
}

void P2PTransferBackend::ResetConnectionLocked(const std::string &key)
{
    EnqueueCleanupLocked(key);
}

void P2PTransferBackend::EnqueueCleanupLocked(const std::string &key)
{
    auto iter = impl_->comms.find(key);
    if (iter == impl_->comms.end()) {
        return;
    }
    {
        std::lock_guard<std::mutex> cleanupLock(impl_->cleanupMutex);
        impl_->cleanupQueue.emplace_back(std::move(iter->second));
    }
    impl_->comms.erase(iter);
    impl_->cleanupCv.notify_one();
}

}  // namespace datasystem
