#include <gtest/gtest.h>

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <poll.h>
#include <signal.h>

#include <cerrno>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

#include <glog/logging.h>

#include "acl_test_utils.h"
#include "internal/backend/mock_data_plane_backend.h"
#include "datasystem/transfer_engine/status_helper.h"
#include "datasystem/transfer_engine/transfer_engine.h"

namespace datasystem {
namespace {

constexpr uint16_t kOwnerPortSuccess = 55051;
constexpr uint16_t kRequesterPortSuccess = 55052;
constexpr uint16_t kOwnerPortReject = 56051;
constexpr uint16_t kRequesterPortReject = 56052;
constexpr int32_t kAclMemcpyHostToDevice = 1;
constexpr int32_t kAclMemcpyDeviceToHost = 2;

class ScopedEnvVar {
public:
    ScopedEnvVar(std::string key, std::string value) : key_(std::move(key))
    {
        const char *old = std::getenv(key_.c_str());
        if (old != nullptr) {
            hadOld_ = true;
            oldValue_ = old;
        }
        (void)setenv(key_.c_str(), value.c_str(), 1);
    }

    ~ScopedEnvVar()
    {
        if (hadOld_) {
            (void)setenv(key_.c_str(), oldValue_.c_str(), 1);
        } else {
            (void)unsetenv(key_.c_str());
        }
    }

private:
    std::string key_;
    bool hadOld_ = false;
    std::string oldValue_;
};

bool WriteByte(int fd, uint8_t value)
{
    return write(fd, &value, sizeof(value)) == static_cast<ssize_t>(sizeof(value));
}

bool ReadByte(int fd, uint8_t *value)
{
    return read(fd, value, sizeof(*value)) == static_cast<ssize_t>(sizeof(*value));
}

bool WriteU64(int fd, uint64_t value)
{
    return write(fd, &value, sizeof(value)) == static_cast<ssize_t>(sizeof(value));
}

bool ReadU64(int fd, uint64_t *value)
{
    return read(fd, value, sizeof(*value)) == static_cast<ssize_t>(sizeof(*value));
}

bool WriteString(int fd, const std::string &value)
{
    uint32_t len = static_cast<uint32_t>(value.size());
    if (write(fd, &len, sizeof(len)) != static_cast<ssize_t>(sizeof(len))) {
        return false;
    }
    if (len == 0) {
        return true;
    }
    return write(fd, value.data(), len) == static_cast<ssize_t>(len);
}

bool ReadString(int fd, std::string *value)
{
    if (value == nullptr) {
        return false;
    }
    uint32_t len = 0;
    if (read(fd, &len, sizeof(len)) != static_cast<ssize_t>(sizeof(len))) {
        return false;
    }
    value->assign(len, '\0');
    if (len == 0) {
        return true;
    }
    return read(fd, value->data(), len) == static_cast<ssize_t>(len);
}

// 在超时时间内等待读取 1 字节，避免父进程无限阻塞。
bool ReadByteWithTimeoutMs(int fd, uint8_t *value, int timeoutMs)
{
    pollfd pfd{};
    pfd.fd = fd;
    pfd.events = POLLIN;
    const int pollRc = poll(&pfd, 1, timeoutMs);
    if (pollRc <= 0) {
        return false;
    }
    if ((pfd.revents & POLLIN) == 0) {
        return false;
    }
    return ReadByte(fd, value);
}

[[noreturn]] void ExitWithShutdown(int exitCode)
{
    if (google::IsGoogleLoggingInitialized()) {
        google::FlushLogFiles(google::GLOG_INFO);
        google::FlushLogFiles(google::GLOG_WARNING);
        google::FlushLogFiles(google::GLOG_ERROR);
        google::ShutdownGoogleLogging();
    }
    _exit(exitCode);
}

Status EnsureAclInitializedForTest()
{
#if defined(TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND) && TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND
    return testutil::EnsureAclInitialized();
#endif
    return Status::OK();
}

Status SetAclDeviceForTest(int32_t deviceId)
{
#if defined(TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND) && TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND
    return testutil::SetAclDevice(deviceId);
#else
    (void)deviceId;
#endif
    return Status::OK();
}

Status AclMallocForTest(size_t size, void **devPtr)
{
#if defined(TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND) && TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND
    return testutil::AclMalloc(size, devPtr);
#else
    (void)size;
    (void)devPtr;
#endif
    return Status::OK();
}

Status AclFreeForTest(void *devPtr)
{
#if defined(TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND) && TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND
    return testutil::AclFree(devPtr);
#else
    (void)devPtr;
#endif
    return Status::OK();
}

Status AclMemcpyForTest(void *dst, size_t dstSize, const void *src, size_t srcSize, int32_t kind)
{
#if defined(TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND) && TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND
    return testutil::AclMemcpy(dst, dstSize, src, srcSize, kind);
#else
    (void)dst;
    (void)dstSize;
    (void)src;
    (void)srcSize;
    (void)kind;
#endif
    return Status::OK();
}

Status BatchReadOne(TransferEngine *engine, const std::string &peerHost, uint16_t peerPort, uint64_t remoteAddr,
                    uint64_t localAddr, uint64_t length)
{
    TE_CHECK_PTR_OR_RETURN(engine);
    const std::string targetHostname = peerHost + ":" + std::to_string(peerPort);
    return engine->BatchTransferSyncRead(
        targetHostname,
        {static_cast<uintptr_t>(localAddr)},
        {static_cast<uintptr_t>(remoteAddr)},
        {static_cast<size_t>(length)});
}

Status RunInProcessTransferCase(bool registerOwnerMemory, StatusCode *requesterCode, std::string *requesterMsg,
                                std::vector<uint8_t> *dst)
{
    if (requesterCode == nullptr || requesterMsg == nullptr || dst == nullptr) {
        return Status(StatusCode::kRuntimeError, "null output pointer");
    }
    std::vector<uint8_t> src(128, registerOwnerMemory ? 7 : 1);
    dst->assign(src.size(), 0);

    const uint16_t ownerPort = registerOwnerMemory ? kOwnerPortSuccess : kOwnerPortReject;
    const uint16_t requesterPort = registerOwnerMemory ? kRequesterPortSuccess : kRequesterPortReject;
    const int32_t ownerDeviceId = 0;
    const int32_t requesterDeviceId = 1;

    auto sharedState = std::make_shared<MockDataPlaneBackend::SharedState>();
    auto ownerBackend = std::make_shared<MockDataPlaneBackend>(sharedState);
    auto requesterBackend = std::make_shared<MockDataPlaneBackend>(sharedState);
    TransferEngine owner(ownerBackend);
    TransferEngine requester(requesterBackend);
    TE_RETURN_IF_ERROR(owner.Initialize("127.0.0.1:" + std::to_string(ownerPort), "ascend",
                                        "npu:" + std::to_string(ownerDeviceId)));
    TE_RETURN_IF_ERROR(requester.Initialize("127.0.0.1:" + std::to_string(requesterPort), "ascend",
                                            "npu:" + std::to_string(requesterDeviceId)));

    if (registerOwnerMemory) {
        TE_RETURN_IF_ERROR(owner.RegisterMemory(reinterpret_cast<uintptr_t>(src.data()), src.size()));
    }

    Status transferRc = BatchReadOne(&requester, "127.0.0.1", ownerPort, reinterpret_cast<uintptr_t>(src.data()),
                                     reinterpret_cast<uint64_t>(dst->data()), dst->size());
    *requesterCode = transferRc.GetCode();
    *requesterMsg = transferRc.GetMsg();
    (void)requester.Finalize();
    (void)owner.Finalize();
    return Status::OK();
}

struct ForkedTransferPipes {
    int ready[2] = {-1, -1};
    int done[2] = {-1, -1};
    int addr[2] = {-1, -1};
};

void ClosePipe(int pipeFd[2])
{
    if (pipeFd[0] >= 0) {
        close(pipeFd[0]);
        pipeFd[0] = -1;
    }
    if (pipeFd[1] >= 0) {
        close(pipeFd[1]);
        pipeFd[1] = -1;
    }
}

Status CreateForkedTransferPipes(ForkedTransferPipes *pipes)
{
    TE_CHECK_PTR_OR_RETURN(pipes);
    if (pipe(pipes->ready) != 0 || pipe(pipes->done) != 0 || pipe(pipes->addr) != 0) {
        ClosePipe(pipes->ready);
        ClosePipe(pipes->done);
        ClosePipe(pipes->addr);
        return Status(StatusCode::kRuntimeError, std::string("pipe failed: ") + std::strerror(errno));
    }
    return Status::OK();
}

void WriteOwnerFailAndExit(int readyWriteFd, int exitCode, const Status &status)
{
    (void)WriteByte(readyWriteFd, 0);
    (void)WriteString(readyWriteFd, status.ToString());
    ExitWithShutdown(exitCode);
}

void OwnerProcessMain(bool registerOwnerMemory, const std::vector<uint8_t> &src, uint16_t ownerPort, int32_t ownerDeviceId,
                      ForkedTransferPipes *pipes)
{
    close(pipes->ready[0]);
    close(pipes->done[1]);
    close(pipes->addr[0]);

    TransferEngine owner;
    Status initRc = owner.Initialize("127.0.0.1:" + std::to_string(ownerPort), "ascend",
                                     "npu:" + std::to_string(ownerDeviceId));
    if (initRc.IsError()) {
        WriteOwnerFailAndExit(pipes->ready[1], 100, initRc);
    }

    Status aclInitRc = EnsureAclInitializedForTest();
    if (aclInitRc.IsError()) {
        WriteOwnerFailAndExit(pipes->ready[1], 99, aclInitRc);
    }
    Status setDeviceRc = SetAclDeviceForTest(ownerDeviceId);
    if (setDeviceRc.IsError()) {
        WriteOwnerFailAndExit(pipes->ready[1], 104, setDeviceRc);
    }

    void *ownerSrcDev = nullptr;
    Status mallocRc = AclMallocForTest(src.size(), &ownerSrcDev);
    if (mallocRc.IsError()) {
        WriteOwnerFailAndExit(pipes->ready[1], 105, mallocRc);
    }
    Status memcpyRc = AclMemcpyForTest(ownerSrcDev, src.size(), src.data(), src.size(), kAclMemcpyHostToDevice);
    if (memcpyRc.IsError()) {
        (void)AclFreeForTest(ownerSrcDev);
        WriteOwnerFailAndExit(pipes->ready[1], 106, memcpyRc);
    }

    if (registerOwnerMemory) {
        Status regRc = owner.RegisterMemory(reinterpret_cast<uintptr_t>(ownerSrcDev), src.size());
        if (regRc.IsError()) {
            (void)AclFreeForTest(ownerSrcDev);
            WriteOwnerFailAndExit(pipes->ready[1], 101, regRc);
        }
    }

    if (!WriteByte(pipes->ready[1], 1) || !WriteU64(pipes->addr[1], reinterpret_cast<uintptr_t>(ownerSrcDev))) {
        (void)AclFreeForTest(ownerSrcDev);
        ExitWithShutdown(102);
    }

    uint8_t done = 0;
    if (!ReadByte(pipes->done[0], &done)) {
        (void)AclFreeForTest(ownerSrcDev);
        ExitWithShutdown(103);
    }
    (void)AclFreeForTest(ownerSrcDev);
    (void)owner.Finalize();
    ExitWithShutdown(0);
}

Status WaitOwnerReadyAndReadAddr(pid_t ownerPid, ForkedTransferPipes *pipes, uint64_t *ownerSrcDevAddr)
{
    TE_CHECK_PTR_OR_RETURN(pipes);
    TE_CHECK_PTR_OR_RETURN(ownerSrcDevAddr);

    uint8_t readyTag = 0;
    if (!ReadByteWithTimeoutMs(pipes->ready[0], &readyTag, 10000)) {
        close(pipes->done[1]);
        int childStatus = 0;
        const pid_t waitRc = waitpid(ownerPid, &childStatus, 0);
        if (waitRc == 0) {
            (void)kill(ownerPid, SIGKILL);
            (void)waitpid(ownerPid, nullptr, 0);
            close(pipes->ready[0]);
            return Status(StatusCode::kRuntimeError, "timeout waiting owner ready signal");
        }
        if (WIFEXITED(childStatus)) {
            std::string childMsg;
            (void)ReadString(pipes->ready[0], &childMsg);
            close(pipes->ready[0]);
            return Status(StatusCode::kRuntimeError,
                          "owner exited before ready, exit_code=" + std::to_string(WEXITSTATUS(childStatus)) +
                              (childMsg.empty() ? "" : ", detail=" + childMsg));
        }
        close(pipes->ready[0]);
        return Status(StatusCode::kRuntimeError, "failed to wait owner ready signal");
    }
    if (readyTag != 1) {
        std::string childMsg;
        (void)ReadString(pipes->ready[0], &childMsg);
        close(pipes->ready[0]);
        int childStatus = 0;
        (void)waitpid(ownerPid, &childStatus, 0);
        return Status(StatusCode::kRuntimeError,
                      "owner init failed before ready" + (childMsg.empty() ? "" : (", detail=" + childMsg)));
    }

    if (!ReadU64(pipes->addr[0], ownerSrcDevAddr) || *ownerSrcDevAddr == 0) {
        close(pipes->ready[0]);
        close(pipes->addr[0]);
        close(pipes->done[1]);
        (void)kill(ownerPid, SIGKILL);
        (void)waitpid(ownerPid, nullptr, 0);
        return Status(StatusCode::kRuntimeError, "failed to read owner src device address");
    }
    close(pipes->ready[0]);
    close(pipes->addr[0]);
    return Status::OK();
}

// 使用 fork 将 owner/requester 放到不同进程，满足 P2P-Transfer 双进程 send/recv 约束。
Status RunForkedTransferCase(bool registerOwnerMemory, StatusCode *requesterCode, std::string *requesterMsg,
                             std::vector<uint8_t> *dst)
{
    if (requesterCode == nullptr || requesterMsg == nullptr || dst == nullptr) {
        return Status(StatusCode::kRuntimeError, "null output pointer");
    }

    std::vector<uint8_t> src(128, registerOwnerMemory ? 7 : 1);
    dst->assign(src.size(), 0);

    ForkedTransferPipes pipes;
    TE_RETURN_IF_ERROR(CreateForkedTransferPipes(&pipes));

    const uint16_t ownerPort = registerOwnerMemory ? kOwnerPortSuccess : kOwnerPortReject;
    const uint16_t requesterPort = registerOwnerMemory ? kRequesterPortSuccess : kRequesterPortReject;
    const int32_t ownerDeviceId = 0;
    const int32_t requesterDeviceId = 1;

    pid_t pid = fork();
    if (pid < 0) {
        ClosePipe(pipes.ready);
        ClosePipe(pipes.done);
        ClosePipe(pipes.addr);
        return Status(StatusCode::kRuntimeError, std::string("fork failed: ") + std::strerror(errno));
    }

    if (pid == 0) {
        OwnerProcessMain(registerOwnerMemory, src, ownerPort, ownerDeviceId, &pipes);
    }

    close(pipes.ready[1]);
    close(pipes.done[0]);
    close(pipes.addr[1]);

    TE_RETURN_IF_ERROR(EnsureAclInitializedForTest());
    TE_RETURN_IF_ERROR(SetAclDeviceForTest(requesterDeviceId));

    uint64_t ownerSrcDevAddr = 0;
    TE_RETURN_IF_ERROR(WaitOwnerReadyAndReadAddr(pid, &pipes, &ownerSrcDevAddr));

    TransferEngine requester;
    Status initRc = requester.Initialize("127.0.0.1:" + std::to_string(requesterPort), "ascend",
                                         "npu:" + std::to_string(requesterDeviceId));
    if (initRc.IsError()) {
        (void)WriteByte(pipes.done[1], 1);
        close(pipes.done[1]);
        (void)waitpid(pid, nullptr, 0);
        return initRc;
    }

    void *requesterDstDev = nullptr;
    Status dstMallocRc = AclMallocForTest(dst->size(), &requesterDstDev);
    if (dstMallocRc.IsError()) {
        (void)WriteByte(pipes.done[1], 1);
        close(pipes.done[1]);
        (void)requester.Finalize();
        (void)waitpid(pid, nullptr, 0);
        return dstMallocRc;
    }

    Status transferRc = BatchReadOne(&requester, "127.0.0.1", ownerPort, ownerSrcDevAddr,
                                     reinterpret_cast<uint64_t>(requesterDstDev), dst->size());
    *requesterCode = transferRc.GetCode();
    *requesterMsg = transferRc.GetMsg();
    if (transferRc.IsOk()) {
        Status copyBackRc =
            AclMemcpyForTest(dst->data(), dst->size(), requesterDstDev, dst->size(), kAclMemcpyDeviceToHost);
        if (copyBackRc.IsError()) {
            *requesterCode = copyBackRc.GetCode();
            *requesterMsg = copyBackRc.GetMsg();
        }
    }

    (void)AclFreeForTest(requesterDstDev);
    (void)requester.Finalize();
    (void)WriteByte(pipes.done[1], 1);
    close(pipes.done[1]);

    int childStatus = 0;
    if (waitpid(pid, &childStatus, 0) < 0) {
        return Status(StatusCode::kRuntimeError, std::string("waitpid failed: ") + std::strerror(errno));
    }
    if (!WIFEXITED(childStatus) || WEXITSTATUS(childStatus) != 0) {
        return Status(StatusCode::kRuntimeError, "owner process exit with failure");
    }
    return Status::OK();
}

// 使用 fork 分别设置 RT_ASCEND_VISIBLE_DEVICES 后，验证同节点 batch_transfer_sync_read 可成功。
Status RunForkedTransferCaseWithRtAscendVisibleDevices(StatusCode *requesterCode, std::string *requesterMsg,
                                                       std::vector<uint8_t> *dst)
{
#if defined(TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND) && TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND
    if (requesterCode == nullptr || requesterMsg == nullptr || dst == nullptr) {
        return Status(StatusCode::kRuntimeError, "null output pointer");
    }

    std::vector<uint8_t> src(128, 7);
    dst->assign(src.size(), 0);

    ForkedTransferPipes pipes;
    TE_RETURN_IF_ERROR(CreateForkedTransferPipes(&pipes));

    constexpr uint16_t kOwnerPort = 59251;
    constexpr uint16_t kRequesterPort = 59252;
    constexpr int32_t kOwnerLogicalDeviceId = 0;
    constexpr int32_t kRequesterLogicalDeviceId = 0;

    pid_t pid = fork();
    if (pid < 0) {
        ClosePipe(pipes.ready);
        ClosePipe(pipes.done);
        ClosePipe(pipes.addr);
        return Status(StatusCode::kRuntimeError, std::string("fork failed: ") + std::strerror(errno));
    }

    if (pid == 0) {
        ScopedEnvVar ownerRtVisible("RT_ASCEND_VISIBLE_DEVICES", "0");
        ScopedEnvVar ownerAscendRtVisible("ASCEND_RT_VISIBLE_DEVICES", "0");
        OwnerProcessMain(true, src, kOwnerPort, kOwnerLogicalDeviceId, &pipes);
    }

    close(pipes.ready[1]);
    close(pipes.done[0]);
    close(pipes.addr[1]);

    ScopedEnvVar requesterRtVisible("RT_ASCEND_VISIBLE_DEVICES", "1");
    ScopedEnvVar requesterAscendRtVisible("ASCEND_RT_VISIBLE_DEVICES", "1");
    TE_RETURN_IF_ERROR(EnsureAclInitializedForTest());
    TE_RETURN_IF_ERROR(SetAclDeviceForTest(kRequesterLogicalDeviceId));

    uint64_t ownerSrcDevAddr = 0;
    TE_RETURN_IF_ERROR(WaitOwnerReadyAndReadAddr(pid, &pipes, &ownerSrcDevAddr));

    TransferEngine requester;
    Status initRc = requester.Initialize("127.0.0.1:" + std::to_string(kRequesterPort), "ascend",
                                         "npu:" + std::to_string(kRequesterLogicalDeviceId));
    if (initRc.IsError()) {
        (void)WriteByte(pipes.done[1], 1);
        close(pipes.done[1]);
        (void)waitpid(pid, nullptr, 0);
        return initRc;
    }

    void *requesterDstDev = nullptr;
    Status dstMallocRc = AclMallocForTest(dst->size(), &requesterDstDev);
    if (dstMallocRc.IsError()) {
        (void)WriteByte(pipes.done[1], 1);
        close(pipes.done[1]);
        (void)requester.Finalize();
        (void)waitpid(pid, nullptr, 0);
        return dstMallocRc;
    }

    Status transferRc = BatchReadOne(&requester, "127.0.0.1", kOwnerPort, ownerSrcDevAddr,
                                     reinterpret_cast<uint64_t>(requesterDstDev), dst->size());
    *requesterCode = transferRc.GetCode();
    *requesterMsg = transferRc.GetMsg();
    if (transferRc.IsOk()) {
        Status copyBackRc =
            AclMemcpyForTest(dst->data(), dst->size(), requesterDstDev, dst->size(), kAclMemcpyDeviceToHost);
        if (copyBackRc.IsError()) {
            *requesterCode = copyBackRc.GetCode();
            *requesterMsg = copyBackRc.GetMsg();
        }
    }

    (void)AclFreeForTest(requesterDstDev);
    (void)requester.Finalize();
    (void)WriteByte(pipes.done[1], 1);
    close(pipes.done[1]);

    int childStatus = 0;
    if (waitpid(pid, &childStatus, 0) < 0) {
        return Status(StatusCode::kRuntimeError, std::string("waitpid failed: ") + std::strerror(errno));
    }
    if (!WIFEXITED(childStatus) || WEXITSTATUS(childStatus) != 0) {
        return Status(StatusCode::kRuntimeError, "owner process exit with failure");
    }
    return Status::OK();
#else
    (void)requesterCode;
    (void)requesterMsg;
    (void)dst;
    return Status(StatusCode::kNotSupported, "p2p backend disabled");
#endif
}

Status RunForkedConcurrentNpuReadCase()
{
#if defined(TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND) && TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND
    constexpr size_t kPayloadSize = 256;
    constexpr uint16_t kOwnerPort = 59051;
    constexpr uint16_t kRequesterPortA = 59052;
    constexpr uint16_t kRequesterPortB = 59053;
    constexpr int32_t kOwnerDeviceId = 0;
    constexpr int32_t kRequesterDeviceId = 1;

    int readyPipe[2] = {-1, -1};
    int donePipe[2] = {-1, -1};
    int addrPipe[2] = {-1, -1};
    if (pipe(readyPipe) != 0 || pipe(donePipe) != 0 || pipe(addrPipe) != 0) {
        return Status(StatusCode::kRuntimeError, std::string("pipe failed: ") + std::strerror(errno));
    }

    pid_t ownerPid = fork();
    if (ownerPid < 0) {
        return Status(StatusCode::kRuntimeError, std::string("fork owner failed: ") + std::strerror(errno));
    }

    if (ownerPid == 0) {
        close(readyPipe[0]);
        close(donePipe[1]);
        close(addrPipe[0]);

        Status rc = EnsureAclInitializedForTest();
        if (rc.IsError() || !SetAclDeviceForTest(kOwnerDeviceId).IsOk()) {
            (void)WriteByte(readyPipe[1], 0);
            ExitWithShutdown(10);
        }

        TransferEngine owner;
        rc = owner.Initialize("127.0.0.1:" + std::to_string(kOwnerPort), "ascend",
                              "npu:" + std::to_string(kOwnerDeviceId));
        if (rc.IsError()) {
            (void)WriteByte(readyPipe[1], 0);
            ExitWithShutdown(11);
        }

        std::vector<uint8_t> hostA(kPayloadSize, 0x11);
        std::vector<uint8_t> hostB(kPayloadSize, 0x22);
        void *srcA = nullptr;
        void *srcB = nullptr;
        if (!AclMallocForTest(kPayloadSize, &srcA).IsOk() || !AclMallocForTest(kPayloadSize, &srcB).IsOk()) {
            (void)WriteByte(readyPipe[1], 0);
            ExitWithShutdown(12);
        }
        if (!AclMemcpyForTest(srcA, kPayloadSize, hostA.data(), kPayloadSize, kAclMemcpyHostToDevice).IsOk() ||
            !AclMemcpyForTest(srcB, kPayloadSize, hostB.data(), kPayloadSize, kAclMemcpyHostToDevice).IsOk()) {
            (void)WriteByte(readyPipe[1], 0);
            ExitWithShutdown(13);
        }
        if (!owner.RegisterMemory(reinterpret_cast<uintptr_t>(srcA), kPayloadSize).IsOk() ||
            !owner.RegisterMemory(reinterpret_cast<uintptr_t>(srcB), kPayloadSize).IsOk()) {
            (void)WriteByte(readyPipe[1], 0);
            ExitWithShutdown(14);
        }

        if (!WriteByte(readyPipe[1], 1) || !WriteU64(addrPipe[1], reinterpret_cast<uintptr_t>(srcA)) ||
            !WriteU64(addrPipe[1], reinterpret_cast<uintptr_t>(srcB))) {
            ExitWithShutdown(15);
        }

        uint8_t done = 0;
        (void)ReadByte(donePipe[0], &done);
        (void)owner.Finalize();
        (void)AclFreeForTest(srcA);
        (void)AclFreeForTest(srcB);
        ExitWithShutdown(0);
    }

    close(readyPipe[1]);
    close(donePipe[0]);
    close(addrPipe[1]);
    uint8_t ready = 0;
    if (!ReadByteWithTimeoutMs(readyPipe[0], &ready, 10000) || ready != 1) {
        close(readyPipe[0]);
        close(donePipe[1]);
        close(addrPipe[0]);
        (void)kill(ownerPid, SIGKILL);
        (void)waitpid(ownerPid, nullptr, 0);
        return Status(StatusCode::kRuntimeError, "owner not ready");
    }
    uint64_t remoteAddrA = 0;
    uint64_t remoteAddrB = 0;
    if (!ReadU64(addrPipe[0], &remoteAddrA) || !ReadU64(addrPipe[0], &remoteAddrB) || remoteAddrA == 0 ||
        remoteAddrB == 0) {
        close(readyPipe[0]);
        close(donePipe[1]);
        close(addrPipe[0]);
        (void)kill(ownerPid, SIGKILL);
        (void)waitpid(ownerPid, nullptr, 0);
        return Status(StatusCode::kRuntimeError, "failed to read owner remote addresses");
    }
    close(readyPipe[0]);
    close(addrPipe[0]);

    auto spawnRequester = [&](uint16_t port, uint64_t remoteAddr, uint8_t expected) -> pid_t {
        pid_t pid = fork();
        if (pid != 0) {
            return pid;
        }
        if (!EnsureAclInitializedForTest().IsOk() || !SetAclDeviceForTest(kRequesterDeviceId).IsOk()) {
            ExitWithShutdown(20);
        }
        TransferEngine requester;
        if (!requester.Initialize("127.0.0.1:" + std::to_string(port), "ascend",
                                  "npu:" + std::to_string(kRequesterDeviceId)).IsOk()) {
            ExitWithShutdown(21);
        }
        void *dstDev = nullptr;
        if (!AclMallocForTest(kPayloadSize, &dstDev).IsOk()) {
            ExitWithShutdown(22);
        }
        Status readRc = BatchReadOne(&requester, "127.0.0.1", kOwnerPort, remoteAddr,
                                     reinterpret_cast<uint64_t>(dstDev), kPayloadSize);
        if (readRc.IsError()) {
            (void)AclFreeForTest(dstDev);
            ExitWithShutdown(23);
        }
        std::vector<uint8_t> hostDst(kPayloadSize, 0);
        if (!AclMemcpyForTest(hostDst.data(), kPayloadSize, dstDev, kPayloadSize, kAclMemcpyDeviceToHost).IsOk()) {
            (void)AclFreeForTest(dstDev);
            ExitWithShutdown(24);
        }
        for (auto v : hostDst) {
            if (v != expected) {
                (void)AclFreeForTest(dstDev);
                ExitWithShutdown(25);
            }
        }
        (void)AclFreeForTest(dstDev);
        (void)requester.Finalize();
        ExitWithShutdown(0);
    };

    const pid_t requesterA = spawnRequester(kRequesterPortA, remoteAddrA, 0x11);
    const pid_t requesterB = spawnRequester(kRequesterPortB, remoteAddrB, 0x22);
    if (requesterA < 0 || requesterB < 0) {
        (void)kill(ownerPid, SIGKILL);
        (void)waitpid(ownerPid, nullptr, 0);
        return Status(StatusCode::kRuntimeError, "fork requester failed");
    }

    int statusA = 0;
    int statusB = 0;
    (void)waitpid(requesterA, &statusA, 0);
    (void)waitpid(requesterB, &statusB, 0);
    (void)WriteByte(donePipe[1], 1);
    close(donePipe[1]);

    int ownerStatus = 0;
    (void)waitpid(ownerPid, &ownerStatus, 0);
    if (!WIFEXITED(statusA) || WEXITSTATUS(statusA) != 0 || !WIFEXITED(statusB) || WEXITSTATUS(statusB) != 0 ||
        !WIFEXITED(ownerStatus) || WEXITSTATUS(ownerStatus) != 0) {
        return Status(StatusCode::kRuntimeError,
                      "concurrent npu read case failed: requesterA=" + std::to_string(WEXITSTATUS(statusA)) +
                          ", requesterB=" + std::to_string(WEXITSTATUS(statusB)) +
                          ", owner=" + std::to_string(WEXITSTATUS(ownerStatus)));
    }
    return Status::OK();
#else
    return Status(StatusCode::kNotSupported, "p2p backend disabled");
#endif
}

Status RunForkedMultiNpuOwnerReadCase()
{
#if defined(TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND) && TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND
    constexpr size_t kPayloadSize = 256;
    constexpr uint16_t kOwnerPort0 = 59151;
    constexpr uint16_t kOwnerPort1 = 59152;
    constexpr uint16_t kRequesterPort0 = 59161;
    constexpr uint16_t kRequesterPort1 = 59162;
    constexpr int32_t kOwnerDevice0 = 0;
    constexpr int32_t kOwnerDevice1 = 1;
    constexpr int32_t kRequesterDevice0 = 1;
    constexpr int32_t kRequesterDevice1 = 0;

    int readyPipe[2] = {-1, -1};
    int donePipe[2] = {-1, -1};
    int addrPipe[2] = {-1, -1};
    if (pipe(readyPipe) != 0 || pipe(donePipe) != 0 || pipe(addrPipe) != 0) {
        return Status(StatusCode::kRuntimeError, std::string("pipe failed: ") + std::strerror(errno));
    }

    pid_t ownerPid = fork();
    if (ownerPid < 0) {
        return Status(StatusCode::kRuntimeError, std::string("fork owner failed: ") + std::strerror(errno));
    }
    if (ownerPid == 0) {
        close(readyPipe[0]);
        close(donePipe[1]);
        close(addrPipe[0]);

        TransferEngine owner0;
        TransferEngine owner1;
        void *src0 = nullptr;
        void *src1 = nullptr;
        auto failAndExit = [&](int code) {
            (void)WriteByte(readyPipe[1], 0);
            ExitWithShutdown(code);
        };

        if (!EnsureAclInitializedForTest().IsOk()) {
            failAndExit(30);
        }
        if (!SetAclDeviceForTest(kOwnerDevice0).IsOk()) {
            failAndExit(31);
        }
        if (!owner0.Initialize("127.0.0.1:" + std::to_string(kOwnerPort0), "ascend",
                               "npu:" + std::to_string(kOwnerDevice0)).IsOk()) {
            failAndExit(32);
        }
        if (!AclMallocForTest(kPayloadSize, &src0).IsOk()) {
            failAndExit(33);
        }
        std::vector<uint8_t> host0(kPayloadSize, static_cast<uint8_t>(kOwnerDevice0 + 1));
        if (!AclMemcpyForTest(src0, kPayloadSize, host0.data(), host0.size(), kAclMemcpyHostToDevice).IsOk()) {
            failAndExit(34);
        }
        if (!owner0.RegisterMemory(reinterpret_cast<uintptr_t>(src0), kPayloadSize).IsOk()) {
            failAndExit(35);
        }

        if (!SetAclDeviceForTest(kOwnerDevice1).IsOk()) {
            failAndExit(36);
        }
        if (!owner1.Initialize("127.0.0.1:" + std::to_string(kOwnerPort1), "ascend",
                               "npu:" + std::to_string(kOwnerDevice1)).IsOk()) {
            failAndExit(37);
        }
        if (!AclMallocForTest(kPayloadSize, &src1).IsOk()) {
            failAndExit(38);
        }
        std::vector<uint8_t> host1(kPayloadSize, static_cast<uint8_t>(kOwnerDevice1 + 1));
        if (!AclMemcpyForTest(src1, kPayloadSize, host1.data(), host1.size(), kAclMemcpyHostToDevice).IsOk()) {
            failAndExit(39);
        }
        if (!owner1.RegisterMemory(reinterpret_cast<uintptr_t>(src1), kPayloadSize).IsOk()) {
            failAndExit(40);
        }

        if (!WriteByte(readyPipe[1], 1) || !WriteU64(addrPipe[1], reinterpret_cast<uintptr_t>(src0)) ||
            !WriteU64(addrPipe[1], reinterpret_cast<uintptr_t>(src1))) {
            failAndExit(41);
        }

        uint8_t done = 0;
        (void)ReadByte(donePipe[0], &done);
        (void)SetAclDeviceForTest(kOwnerDevice0);
        (void)AclFreeForTest(src0);
        (void)SetAclDeviceForTest(kOwnerDevice1);
        (void)AclFreeForTest(src1);
        (void)owner0.Finalize();
        (void)owner1.Finalize();
        ExitWithShutdown(0);
    }

    close(readyPipe[1]);
    close(donePipe[0]);
    close(addrPipe[1]);

    uint8_t ready = 0;
    if (!ReadByteWithTimeoutMs(readyPipe[0], &ready, 10000) || ready != 1) {
        close(readyPipe[0]);
        close(donePipe[1]);
        close(addrPipe[0]);
        (void)kill(ownerPid, SIGKILL);
        (void)waitpid(ownerPid, nullptr, 0);
        return Status(StatusCode::kRuntimeError, "owner not ready");
    }
    uint64_t remoteAddr0 = 0;
    uint64_t remoteAddr1 = 0;
    if (!ReadU64(addrPipe[0], &remoteAddr0) || !ReadU64(addrPipe[0], &remoteAddr1) ||
        remoteAddr0 == 0 || remoteAddr1 == 0) {
        close(readyPipe[0]);
        close(donePipe[1]);
        close(addrPipe[0]);
        (void)kill(ownerPid, SIGKILL);
        (void)waitpid(ownerPid, nullptr, 0);
        return Status(StatusCode::kRuntimeError, "failed to read owner remote addresses");
    }
    close(readyPipe[0]);
    close(addrPipe[0]);

    auto readOne = [&](uint16_t requesterPort, int32_t requesterDeviceId, uint16_t ownerPort, uint64_t remoteAddr,
                       uint8_t expected) -> Status {
        TE_RETURN_IF_ERROR(EnsureAclInitializedForTest());
        TE_RETURN_IF_ERROR(SetAclDeviceForTest(requesterDeviceId));
        TransferEngine requester;
        TE_RETURN_IF_ERROR(requester.Initialize("127.0.0.1:" + std::to_string(requesterPort), "ascend",
                                                "npu:" + std::to_string(requesterDeviceId)));

        void *dstDev = nullptr;
        TE_RETURN_IF_ERROR(AclMallocForTest(kPayloadSize, &dstDev));
        Status readRc = BatchReadOne(&requester, "127.0.0.1", ownerPort, remoteAddr,
                                     reinterpret_cast<uint64_t>(dstDev), kPayloadSize);
        if (readRc.IsError()) {
            (void)AclFreeForTest(dstDev);
            (void)requester.Finalize();
            return readRc;
        }

        std::vector<uint8_t> hostDst(kPayloadSize, 0);
        Status copyRc = AclMemcpyForTest(hostDst.data(), hostDst.size(), dstDev, kPayloadSize, kAclMemcpyDeviceToHost);
        (void)AclFreeForTest(dstDev);
        (void)requester.Finalize();
        if (copyRc.IsError()) {
            return copyRc;
        }
        for (auto v : hostDst) {
            if (v != expected) {
                return Status(StatusCode::kRuntimeError,
                              "verify failed, expected=" + std::to_string(expected) + ", got=" + std::to_string(v));
            }
        }
        return Status::OK();
    };

    Status rc0 = readOne(kRequesterPort0, kRequesterDevice0, kOwnerPort0, remoteAddr0, static_cast<uint8_t>(kOwnerDevice0 + 1));
    Status rc1 = readOne(kRequesterPort1, kRequesterDevice1, kOwnerPort1, remoteAddr1, static_cast<uint8_t>(kOwnerDevice1 + 1));

    (void)WriteByte(donePipe[1], 1);
    close(donePipe[1]);
    int ownerStatus = 0;
    (void)waitpid(ownerPid, &ownerStatus, 0);
    if (!WIFEXITED(ownerStatus) || WEXITSTATUS(ownerStatus) != 0) {
        return Status(StatusCode::kRuntimeError, "owner process exit with failure");
    }
    if (rc0.IsError()) {
        return rc0;
    }
    return rc1;
#else
    return Status(StatusCode::kNotSupported, "p2p backend disabled");
#endif
}

struct RequesterProcess {
    pid_t pid = -1;
    int resultReadFd = -1;
    std::string name;
};

Status RunRequesterReadTask(const std::string &visibleDevices, int32_t requesterDeviceId, uint16_t requesterPort,
                            uint16_t ownerPort, uint64_t remoteAddr, size_t payloadSize, uint8_t expected)
{
    ScopedEnvVar rtVisible("RT_ASCEND_VISIBLE_DEVICES", visibleDevices);
    ScopedEnvVar ascendRtVisible("ASCEND_RT_VISIBLE_DEVICES", visibleDevices);

    TE_RETURN_IF_ERROR(EnsureAclInitializedForTest());
    TE_RETURN_IF_ERROR(SetAclDeviceForTest(requesterDeviceId));

    TransferEngine requester;
    TE_RETURN_IF_ERROR(requester.Initialize("127.0.0.1:" + std::to_string(requesterPort), "ascend",
                                            "npu:" + std::to_string(requesterDeviceId)));

    void *dstDev = nullptr;
    TE_RETURN_IF_ERROR(AclMallocForTest(payloadSize, &dstDev));
    Status readRc =
        BatchReadOne(&requester, "127.0.0.1", ownerPort, remoteAddr, reinterpret_cast<uint64_t>(dstDev), payloadSize);
    if (readRc.IsError()) {
        (void)AclFreeForTest(dstDev);
        (void)requester.Finalize();
        return readRc;
    }

    std::vector<uint8_t> hostDst(payloadSize, 0);
    Status copyRc = AclMemcpyForTest(hostDst.data(), hostDst.size(), dstDev, payloadSize, kAclMemcpyDeviceToHost);
    (void)AclFreeForTest(dstDev);
    (void)requester.Finalize();
    if (copyRc.IsError()) {
        return copyRc;
    }

    for (auto v : hostDst) {
        if (v != expected) {
            return Status(StatusCode::kRuntimeError,
                          "verify failed, expected=" + std::to_string(expected) + ", got=" + std::to_string(v));
        }
    }
    return Status::OK();
}

Status SpawnRequesterProcess(const std::string &name, const std::string &visibleDevices, int32_t requesterDeviceId,
                             uint16_t requesterPort, uint16_t ownerPort, uint64_t remoteAddr, size_t payloadSize,
                             uint8_t expected, RequesterProcess *proc)
{
    TE_CHECK_PTR_OR_RETURN(proc);
    int resultPipe[2] = {-1, -1};
    if (pipe(resultPipe) != 0) {
        return Status(StatusCode::kRuntimeError, std::string("pipe failed: ") + std::strerror(errno));
    }

    const pid_t pid = fork();
    if (pid < 0) {
        close(resultPipe[0]);
        close(resultPipe[1]);
        return Status(StatusCode::kRuntimeError, std::string("fork failed: ") + std::strerror(errno));
    }

    if (pid == 0) {
        close(resultPipe[0]);
        Status rc = RunRequesterReadTask(visibleDevices, requesterDeviceId, requesterPort, ownerPort, remoteAddr,
                                         payloadSize, expected);
        const uint8_t ok = rc.IsOk() ? 1 : 0;
        (void)WriteByte(resultPipe[1], ok);
        (void)WriteString(resultPipe[1], rc.IsOk() ? std::string("ok") : rc.ToString());
        close(resultPipe[1]);
        ExitWithShutdown(0);
    }

    close(resultPipe[1]);
    proc->pid = pid;
    proc->resultReadFd = resultPipe[0];
    proc->name = name;
    return Status::OK();
}

Status WaitRequesterProcess(const RequesterProcess &proc)
{
    uint8_t ok = 0;
    std::string detail;
    if (!ReadByte(proc.resultReadFd, &ok) || !ReadString(proc.resultReadFd, &detail)) {
        close(proc.resultReadFd);
        (void)waitpid(proc.pid, nullptr, 0);
        return Status(StatusCode::kRuntimeError, proc.name + " failed to return result");
    }
    close(proc.resultReadFd);

    int childStatus = 0;
    if (waitpid(proc.pid, &childStatus, 0) < 0) {
        return Status(StatusCode::kRuntimeError, std::string("waitpid failed: ") + std::strerror(errno));
    }
    if (!WIFEXITED(childStatus) || WEXITSTATUS(childStatus) != 0) {
        return Status(StatusCode::kRuntimeError, proc.name + " process exited unexpectedly");
    }
    if (ok != 1) {
        return Status(StatusCode::kRuntimeError, proc.name + " failed: " + detail);
    }
    return Status::OK();
}

Status RunEightNpuFanOutReadCase()
{
#if defined(TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND) && TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND
    constexpr size_t kPayloadSize = 128;
    constexpr uint16_t kOwnerPort = 59651;
    constexpr uint16_t kRequesterBasePort = 59660;
    constexpr int32_t kOwnerDeviceId = 0;

    std::vector<uint8_t> src(kPayloadSize, 0x5A);
    ForkedTransferPipes pipes;
    TE_RETURN_IF_ERROR(CreateForkedTransferPipes(&pipes));

    const pid_t ownerPid = fork();
    if (ownerPid < 0) {
        ClosePipe(pipes.ready);
        ClosePipe(pipes.done);
        ClosePipe(pipes.addr);
        return Status(StatusCode::kRuntimeError, std::string("fork failed: ") + std::strerror(errno));
    }

    if (ownerPid == 0) {
        ScopedEnvVar rtVisible("RT_ASCEND_VISIBLE_DEVICES", "0");
        ScopedEnvVar ascendRtVisible("ASCEND_RT_VISIBLE_DEVICES", "0");
        OwnerProcessMain(true, src, kOwnerPort, kOwnerDeviceId, &pipes);
    }

    close(pipes.ready[1]);
    close(pipes.done[0]);
    close(pipes.addr[1]);

    uint64_t ownerSrcDevAddr = 0;
    TE_RETURN_IF_ERROR(WaitOwnerReadyAndReadAddr(ownerPid, &pipes, &ownerSrcDevAddr));

    std::vector<RequesterProcess> requesters;
    requesters.reserve(7);
    for (int dev = 1; dev <= 7; ++dev) {
        RequesterProcess proc;
        Status spawnRc =
            SpawnRequesterProcess("requester_" + std::to_string(dev), std::to_string(dev), 0,
                                  static_cast<uint16_t>(kRequesterBasePort + dev), kOwnerPort, ownerSrcDevAddr,
                                  kPayloadSize, src[0], &proc);
        if (spawnRc.IsError()) {
            (void)WriteByte(pipes.done[1], 1);
            close(pipes.done[1]);
            (void)kill(ownerPid, SIGKILL);
            (void)waitpid(ownerPid, nullptr, 0);
            return spawnRc;
        }
        requesters.emplace_back(proc);
    }

    Status firstErr = Status::OK();
    for (const auto &proc : requesters) {
        Status rc = WaitRequesterProcess(proc);
        if (firstErr.IsOk() && rc.IsError()) {
            firstErr = rc;
        }
    }

    (void)WriteByte(pipes.done[1], 1);
    close(pipes.done[1]);
    int ownerStatus = 0;
    (void)waitpid(ownerPid, &ownerStatus, 0);
    if (!WIFEXITED(ownerStatus) || WEXITSTATUS(ownerStatus) != 0) {
        return Status(StatusCode::kRuntimeError, "owner process exit with failure");
    }
    return firstErr;
#else
    return Status(StatusCode::kNotSupported, "p2p backend disabled");
#endif
}

Status RunTp2CrossGroupReadCase()
{
#if defined(TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND) && TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND
    constexpr size_t kPayloadSize = 128;
    constexpr uint16_t kOwnerPortA = 59751;
    constexpr uint16_t kOwnerPortB = 59752;
    constexpr uint16_t kRequesterPortA = 59761;
    constexpr uint16_t kRequesterPortB = 59762;
    constexpr char kOwnerVisible[] = "0,1";
    constexpr char kRequesterVisible[] = "2,3";

    ForkedTransferPipes ownerPipesA;
    ForkedTransferPipes ownerPipesB;
    TE_RETURN_IF_ERROR(CreateForkedTransferPipes(&ownerPipesA));
    TE_RETURN_IF_ERROR(CreateForkedTransferPipes(&ownerPipesB));

    std::vector<uint8_t> srcA(kPayloadSize, 0x31);
    std::vector<uint8_t> srcB(kPayloadSize, 0x42);

    const pid_t ownerPidA = fork();
    if (ownerPidA < 0) {
        return Status(StatusCode::kRuntimeError, std::string("fork ownerA failed: ") + std::strerror(errno));
    }
    if (ownerPidA == 0) {
        ScopedEnvVar rtVisible("RT_ASCEND_VISIBLE_DEVICES", kOwnerVisible);
        ScopedEnvVar ascendRtVisible("ASCEND_RT_VISIBLE_DEVICES", kOwnerVisible);
        OwnerProcessMain(true, srcA, kOwnerPortA, 0, &ownerPipesA);
    }

    const pid_t ownerPidB = fork();
    if (ownerPidB < 0) {
        (void)kill(ownerPidA, SIGKILL);
        (void)waitpid(ownerPidA, nullptr, 0);
        return Status(StatusCode::kRuntimeError, std::string("fork ownerB failed: ") + std::strerror(errno));
    }
    if (ownerPidB == 0) {
        ScopedEnvVar rtVisible("RT_ASCEND_VISIBLE_DEVICES", kOwnerVisible);
        ScopedEnvVar ascendRtVisible("ASCEND_RT_VISIBLE_DEVICES", kOwnerVisible);
        OwnerProcessMain(true, srcB, kOwnerPortB, 1, &ownerPipesB);
    }

    close(ownerPipesA.ready[1]);
    close(ownerPipesA.done[0]);
    close(ownerPipesA.addr[1]);
    close(ownerPipesB.ready[1]);
    close(ownerPipesB.done[0]);
    close(ownerPipesB.addr[1]);

    uint64_t ownerAddrA = 0;
    uint64_t ownerAddrB = 0;
    TE_RETURN_IF_ERROR(WaitOwnerReadyAndReadAddr(ownerPidA, &ownerPipesA, &ownerAddrA));
    TE_RETURN_IF_ERROR(WaitOwnerReadyAndReadAddr(ownerPidB, &ownerPipesB, &ownerAddrB));

    RequesterProcess requesterA;
    RequesterProcess requesterB;
    TE_RETURN_IF_ERROR(SpawnRequesterProcess("requester_3", kRequesterVisible, 0, kRequesterPortA, kOwnerPortA,
                                             ownerAddrA, kPayloadSize, srcA[0], &requesterA));
    TE_RETURN_IF_ERROR(SpawnRequesterProcess("requester_4", kRequesterVisible, 1, kRequesterPortB, kOwnerPortB,
                                             ownerAddrB, kPayloadSize, srcB[0], &requesterB));

    Status rcA = WaitRequesterProcess(requesterA);
    Status rcB = WaitRequesterProcess(requesterB);

    (void)WriteByte(ownerPipesA.done[1], 1);
    (void)WriteByte(ownerPipesB.done[1], 1);
    close(ownerPipesA.done[1]);
    close(ownerPipesB.done[1]);

    int ownerStatusA = 0;
    int ownerStatusB = 0;
    (void)waitpid(ownerPidA, &ownerStatusA, 0);
    (void)waitpid(ownerPidB, &ownerStatusB, 0);
    if (!WIFEXITED(ownerStatusA) || WEXITSTATUS(ownerStatusA) != 0 || !WIFEXITED(ownerStatusB) ||
        WEXITSTATUS(ownerStatusB) != 0) {
        return Status(StatusCode::kRuntimeError, "owner process exit with failure");
    }
    if (rcA.IsError()) {
        return rcA;
    }
    return rcB;
#else
    return Status(StatusCode::kNotSupported, "p2p backend disabled");
#endif
}

// 中文说明：验证在被读端先注册内存后，调用 BatchTransferSyncRead 可成功通过控制面与数据面流程。
TEST(TransferEngineBasicTest, SyncReadRegisteredOk)
{
    StatusCode requesterCode = StatusCode::kRuntimeError;
    std::string requesterMsg;
    std::vector<uint8_t> dst;
#if defined(TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND) && TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND
    Status aclRc = EnsureAclInitializedForTest();
    if (aclRc.IsError()) {
        GTEST_SKIP() << "skip because acl init is unavailable: " << aclRc.ToString();
    }
    Status rc = RunForkedTransferCase(true, &requesterCode, &requesterMsg, &dst);
#else
    Status rc = RunInProcessTransferCase(true, &requesterCode, &requesterMsg, &dst);
#endif
    ASSERT_TRUE(rc.IsOk()) << rc.GetMsg();
    EXPECT_EQ(requesterCode, StatusCode::kOk) << "requester status msg: " << requesterMsg;
    EXPECT_EQ(dst, std::vector<uint8_t>(128, 7));
}

// 中文说明：验证未注册内存范围时，读取请求会被拒绝并返回 kNotAuthorized。
TEST(TransferEngineBasicTest, SyncReadRejectUnregistered)
{
    StatusCode requesterCode = StatusCode::kRuntimeError;
    std::string requesterMsg;
    std::vector<uint8_t> dst;
#if defined(TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND) && TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND
    Status aclRc = EnsureAclInitializedForTest();
    if (aclRc.IsError()) {
        GTEST_SKIP() << "skip because acl init is unavailable: " << aclRc.ToString();
    }
    Status rc = RunForkedTransferCase(false, &requesterCode, &requesterMsg, &dst);
#else
    Status rc = RunInProcessTransferCase(false, &requesterCode, &requesterMsg, &dst);
#endif
    ASSERT_TRUE(rc.IsOk()) << rc.GetMsg();
    EXPECT_EQ(requesterCode, StatusCode::kNotAuthorized) << "requester status msg: " << requesterMsg;
}

// 中文说明：验证 BatchTransferSyncRead 的入参校验，空 peer_host 会返回 kInvalid。
TEST(TransferEngineBasicTest, SyncReadArgsInvalid)
{
    TransferEngine requester;
    std::vector<uint8_t> src(64, 1);
    std::vector<uint8_t> dst(64, 0);

    Status initInvalidRc = requester.Initialize("127.0.0.1:57051", "ascend", "npu:x");
    EXPECT_EQ(initInvalidRc.GetCode(), StatusCode::kInvalid);

    ASSERT_TRUE(requester.Initialize("127.0.0.1:57052", "ascend", "npu:2").IsOk());

    Status status = BatchReadOne(&requester, "", 57051, reinterpret_cast<uintptr_t>(src.data()),
                                 reinterpret_cast<uint64_t>(dst.data()), dst.size());
    EXPECT_EQ(status.GetCode(), StatusCode::kInvalid);
}

// 中文说明：验证同节点同 device_id 场景不再由控制面提前拦截，可走完 mock 数据面读流程。
TEST(TransferEngineBasicTest, SyncReadSameDeviceMockOk)
{
    std::vector<uint8_t> src(64, 1);
    std::vector<uint8_t> dst(64, 0);

    auto sharedState = std::make_shared<MockDataPlaneBackend::SharedState>();
    auto ownerBackend = std::make_shared<MockDataPlaneBackend>(sharedState);
    auto requesterBackend = std::make_shared<MockDataPlaneBackend>(sharedState);
    TransferEngine owner(ownerBackend);
    TransferEngine requester(requesterBackend);
    ASSERT_TRUE(owner.Initialize("127.0.0.1:58051", "ascend", "npu:0").IsOk());
    ASSERT_TRUE(requester.Initialize("127.0.0.1:58052", "ascend", "npu:0").IsOk());
    ASSERT_TRUE(owner.RegisterMemory(reinterpret_cast<uintptr_t>(src.data()), src.size()).IsOk());

    Status rc = BatchReadOne(&requester, "127.0.0.1", 58051, reinterpret_cast<uintptr_t>(src.data()),
                             reinterpret_cast<uint64_t>(dst.data()), dst.size());
    EXPECT_TRUE(rc.IsOk()) << rc.ToString();
    EXPECT_EQ(dst, src);

    (void)requester.Finalize();
    (void)owner.Finalize();
}

// 中文说明：验证 owner/requester 分别设置 RT_ASCEND_VISIBLE_DEVICES=0/1 时，
// 同节点且逻辑卡号均为0仍可完成 batch_transfer_sync_read。
TEST(TransferEngineBasicTest, SyncReadPerProcVisibleOk)
{
#if defined(TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND) && TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND
    int resultPipe[2] = {-1, -1};
    ASSERT_EQ(pipe(resultPipe), 0);

    const pid_t pid = fork();
    ASSERT_GE(pid, 0) << "fork failed: " << std::strerror(errno);
    if (pid == 0) {
        close(resultPipe[0]);
        StatusCode requesterCode = StatusCode::kRuntimeError;
        std::string requesterMsg;
        std::vector<uint8_t> dst;
        const Status rc = RunForkedTransferCaseWithRtAscendVisibleDevices(&requesterCode, &requesterMsg, &dst);
        uint8_t tag = 0;
        std::string detail;
        if (rc.GetCode() == StatusCode::kNotSupported) {
            tag = 2;
            detail = rc.ToString();
        } else if (rc.IsError()) {
            tag = 0;
            detail = rc.ToString();
        } else if (requesterCode != StatusCode::kOk) {
            tag = 0;
            detail = "requester status is not kOk: " + requesterMsg;
        } else if (dst != std::vector<uint8_t>(128, 7)) {
            tag = 0;
            detail = "dst verify failed";
        } else {
            tag = 1;
            detail = "ok";
        }
        (void)WriteByte(resultPipe[1], tag);
        (void)WriteString(resultPipe[1], detail);
        close(resultPipe[1]);
        ExitWithShutdown(0);
    }

    close(resultPipe[1]);
    uint8_t tag = 0;
    std::string detail;
    ASSERT_TRUE(ReadByte(resultPipe[0], &tag));
    ASSERT_TRUE(ReadString(resultPipe[0], &detail));
    close(resultPipe[0]);
    int status = 0;
    ASSERT_EQ(waitpid(pid, &status, 0), pid);
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 0);

    if (tag == 2) {
        GTEST_SKIP() << detail;
    }
    ASSERT_EQ(tag, 1) << detail;
#else
    GTEST_SKIP() << "skip because p2p backend is disabled";
#endif
}

// 中文说明：验证真实 NPU 场景下（fork + device memory），两个 requester 并发读取 owner 不同显存区域可正确完成。
TEST(TransferEngineBasicTest, ConcurrentReadersOk)
{
#if defined(TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND) && TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND
    Status aclRc = EnsureAclInitializedForTest();
    if (aclRc.IsError()) {
        GTEST_SKIP() << "skip because acl init is unavailable: " << aclRc.ToString();
    }
    Status rc = RunForkedConcurrentNpuReadCase();
    ASSERT_TRUE(rc.IsOk()) << rc.ToString();
#else
    GTEST_SKIP() << "skip because p2p backend is disabled";
#endif
}

// 中文说明：验证真实 NPU 场景下，owner 单进程绑定多个 npu_id 显存并分别被 requester 成功读取。
TEST(TransferEngineBasicTest, MultiNpuOwnerReadOk)
{
#if defined(TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND) && TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND
    Status aclRc = EnsureAclInitializedForTest();
    if (aclRc.IsError()) {
        GTEST_SKIP() << "skip because acl init is unavailable: " << aclRc.ToString();
    }
    Status rc = RunForkedMultiNpuOwnerReadCase();
    ASSERT_TRUE(rc.IsOk()) << rc.ToString();
#else
    GTEST_SKIP() << "skip because p2p backend is disabled";
#endif
}

// 中文说明：验证 8 进程分别设置 ASCEND_RT_VISIBLE_DEVICES=0~7 后，
// owner(0卡) 注册内存，requester(1~7卡) 可从 owner 拉取并校验数据。
TEST(TransferEngineBasicTest, EightNpuFanOutReadOk)
{
#if defined(TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND) && TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND
    Status rc = RunEightNpuFanOutReadCase();
    if (rc.GetCode() == StatusCode::kNotSupported || rc.GetCode() == StatusCode::kNotReady) {
        GTEST_SKIP() << rc.ToString();
    }
    ASSERT_TRUE(rc.IsOk()) << rc.ToString();
#else
    GTEST_SKIP() << "skip because p2p backend is disabled";
#endif
}

// 中文说明：模拟 vLLM Ascend tp=2 映射：
// owner1/2 在 ASCEND_RT_VISIBLE_DEVICES=0,1 上使用 device_id=0/1；
// requester3/4 在 ASCEND_RT_VISIBLE_DEVICES=2,3 上使用 device_id=0/1，
// requester3 读取 owner1，requester4 读取 owner2。
TEST(TransferEngineBasicTest, Tp2CrossGroupReadOk)
{
#if defined(TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND) && TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND
    Status rc = RunTp2CrossGroupReadCase();
    if (rc.GetCode() == StatusCode::kNotSupported || rc.GetCode() == StatusCode::kNotReady) {
        GTEST_SKIP() << rc.ToString();
    }
    ASSERT_TRUE(rc.IsOk()) << rc.ToString();
#else
    GTEST_SKIP() << "skip because p2p backend is disabled";
#endif
}

}  // namespace
}  // namespace datasystem
