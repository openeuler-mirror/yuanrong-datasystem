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
 * Description: HIXL RH2D smoke test.
 *
 * Confirms cann_hixl is linked correctly and the environment can pull a host buffer on a "server" into device memory on
 * a "client" via HIXL's public API directly (i.e. no datasystem RemoteH2DManager/AclDeviceManager paths are exercised).
 * HIXL_RH2D_MODE selects RoCE or HCCS with HIXL buffer-pool relay.
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <cerrno>
#include <cctype>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include <sys/wait.h>
#include <unistd.h>

#include "acl/acl.h"
#include "datasystem/common/log/log.h"
#include "hixl/hixl.h"
#include "hixl/hixl_types.h"

namespace {

constexpr int32_t kDefaultServerDeviceId = 0;
constexpr int32_t kDefaultClientDeviceId = 1;
constexpr size_t kBufferSize = 4096;
constexpr int32_t kDefaultHixlTimeoutMs = 10000;
constexpr const char *kServerDeviceIdEnv = "HIXL_SERVER_DEVICE_ID";
constexpr const char *kClientDeviceIdEnv = "HIXL_CLIENT_DEVICE_ID";
constexpr const char *kHixlTimeoutMsEnv = "HIXL_TIMEOUT_MS";
constexpr const char *kServerEngineEnv = "HIXL_SERVER_ENGINE";
constexpr const char *kClientEngineEnv = "HIXL_CLIENT_ENGINE";
constexpr const char *kRh2dModeEnv = "HIXL_RH2D_MODE";
constexpr const char *kRoceForceEnv = "HCCL_INTRA_ROCE_ENABLE";
constexpr const char *kDefaultServerEngine = "127.0.0.1:34701";
constexpr const char *kDefaultClientEngine = "127.0.0.1:34702";
constexpr const char *kNoBufferPoolConfig = "0:0";
constexpr const char *kRelayBufferPoolConfig = "4:8";

enum class Rh2dMode {
    ROCE,
    HCCS_BUFFERPOOL,
};

enum class ServerStatus : int32_t {
    SUCCESS = 0,
    SET_DEVICE_FAILED,
    MALLOC_HOST_FAILED,
    HIXL_INITIALIZE_FAILED,
    REGISTER_HOST_MEM_FAILED,
    CLIENT_DONE_SIGNAL_MISSING,
    PUBLISH_READY_FAILED,
};

enum class ClientStatus : int32_t {
    SUCCESS = 0,
    SET_DEVICE_FAILED,
    MALLOC_DEVICE_FAILED,
    HIXL_INITIALIZE_FAILED,
    REGISTER_DEVICE_MEM_FAILED,
    HIXL_CONNECT_FAILED,
    HIXL_TRANSFER_FAILED,
    VERIFY_MISMATCH,
    VERIFY_COPY_FAILED,
};

constexpr int kWaitChildFailed = 255;
constexpr int kSignalExitStatusBase = 128;

int StatusCode(ServerStatus status)
{
    return static_cast<int>(status);
}

int StatusCode(ClientStatus status)
{
    return static_cast<int>(status);
}

const char *HixlStatusName(hixl::Status status)
{
    switch (status) {
        case hixl::SUCCESS:
            return "SUCCESS";
        case hixl::PARAM_INVALID:
            return "PARAM_INVALID";
        case hixl::TIMEOUT:
            return "TIMEOUT";
        case hixl::NOT_CONNECTED:
            return "NOT_CONNECTED";
        case hixl::ALREADY_CONNECTED:
            return "ALREADY_CONNECTED";
        case hixl::NOTIFY_FAILED:
            return "NOTIFY_FAILED";
        case hixl::UNSUPPORTED:
            return "UNSUPPORTED";
        case hixl::FAILED:
            return "FAILED";
        case hixl::RESOURCE_EXHAUSTED:
            return "RESOURCE_EXHAUSTED";
        default:
            return "UNKNOWN";
    }
}

const char *ServerStatusName(ServerStatus status)
{
    switch (status) {
        case ServerStatus::SUCCESS:
            return "success";
        case ServerStatus::SET_DEVICE_FAILED:
            return "aclrtSetDevice failed";
        case ServerStatus::MALLOC_HOST_FAILED:
            return "aclrtMallocHost failed";
        case ServerStatus::HIXL_INITIALIZE_FAILED:
            return "Hixl::Initialize failed";
        case ServerStatus::REGISTER_HOST_MEM_FAILED:
            return "Hixl::RegisterMem(MEM_HOST) failed";
        case ServerStatus::CLIENT_DONE_SIGNAL_MISSING:
            return "client done signal missing";
        case ServerStatus::PUBLISH_READY_FAILED:
            return "failed to publish ready state";
        default:
            return "unknown";
    }
}

const char *ServerStatusName(int status)
{
    return ServerStatusName(static_cast<ServerStatus>(status));
}

const char *ClientStatusName(ClientStatus status)
{
    switch (status) {
        case ClientStatus::SUCCESS:
            return "success";
        case ClientStatus::SET_DEVICE_FAILED:
            return "aclrtSetDevice failed";
        case ClientStatus::MALLOC_DEVICE_FAILED:
            return "aclrtMalloc device failed";
        case ClientStatus::HIXL_INITIALIZE_FAILED:
            return "Hixl::Initialize failed";
        case ClientStatus::REGISTER_DEVICE_MEM_FAILED:
            return "Hixl::RegisterMem(MEM_DEVICE) failed";
        case ClientStatus::HIXL_CONNECT_FAILED:
            return "Hixl::Connect failed";
        case ClientStatus::HIXL_TRANSFER_FAILED:
            return "Hixl::TransferSync(READ) failed";
        case ClientStatus::VERIFY_MISMATCH:
            return "device buffer verification mismatch";
        case ClientStatus::VERIFY_COPY_FAILED:
            return "aclrtMemcpy verification failed";
        default:
            return "unknown";
    }
}

const char *ClientStatusName(int status)
{
    return ClientStatusName(static_cast<ClientStatus>(status));
}

const char *EnvOrUnset(const char *name)
{
    const char *value = std::getenv(name);
    return value == nullptr ? "<unset>" : value;
}

std::string EnvOrDefault(const char *name, const char *defaultValue)
{
    const char *value = std::getenv(name);
    return value == nullptr || value[0] == '\0' ? defaultValue : value;
}

std::string ToLower(std::string value)
{
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return value;
}

Rh2dMode ParseRh2dMode()
{
    const std::string value = ToLower(EnvOrDefault(kRh2dModeEnv, "roce"));
    if (value == "roce") {
        return Rh2dMode::ROCE;
    }
    if (value != "hccs-bufferpool" && value != "hccs_bufferpool" && value != "hccs") {
        LOG(WARNING) << "Invalid " << kRh2dModeEnv << "=" << value << ", using roce mode.";
        return Rh2dMode::ROCE;
    }
    return Rh2dMode::HCCS_BUFFERPOOL;
}

const char *Rh2dModeName(Rh2dMode mode)
{
    switch (mode) {
        case Rh2dMode::ROCE:
            return "roce";
        case Rh2dMode::HCCS_BUFFERPOOL:
            return "hccs-bufferpool";
        default:
            return "UNKNOWN";
    }
}

bool UseBufferPool(Rh2dMode mode)
{
    return mode == Rh2dMode::HCCS_BUFFERPOOL;
}

const char *BufferPoolOptionValue(Rh2dMode mode)
{
    if (UseBufferPool(mode)) {
        return kRelayBufferPoolConfig;
    }
    return kNoBufferPoolConfig;
}

struct EnvSnapshot {
    bool exists = false;
    std::string value;
};

EnvSnapshot CaptureEnv(const char *name)
{
    const char *value = std::getenv(name);
    if (value == nullptr) {
        return {};
    }
    return { true, value };
}

void RestoreEnv(const char *name, const EnvSnapshot &snapshot)
{
    if (snapshot.exists) {
        (void)setenv(name, snapshot.value.c_str(), 1);
        return;
    }
    (void)unsetenv(name);
}

void ConfigureLinkEnv(Rh2dMode mode)
{
    if (mode == Rh2dMode::ROCE) {
        (void)setenv(kRoceForceEnv, "1", 1);
        return;
    }
    (void)unsetenv(kRoceForceEnv);
}

int32_t EnvIntOrDefault(const char *name, int32_t defaultValue, int32_t minValue)
{
    const char *value = std::getenv(name);
    if (value == nullptr || value[0] == '\0') {
        return defaultValue;
    }
    char *end = nullptr;
    errno = 0;
    const long parsed = std::strtol(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0' || parsed < minValue
        || parsed > std::numeric_limits<int32_t>::max()) {
        LOG(WARNING) << "Invalid integer env " << name << "=" << value << ", using default " << defaultValue;
        return defaultValue;
    }
    return static_cast<int32_t>(parsed);
}

const char *GetAclRecentErrMsg()
{
    const char *msg = aclGetRecentErrMsg();
    return msg == nullptr ? "no acl recent error" : msg;
}

void LogEnvironment(const std::string &serverEngine, const std::string &clientEngine, int32_t serverDeviceId,
                    int32_t clientDeviceId, int32_t hixlTimeoutMs, Rh2dMode mode)
{
    LOG(INFO) << "HIXL RH2D smoke test env: server_device_id=" << serverDeviceId
              << ", client_device_id=" << clientDeviceId << ", buffer_size=" << kBufferSize
              << ", hixl_timeout_ms=" << hixlTimeoutMs << ", server_engine=" << serverEngine
              << ", client_engine=" << clientEngine << ", " << kServerEngineEnv << "=" << EnvOrUnset(kServerEngineEnv)
              << ", " << kClientEngineEnv << "=" << EnvOrUnset(kClientEngineEnv) << ", " << kRh2dModeEnv << "="
              << Rh2dModeName(mode) << ", HIXL " << hixl::OPTION_BUFFER_POOL << "=" << BufferPoolOptionValue(mode)
              << ", " << kRoceForceEnv << "=" << EnvOrUnset(kRoceForceEnv)
              << ", " << kServerDeviceIdEnv << "=" << EnvOrUnset(kServerDeviceIdEnv) << ", " << kClientDeviceIdEnv
              << "=" << EnvOrUnset(kClientDeviceIdEnv) << ", " << kHixlTimeoutMsEnv << "="
              << EnvOrUnset(kHixlTimeoutMsEnv)
              << ", ASCEND_VISIBLE_DEVICES=" << EnvOrUnset("ASCEND_VISIBLE_DEVICES")
              << ", ASCEND_RT_VISIBLE_DEVICES=" << EnvOrUnset("ASCEND_RT_VISIBLE_DEVICES");
    if (serverEngine == kDefaultServerEngine || clientEngine == kDefaultClientEngine) {
        LOG(WARNING) << "Using loopback HIXL engine defaults. RoCE or HCCS validation may require "
                     << kServerEngineEnv << " and " << kClientEngineEnv << " to be real Ascend network IPs.";
    }
}

void LogAclFailure(const char *step, aclError status)
{
    LOG(ERROR) << step << " failed, acl status=" << status << ", recent error=" << GetAclRecentErrMsg();
}

void LogHixlFailure(const char *step, hixl::Status status)
{
    LOG(ERROR) << step << " failed, hixl status=" << status << " (" << HixlStatusName(status)
               << "), recent acl error=" << GetAclRecentErrMsg();
}

struct HixlSmokePipes {
    int ready[2] = {-1, -1};
    int done[2] = {-1, -1};
};

struct ServerReadyMessage {
    int32_t status = -1;
    uint64_t hostAddr = 0;
};

void CloseFd(int &fd)
{
    if (fd >= 0) {
        (void)close(fd);
        fd = -1;
    }
}

void ClosePipe(int pipeFd[2])
{
    CloseFd(pipeFd[0]);
    CloseFd(pipeFd[1]);
}

void ClosePipes(HixlSmokePipes *pipes)
{
    ClosePipe(pipes->ready);
    ClosePipe(pipes->done);
}

bool CreatePipes(HixlSmokePipes *pipes)
{
    if (pipe(pipes->ready) != 0 || pipe(pipes->done) != 0) {
        ClosePipes(pipes);
        return false;
    }
    return true;
}

bool WriteAll(int fd, const void *data, size_t size)
{
    const auto *cursor = static_cast<const uint8_t *>(data);
    size_t written = 0;
    while (written < size) {
        const ssize_t rc = write(fd, cursor + written, size - written);
        if (rc < 0 && errno == EINTR) {
            continue;
        }
        if (rc <= 0) {
            return false;
        }
        written += static_cast<size_t>(rc);
    }
    return true;
}

bool ReadAll(int fd, void *data, size_t size)
{
    auto *cursor = static_cast<uint8_t *>(data);
    size_t readSize = 0;
    while (readSize < size) {
        const ssize_t rc = read(fd, cursor + readSize, size - readSize);
        if (rc < 0 && errno == EINTR) {
            continue;
        }
        if (rc <= 0) {
            return false;
        }
        readSize += static_cast<size_t>(rc);
    }
    return true;
}

bool WriteI32(int fd, int32_t value)
{
    return WriteAll(fd, &value, sizeof(value));
}

bool ReadI32(int fd, int32_t *value)
{
    return value != nullptr && ReadAll(fd, value, sizeof(*value));
}

bool WriteServerReady(int fd, const ServerReadyMessage &message)
{
    return WriteAll(fd, &message, sizeof(message));
}

template <typename StatusT>
[[noreturn]] void ExitChild(StatusT status)
{
    _exit(StatusCode(status));
}

class ScopeExit {
public:
    explicit ScopeExit(std::function<void()> fn) : fn_(std::move(fn))
    {
    }

    ~ScopeExit()
    {
        if (fn_) {
            fn_();
        }
    }

    ScopeExit(const ScopeExit &) = delete;
    ScopeExit &operator=(const ScopeExit &) = delete;

private:
    std::function<void()> fn_;
};

ServerStatus ServerFail(int readyWriteFd, ServerStatus status)
{
    ServerReadyMessage message;
    message.status = static_cast<int32_t>(status);
    (void)WriteServerReady(readyWriteFd, message);
    return status;
}

std::map<hixl::AscendString, hixl::AscendString> CreateHixlOptions(Rh2dMode mode)
{
    std::map<hixl::AscendString, hixl::AscendString> options;
    options[hixl::AscendString(hixl::OPTION_BUFFER_POOL)] = hixl::AscendString(BufferPoolOptionValue(mode));
    return options;
}

ServerStatus ServerProcessMain(const std::string &serverEngine, int32_t serverDeviceId, Rh2dMode mode,
                               HixlSmokePipes *pipes)
{
    CloseFd(pipes->ready[0]);
    CloseFd(pipes->done[1]);

    auto aclStatus = aclrtSetDevice(serverDeviceId);
    if (aclStatus != ACL_ERROR_NONE) {
        LogAclFailure("server aclrtSetDevice", aclStatus);
        return ServerFail(pipes->ready[1], ServerStatus::SET_DEVICE_FAILED);
    }
    ScopeExit resetDevice([&]() { (void)aclrtResetDevice(serverDeviceId); });

    void *rawHostBuf = nullptr;
    aclStatus = aclrtMallocHost(&rawHostBuf, kBufferSize);
    if (aclStatus != ACL_ERROR_NONE) {
        LogAclFailure("server aclrtMallocHost", aclStatus);
        return ServerFail(pipes->ready[1], ServerStatus::MALLOC_HOST_FAILED);
    }
    auto *hostBuf = static_cast<uint8_t *>(rawHostBuf);
    ScopeExit freeHost([&]() { (void)aclrtFreeHost(rawHostBuf); });
    for (size_t i = 0; i < kBufferSize; ++i) {
        hostBuf[i] = static_cast<uint8_t>(i & 0xFFU);
    }

    hixl::Hixl engine;
    auto options = CreateHixlOptions(mode);
    auto hixlStatus = engine.Initialize(hixl::AscendString(serverEngine.c_str()), options);
    if (hixlStatus != hixl::SUCCESS) {
        LogHixlFailure("server Hixl::Initialize", hixlStatus);
        return ServerFail(pipes->ready[1], ServerStatus::HIXL_INITIALIZE_FAILED);
    }
    ScopeExit finalizeEngine([&]() { engine.Finalize(); });

    hixl::MemHandle hostHandle = nullptr;
    if (!UseBufferPool(mode)) {
        hixl::MemDesc desc{};
        desc.addr = reinterpret_cast<uintptr_t>(hostBuf);
        desc.len = kBufferSize;
        hixlStatus = engine.RegisterMem(desc, hixl::MEM_HOST, hostHandle);
        if (hixlStatus != hixl::SUCCESS) {
            LogHixlFailure("server Hixl::RegisterMem(MEM_HOST)", hixlStatus);
            return ServerFail(pipes->ready[1], ServerStatus::REGISTER_HOST_MEM_FAILED);
        }
    } else {
        LOG(INFO) << "HIXL buffer-pool mode: skip server RegisterMem(MEM_HOST).";
    }
    ScopeExit deregisterHostMem([&]() {
        if (hostHandle != nullptr) {
            (void)engine.DeregisterMem(hostHandle);
        }
    });

    LOG(INFO) << "HIXL server ready: host_addr=" << static_cast<void *>(hostBuf) << ", len=" << kBufferSize
              << ", mode=" << Rh2dModeName(mode) << ", host_handle=" << hostHandle;
    ServerReadyMessage ready;
    ready.status = static_cast<int32_t>(ServerStatus::SUCCESS);
    ready.hostAddr = reinterpret_cast<uintptr_t>(hostBuf);
    if (!WriteServerReady(pipes->ready[1], ready)) {
        LOG(ERROR) << "server failed to publish ready state";
        return ServerStatus::PUBLISH_READY_FAILED;
    }

    int32_t done = 0;
    if (!ReadI32(pipes->done[0], &done)) {
        LOG(ERROR) << "server failed to read client done signal";
        return ServerStatus::CLIENT_DONE_SIGNAL_MISSING;
    }
    return ServerStatus::SUCCESS;
}

ClientStatus ClientProcessMain(const std::string &serverEngine, const std::string &clientEngine, int32_t clientDeviceId,
                               uint64_t serverHostAddr, int32_t hixlTimeoutMs, Rh2dMode mode, int doneWriteFd)
{
    ScopeExit notifyServerDone([&]() {
        (void)WriteI32(doneWriteFd, 1);
        CloseFd(doneWriteFd);
    });

    auto aclStatus = aclrtSetDevice(clientDeviceId);
    if (aclStatus != ACL_ERROR_NONE) {
        LogAclFailure("client aclrtSetDevice", aclStatus);
        return ClientStatus::SET_DEVICE_FAILED;
    }
    ScopeExit resetDevice([&]() { (void)aclrtResetDevice(clientDeviceId); });

    void *devBuf = nullptr;
    aclStatus = aclrtMalloc(&devBuf, kBufferSize, ACL_MEM_MALLOC_HUGE_ONLY);
    if (aclStatus != ACL_ERROR_NONE) {
        LogAclFailure("client aclrtMalloc", aclStatus);
        return ClientStatus::MALLOC_DEVICE_FAILED;
    }
    ScopeExit freeDevice([&]() { (void)aclrtFree(devBuf); });

    hixl::Hixl engine;
    auto options = CreateHixlOptions(mode);
    auto hixlStatus = engine.Initialize(hixl::AscendString(clientEngine.c_str()), options);
    if (hixlStatus != hixl::SUCCESS) {
        LogHixlFailure("client Hixl::Initialize", hixlStatus);
        return ClientStatus::HIXL_INITIALIZE_FAILED;
    }
    ScopeExit finalizeEngine([&]() { engine.Finalize(); });

    hixl::MemDesc desc{};
    desc.addr = reinterpret_cast<uintptr_t>(devBuf);
    desc.len = kBufferSize;
    hixl::MemHandle handle = nullptr;
    hixlStatus = engine.RegisterMem(desc, hixl::MEM_DEVICE, handle);
    if (hixlStatus != hixl::SUCCESS) {
        LogHixlFailure("client Hixl::RegisterMem(MEM_DEVICE)", hixlStatus);
        return ClientStatus::REGISTER_DEVICE_MEM_FAILED;
    }
    ScopeExit deregisterMem([&]() { (void)engine.DeregisterMem(handle); });

    LOG(INFO) << "HIXL client connecting: local_device_addr=" << devBuf
              << ", remote_host_addr=" << reinterpret_cast<void *>(serverHostAddr) << ", len=" << kBufferSize
              << ", handle=" << handle;
    hixlStatus = engine.Connect(hixl::AscendString(serverEngine.c_str()), hixlTimeoutMs);
    if (hixlStatus != hixl::SUCCESS) {
        LogHixlFailure("client Hixl::Connect", hixlStatus);
        return ClientStatus::HIXL_CONNECT_FAILED;
    }
    ScopeExit disconnect([&]() { (void)engine.Disconnect(hixl::AscendString(serverEngine.c_str()), hixlTimeoutMs); });

    hixl::TransferOpDesc op{reinterpret_cast<uintptr_t>(devBuf), serverHostAddr, kBufferSize};
    const auto readStatus =
        engine.TransferSync(hixl::AscendString(serverEngine.c_str()), hixl::READ, {op}, hixlTimeoutMs);

    if (readStatus != hixl::SUCCESS) {
        LogHixlFailure("client Hixl::TransferSync(READ)", readStatus);
        return ClientStatus::HIXL_TRANSFER_FAILED;
    }

    std::vector<uint8_t> verify(kBufferSize, 0U);
    const auto copyStatus = aclrtMemcpy(verify.data(), kBufferSize, devBuf, kBufferSize, ACL_MEMCPY_DEVICE_TO_HOST);
    if (copyStatus != ACL_ERROR_NONE) {
        LogAclFailure("client aclrtMemcpy verify", copyStatus);
        return ClientStatus::VERIFY_COPY_FAILED;
    }

    for (size_t i = 0; i < kBufferSize; ++i) {
        if (verify[i] != static_cast<uint8_t>(i & 0xFFU)) {
            LOG(ERROR) << "verify mismatch at index=" << i << ", expected=" << static_cast<int>(i & 0xFFU)
                       << ", actual=" << static_cast<int>(verify[i]);
            return ClientStatus::VERIFY_MISMATCH;
        }
    }
    return ClientStatus::SUCCESS;
}

int WaitChild(pid_t pid)
{
    int status = 0;
    if (waitpid(pid, &status, 0) != pid) {
        LOG(ERROR) << "waitpid failed for pid=" << pid << ", errno=" << errno << " (" << std::strerror(errno) << ")";
        return kWaitChildFailed;
    }
    if (WIFEXITED(status)) {
        return WEXITSTATUS(status);
    }
    if (WIFSIGNALED(status)) {
        return kSignalExitStatusBase + WTERMSIG(status);
    }
    return kWaitChildFailed;
}

}  // namespace

class HixlRh2dSmokeTest : public ::testing::Test {};

// Server process owns a host buffer with a known byte pattern. Client process pulls it into local HBM via
// TransferSync(READ). The descriptor shape is identical across all test modes.
TEST_F(HixlRh2dSmokeTest, DISABLED_OneSidedReadFromHostMemory)
{
    const std::string serverEngine = EnvOrDefault(kServerEngineEnv, kDefaultServerEngine);
    const std::string clientEngine = EnvOrDefault(kClientEngineEnv, kDefaultClientEngine);
    const int32_t serverDeviceId = EnvIntOrDefault(kServerDeviceIdEnv, kDefaultServerDeviceId, 0);
    const int32_t clientDeviceId = EnvIntOrDefault(kClientDeviceIdEnv, kDefaultClientDeviceId, 0);
    const int32_t hixlTimeoutMs = EnvIntOrDefault(kHixlTimeoutMsEnv, kDefaultHixlTimeoutMs, 1);
    const Rh2dMode mode = ParseRh2dMode();
    const auto savedRoceEnv = CaptureEnv(kRoceForceEnv);
    ConfigureLinkEnv(mode);
    ScopeExit restoreRoceEnv([&]() { RestoreEnv(kRoceForceEnv, savedRoceEnv); });
    LogEnvironment(serverEngine, clientEngine, serverDeviceId, clientDeviceId, hixlTimeoutMs, mode);

    HixlSmokePipes pipes;
    ASSERT_TRUE(CreatePipes(&pipes)) << "pipe failed: " << std::strerror(errno);

    pid_t serverPid = fork();
    ASSERT_GE(serverPid, 0) << "fork server failed: " << std::strerror(errno);
    if (serverPid == 0) {
        const ServerStatus status = ServerProcessMain(serverEngine, serverDeviceId, mode, &pipes);
        ExitChild(status);
    }

    CloseFd(pipes.ready[1]);
    CloseFd(pipes.done[0]);

    ServerReadyMessage serverReady;
    if (!ReadAll(pipes.ready[0], &serverReady, sizeof(serverReady))) {
        CloseFd(pipes.done[1]);
        const int serverStatus = WaitChild(serverPid);
        FAIL() << "failed to read server ready status, server_status=" << serverStatus;
    }
    if (serverReady.status != StatusCode(ServerStatus::SUCCESS)) {
        CloseFd(pipes.done[1]);
        const int serverStatus = WaitChild(serverPid);
        FAIL() << "server process failed before ready, status=" << serverReady.status
               << " (" << ServerStatusName(serverReady.status) << "), exit_status=" << serverStatus;
    }
    if (serverReady.hostAddr == 0) {
        (void)WriteI32(pipes.done[1], 1);
        CloseFd(pipes.done[1]);
        const int serverStatus = WaitChild(serverPid);
        FAIL() << "server returned empty host address, server_status=" << serverStatus;
    }

    pid_t clientPid = fork();
    if (clientPid < 0) {
        (void)WriteI32(pipes.done[1], 1);
        CloseFd(pipes.done[1]);
        const int serverStatus = WaitChild(serverPid);
        FAIL() << "fork client failed: " << std::strerror(errno) << ", server_status=" << serverStatus;
    }
    if (clientPid == 0) {
        CloseFd(pipes.ready[0]);
        const ClientStatus status =
            ClientProcessMain(serverEngine, clientEngine, clientDeviceId, serverReady.hostAddr, hixlTimeoutMs,
                              mode, pipes.done[1]);
        ExitChild(status);
    }
    CloseFd(pipes.ready[0]);
    CloseFd(pipes.done[1]);

    const int finalClientStatus = WaitChild(clientPid);
    const int finalServerStatus = WaitChild(serverPid);
    LOG(INFO) << "HIXL RH2D smoke test summary: server_status=" << finalServerStatus << " ("
              << ServerStatusName(finalServerStatus) << "), client_status=" << finalClientStatus << " ("
              << ClientStatusName(finalClientStatus) << ")";

    EXPECT_EQ(finalServerStatus, StatusCode(ServerStatus::SUCCESS))
        << "server process reported failure code: " << ServerStatusName(finalServerStatus);
    EXPECT_EQ(finalClientStatus, StatusCode(ClientStatus::SUCCESS))
        << "client process reported failure code: " << ClientStatusName(finalClientStatus);
}
