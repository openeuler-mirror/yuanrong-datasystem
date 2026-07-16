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

#include "datasystem/common/urma_mock/transport/uds_endpoint_service.h"

#include <pthread.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <charconv>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/urma_mock/abi/urma_abi_compat.h"
#include "datasystem/common/urma_mock/inject/fault_inject.h"
#include "datasystem/common/urma_mock/mock_registry.h"
#include "datasystem/common/urma_mock/objects/mock_seg.h"
#include "datasystem/common/urma_mock/segment/segment_endpoint_registry.h"
#include "datasystem/common/urma_mock/segment/segment_identity.h"
#include "datasystem/common/urma_mock/transport/uds_transport.h"
#include "datasystem/common/urma_mock/urma_mock_backend.h"

namespace datasystem {
namespace urma_mock {

namespace {
constexpr size_t K_HELLO_TOKEN_SIZE = sizeof(uint64_t);
constexpr size_t K_HELLO_REQUEST_PAYLOAD_SIZE = sizeof(uint64_t) * 2;
constexpr size_t K_HELLO_ACK_MIN_PAYLOAD_SIZE = sizeof(uint64_t) * 2;
constexpr size_t K_HELLO_ACK_PAYLOAD_SIZE = sizeof(uint64_t) * 3;
constexpr size_t K_HELLO_ACK_LEN_OFFSET = sizeof(uint64_t);
constexpr size_t K_HELLO_ACK_MEMFD_OFFSET_OFFSET = sizeof(uint64_t) * 2;
constexpr int K_ACCEPT_PEER_RECV_TIMEOUT_MS = 1000;
constexpr int K_DECIMAL_BASE = 10;
constexpr uint32_t K_HANDSHAKE_TIMEOUT_MODE = 1;
constexpr uint32_t K_BAD_HANDSHAKE_PAYLOAD_MODE = 2;
constexpr mode_t K_UDS_DIR_MODE = 0700;
constexpr size_t K_PID_NS_LINK_BUFFER_SIZE = 128;

struct UdsListenerState {
    ~UdsListenerState()
    {
        for (const auto &aliasPath : aliasPaths) {
            ::unlink(aliasPath.c_str());
        }
        aliasPaths.clear();
        if (listener) {
            listener->Close();
        }
        if (acceptTh.joinable()) {
            acceptTh.join();
        }
        listener.reset();
        bound = false;
    }

    std::mutex mu;
    bool bound = false;
    pid_t ownerPid = getpid();
    std::unique_ptr<UdsListener> listener;
    std::thread acceptTh;
    std::vector<std::string> aliasPaths;
};

volatile sig_atomic_t &ChildInheritedListenFd()
{
    static volatile sig_atomic_t fd = -1;
    return fd;
}

volatile sig_atomic_t &ForkedChildNeedsNewState()
{
    static volatile sig_atomic_t flag = 0;
    return flag;
}

UdsListenerState *&ListenerStatePtr()
{
    // Process-lifetime singleton. Forked children intentionally switch this pointer to a fresh listener state.
    static UdsListenerState *s = new UdsListenerState();
    return s;
}

UdsListenerState &ListenerState()
{
    auto &state = ListenerStatePtr();
    if (ForkedChildNeedsNewState() != 0 || state->ownerPid != getpid()) {
        // The forked child replaces the inherited process-lifetime listener state.
        state = new UdsListenerState();
        ImportEndpointRegistry::Instance().Clear();
        ForkedChildNeedsNewState() = 0;
    }
    return *state;
}

std::string GetEnvOrEmpty(const char *name)
{
    const char *value = std::getenv(name);
    return value == nullptr ? std::string{} : std::string(value);
}

std::string ReadPidNamespaceToken()
{
    char nsLink[K_PID_NS_LINK_BUFFER_SIZE] = { 0 };
    ssize_t len = ::readlink("/proc/self/ns/pid", nsLink, sizeof(nsLink) - 1);
    if (len <= 0) {
        return {};
    }
    nsLink[len] = '\0';
    std::string token;
    token.reserve(static_cast<size_t>(len));
    for (ssize_t i = 0; i < len; ++i) {
        if (nsLink[i] >= '0' && nsLink[i] <= '9') {
            token.push_back(nsLink[i]);
        }
    }
    return token;
}

bool IsInheritedAutoInstance(const std::string &instance)
{
    const std::string prefix = "instance_";
    if (instance.rfind(prefix, 0) != 0) {
        return false;
    }
    const auto pidStart = prefix.size();
    auto pidEnd = instance.find('_', pidStart);
    auto pidText = instance.substr(pidStart, pidEnd == std::string::npos ? std::string::npos : pidEnd - pidStart);
    if (pidText.empty()) {
        return false;
    }
    long long pid = 0;
    auto result = std::from_chars(pidText.data(), pidText.data() + pidText.size(), pid, K_DECIMAL_BASE);
    return result.ec == std::errc() && result.ptr == pidText.data() + pidText.size() && pid > 0
           && pid != static_cast<long long>(::getpid());
}

void EnsureProcessInstanceEnv()
{
    auto existing = GetEnvOrEmpty("URMA_MOCK_UDS_INSTANCE");
    if (!existing.empty() && !IsInheritedAutoInstance(existing)) {
        return;
    }
    auto instance = "instance_" + std::to_string(static_cast<long long>(::getpid()));
    auto nsToken = ReadPidNamespaceToken();
    if (!nsToken.empty()) {
        instance += "_" + nsToken;
    }
    static_cast<void>(::setenv("URMA_MOCK_UDS_INSTANCE", instance.c_str(), 1));
}

std::string CurrentInstanceId()
{
    return GetEnvOrEmpty("URMA_MOCK_UDS_INSTANCE");
}

bool MkdirAll(const std::string &dir)
{
    if (dir.empty()) {
        return true;
    }
    size_t pos = dir[0] == '/' ? 1 : 0;
    while (pos <= dir.size()) {
        auto next = dir.find('/', pos);
        auto current = dir.substr(0, next);
        if (!current.empty() && ::mkdir(current.c_str(), K_UDS_DIR_MODE) != 0 && errno != EEXIST) {
            LOG(WARNING) << "[MockUrma-UDS] mkdir " << current << " failed: " << StrErr(errno);
            return false;
        }
        if (next == std::string::npos) {
            break;
        }
        pos = next + 1;
    }
    return true;
}

void BindAliasPath(const std::string &targetPath, const std::string &aliasInstance, UdsListenerState &state)
{
    if (aliasInstance.empty()) {
        return;
    }
    auto aliasPath = ResolveUdsPathForInstance(aliasInstance);
    if (aliasPath == targetPath) {
        return;
    }
    auto slash = aliasPath.find_last_of('/');
    if (slash == std::string::npos || !MkdirAll(aliasPath.substr(0, slash))) {
        return;
    }
    ::unlink(aliasPath.c_str());
    if (::symlink(targetPath.c_str(), aliasPath.c_str()) != 0) {
        LOG(WARNING) << "[MockUrma-UDS] symlink alias " << aliasPath << " -> " << targetPath
                     << " failed: " << StrErr(errno);
        return;
    }
    state.aliasPaths.emplace_back(std::move(aliasPath));
}

struct HelloRequest {
    uint64_t token = 0;
    uint64_t requestedVa = 0;
};

std::shared_ptr<MockSeg> FindSegForUdsRequest(const HelloRequest &request)
{
    MockTablesLock lock;
    auto &t = lock.GetTables();
    std::string tokStr = std::to_string(request.token);
    std::shared_ptr<MockSeg> remoteAlias;
    std::shared_ptr<MockSeg> tokenFallback;
    for (auto &[raw, mockSeg] : t.tseg) {
        (void)raw;
        if (mockSeg == nullptr || (mockSeg->GetMemfdFd() < 0 && mockSeg->GetShmFd() < 0)) {
            continue;
        }
        const uint64_t segBase =
            mockSeg->GetRemoteVa() != 0 ? mockSeg->GetRemoteVa() : reinterpret_cast<uint64_t>(mockSeg->GetPtr());
        const bool vaMatched = request.requestedVa != 0 && request.requestedVa >= segBase
                               && request.requestedVa < segBase + mockSeg->GetSize();
        const bool tokenMatched = request.token != 0 && !mockSeg->GetKey().empty() && mockSeg->GetKey() == tokStr;
        if (vaMatched) {
            if (!mockSeg->IsRemote()) {
                return mockSeg;
            }
            if (remoteAlias == nullptr) {
                remoteAlias = mockSeg;
            }
            continue;
        }
        if (request.requestedVa == 0 && tokenMatched && !mockSeg->IsRemote() && tokenFallback == nullptr) {
            tokenFallback = mockSeg;
        }
    }
    return remoteAlias == nullptr ? tokenFallback : remoteAlias;
}

bool RecvHelloRequest(UdsConnection &conn, HelloRequest &request)
{
    UdsMsgType inType = UdsMsgType::HELLO;
    std::vector<uint8_t> inPayload;
    std::vector<int> inFds;
    int err = 0;
    if (!conn.Recv(&inType, nullptr, &inPayload, &inFds, nullptr, &err)) {
        LOG(WARNING) << "[MockUrma-UDS] accept Recv failed errno=" << err;
        return false;
    }
    if (inType != UdsMsgType::HELLO || inPayload.size() < K_HELLO_TOKEN_SIZE) {
        LOG(WARNING) << "[MockUrma-UDS] accept got type=" << static_cast<int>(inType)
                     << " payload=" << inPayload.size();
        return false;
    }
    std::memcpy(&request.token, inPayload.data(), sizeof(request.token));
    if (inPayload.size() >= K_HELLO_REQUEST_PAYLOAD_SIZE) {
        std::memcpy(&request.requestedVa, inPayload.data() + K_HELLO_TOKEN_SIZE, sizeof(request.requestedVa));
    }
    return true;
}

bool SendHelloAck(UdsConnection &conn, uint64_t token, const std::shared_ptr<MockSeg> &segShptr)
{
    int backingFd = -1;
    if (segShptr != nullptr) {
        backingFd = segShptr->GetMemfdFd();
        if (backingFd < 0) {
            backingFd = segShptr->GetShmFd();
        }
    }
    if (segShptr == nullptr || backingFd < 0) {
        LOG(WARNING) << "[MockUrma-UDS] HELLO token=" << token << " no seg or no backing fd";
        return false;
    }

    uint64_t va =
        segShptr->GetRemoteVa() != 0 ? segShptr->GetRemoteVa() : reinterpret_cast<uint64_t>(segShptr->GetPtr());
    uint64_t len = segShptr->GetSize();
    uint64_t memfdOffset = segShptr->GetMemfdOffset();
    uint8_t ack[K_HELLO_ACK_PAYLOAD_SIZE];
    std::memcpy(ack, &va, sizeof(va));
    std::memcpy(ack + K_HELLO_ACK_LEN_OFFSET, &len, sizeof(len));
    std::memcpy(ack + K_HELLO_ACK_MEMFD_OFFSET_OFFSET, &memfdOffset, sizeof(memfdOffset));

    int dupFd = ::dup(backingFd);
    if (dupFd < 0) {
        LOG(WARNING) << "[MockUrma-UDS] dup backing fd failed errno=" << errno;
        return false;
    }
    Raii closeDupFd([dupFd]() { ::close(dupFd); });
    if (!conn.Send(UdsMsgType::HELLO_ACK, 0, ack, sizeof(ack), { dupFd })) {
        LOG(WARNING) << "[MockUrma-UDS] HELLO_ACK Send failed";
        return false;
    }
    return true;
}

void HandleUdsPeer(int peerFd)
{
    UdsConnection conn;
    conn.AdoptFd(peerFd);
    static_cast<void>(conn.SetRecvTimeout(K_ACCEPT_PEER_RECV_TIMEOUT_MS));
    HelloRequest request;
    if (!RecvHelloRequest(conn, request)) {
        return;
    }
    SendHelloAck(conn, request.token, FindSegForUdsRequest(request));
}

// Accept-thread loop: parse HELLO { token(8B), optional requested_va(8B) } -> look up tseg
// -> reply HELLO_ACK { va(8B) + len(8B) } + cmsg SCM_RIGHTS [backing fd].
void RunUdsAcceptLoop(UdsListener *listener)
{
    bool keepRunning = true;
    while (keepRunning) {
        try {
            int peerFd = listener->Accept();
            if (peerFd < 0) {
                // Listener closed -> exit thread.
                keepRunning = false;
                continue;
            }
            HandleUdsPeer(peerFd);
        } catch (const std::exception &e) {
            LOG(WARNING) << "[MockUrma-UDS] accept loop caught exception: " << e.what();
        } catch (...) {
            LOG(WARNING) << "[MockUrma-UDS] accept loop caught unknown exception";
        }
    }
}

bool StartUdsListenerThread(UdsListenerState &st, UdsListener *raw)
{
    ChildInheritedListenFd() = raw->GetListenFd();
    try {
        st.acceptTh = std::thread(RunUdsAcceptLoop, raw);
        return true;
    } catch (const std::exception &e) {
        ChildInheritedListenFd() = -1;
        st.listener.reset();
        LOG(WARNING) << "[MockUrma-UDS] start accept thread failed: " << e.what();
    } catch (...) {
        ChildInheritedListenFd() = -1;
        st.listener.reset();
        LOG(WARNING) << "[MockUrma-UDS] start accept thread failed";
    }
    return false;
}

void RegisterForkHandlerOnce()
{
    static std::once_flag atforkFlag;
    std::call_once(atforkFlag, []() {
        pthread_atfork(nullptr, nullptr, []() {
            int fd = ChildInheritedListenFd();
            if (fd >= 0) {
                ::close(fd);
                ChildInheritedListenFd() = -1;
            }
            ForkedChildNeedsNewState() = 1;
        });
    });
}

}  // namespace

const UdsEndpointService &UdsEndpointService::Instance()
{
    static UdsEndpointService *s = new UdsEndpointService();
    return *s;
}

void UdsEndpointService::EnsureListener() const
{
    try {
        EnsureProcessInstanceEnv();
        auto &st = ListenerState();
        std::lock_guard<std::mutex> lk(st.mu);
        if (st.bound) {
            return;
        }
        auto path = ResolveUdsPath();
        if (path.empty()) {
            LOG(INFO) << "[MockUrma-UDS] ResolveUdsPath empty (env not set); skipping listener";
            return;
        }
        auto listener = std::make_unique<UdsListener>();
        if (!listener->Bind(path)) {
            LOG(WARNING) << "[MockUrma-UDS] Bind failed for " << path;
            return;
        }
        UdsListener *raw = listener.get();
        st.listener = std::move(listener);
        // Stash the live fd for the atfork child handler. The handler runs in signal-handler context and cannot reach
        // into UdsListener without invoking a non-async-signal-safe dtor.
        if (!StartUdsListenerThread(st, raw)) {
            return;
        }
        st.bound = true;
        const char *alias = std::getenv("URMA_MOCK_UDS_ALIAS");
        BindAliasPath(path, alias == nullptr ? std::string{} : std::string(alias), st);

        // fork-safety. The child handler must not touch C++ mutex,
        // unique_ptr, or std::thread state inherited from the parent. It only
        // closes the inherited fd and marks a sidecar flag. The child swaps in a
        // fresh ListenerState on the next normal EnsureListener() call.
        RegisterForkHandlerOnce();
        LOG(INFO) << "[MockUrma-UDS] listener bound at " << path;
    } catch (const std::exception &e) {
        LOG(WARNING) << "[MockUrma-UDS] ensure listener failed: " << e.what();
    } catch (...) {
        LOG(WARNING) << "[MockUrma-UDS] ensure listener failed";
    }
}

void UdsEndpointService::ShutdownListener() const
{
    auto &st = ListenerState();
    std::lock_guard<std::mutex> lk(st.mu);
    if (!st.bound) {
        return;
    }
    // Closing the listener fd causes Accept to return -1 and the thread
    // to exit its loop on its own. Keep the listener object alive until
    // the accept thread has joined because the thread owns only a raw
    // pointer to it.
    st.listener->Close();
    ChildInheritedListenFd() = -1;
    if (st.acceptTh.joinable()) {
        st.acceptTh.join();
    }
    st.listener.reset();  // dtor unlinks path
    for (const auto &aliasPath : st.aliasPaths) {
        ::unlink(aliasPath.c_str());
    }
    st.aliasPaths.clear();
    st.bound = false;
    SegmentEndpointRegistry::Instance().Clear(CurrentInstanceId());
    LOG(INFO) << "[MockUrma-UDS] listener shut down";
}

void UdsEndpointService::RegisterImportEndpoint(uint64_t token, const ImportEndpoint &ep) const
{
    ImportEndpointRegistry::Instance().Register(token, ep);
}

ImportEndpoint UdsEndpointService::LookupImportEndpoint(uint64_t token, uint64_t remoteVa) const
{
    return ImportEndpointRegistry::Instance().Lookup(token, remoteVa);
}

void UdsEndpointService::RegisterSegmentEndpoint(const urma_seg_t &seg, uint64_t token) const
{
    if (token == 0 || seg.ubva.va == 0 || seg.len == 0) {
        return;
    }
    EnsureProcessInstanceEnv();
    auto instance = CurrentInstanceId();
    if (instance.empty()) {
        return;
    }
    ImportEndpoint ep;
    ep.instanceId = instance;
    ep.va = seg.ubva.va;
    ep.len = seg.len;
    static_cast<void>(SegmentEndpointRegistry::Instance().Register(BuildSegmentEndpointKey(seg, token), ep));
}

void UdsEndpointService::UnregisterSegmentEndpoint(const urma_seg_t &seg, uint64_t token) const
{
    if (token != 0 && seg.ubva.va != 0) {
        SegmentEndpointRegistry::Instance().Unregister(BuildSegmentEndpointKey(seg, token));
    }
}

ImportEndpoint UdsEndpointService::LookupSegmentEndpoint(const urma_seg_t &seg, uint64_t token) const
{
    if (token == 0 || seg.ubva.va == 0) {
        return {};
    }
    return SegmentEndpointRegistry::Instance().Lookup(BuildSegmentEndpointKey(seg, token),
                                                      BuildSegmentEndpointPrefix(seg, token), seg.ubva.va);
}

namespace {

void CloseFdsFrom(std::vector<int> &fds, size_t start)
{
    for (size_t i = start; i < fds.size(); ++i) {
        if (fds[i] >= 0) {
            ::close(fds[i]);
        }
    }
}

void ResetUdsHelloAckOutput(uint64_t *outVa, uint64_t *outLen, uint64_t *outOffset)
{
    if (outVa != nullptr) {
        *outVa = 0;
    }
    if (outLen != nullptr) {
        *outLen = 0;
    }
    if (outOffset != nullptr) {
        *outOffset = 0;
    }
}

bool SendUdsHello(UdsConnection &conn, uint64_t token, uint64_t requestedVa)
{
    std::array<uint8_t, K_HELLO_REQUEST_PAYLOAD_SIZE> tokenPayload{};
    std::memcpy(tokenPayload.data(), &token, sizeof(token));
    std::memcpy(tokenPayload.data() + K_HELLO_TOKEN_SIZE, &requestedVa, sizeof(requestedVa));
    if (!conn.Send(UdsMsgType::HELLO, 0, tokenPayload.data(), tokenPayload.size(), {})) {
        LOG(WARNING) << "[MockUrma-UDS] HELLO Send failed";
        return false;
    }
    return true;
}

bool RecvUdsHelloAck(UdsConnection &conn, std::vector<uint8_t> *payload, std::vector<int> *fds)
{
    UdsMsgType inType = UdsMsgType::HELLO_ACK;
    int err = 0;
    if (!conn.Recv(&inType, nullptr, payload, fds, nullptr, &err)) {
        LOG(WARNING) << "[MockUrma-UDS] HELLO_ACK Recv failed errno=" << err;
        return false;
    }
    if (inType == UdsMsgType::HELLO_ACK && payload->size() >= K_HELLO_ACK_MIN_PAYLOAD_SIZE && !fds->empty()) {
        return true;
    }
    LOG(WARNING) << "[MockUrma-UDS] HELLO_ACK bad frame type=" << static_cast<int>(inType)
                 << " payload=" << payload->size() << " fds=" << fds->size();
    CloseFdsFrom(*fds, 0);
    return false;
}

void FillUdsHelloAckOutput(const std::vector<uint8_t> &payload, uint64_t *outVa, uint64_t *outLen, uint64_t *outOffset)
{
    uint64_t va = 0;
    uint64_t len = 0;
    uint64_t offset = 0;
    std::memcpy(&va, payload.data(), sizeof(va));
    std::memcpy(&len, payload.data() + K_HELLO_ACK_LEN_OFFSET, sizeof(len));
    if (payload.size() >= K_HELLO_ACK_PAYLOAD_SIZE) {
        std::memcpy(&offset, payload.data() + K_HELLO_ACK_MEMFD_OFFSET_OFFSET, sizeof(offset));
    }
    if (outVa != nullptr) {
        *outVa = va;
    }
    if (outLen != nullptr) {
        *outLen = len;
    }
    if (outOffset != nullptr) {
        *outOffset = offset;
    }
}

int TransferFirstFd(std::vector<int> &fds)
{
    int transferredFd = fds[0];
    CloseFdsFrom(fds, 1);
    fds.clear();
    return transferredFd;
}

// Performs HELLO/HELLO_ACK against the already-derived UDS path.
int DoUdsHelloAck(const std::string &path, uint64_t token, uint64_t *outVa, uint64_t *outLen, uint64_t requestedVa,
                  uint64_t *outOffset)
{
    ResetUdsHelloAckOutput(outVa, outLen, outOffset);
    UdsConnection conn;
    if (!conn.Connect(path)) {
        LOG(INFO) << "[MockUrma-UDS] connect " << path << " failed errno=" << errno;
        return -1;
    }
    if (!SendUdsHello(conn, token, requestedVa)) {
        return -1;
    }

    auto delayMs = GetMockInjectHandshakeDelayMs();
    if (delayMs != 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
    }
    auto timeoutMode = GetMockInjectHandshakeTimeoutMode();
    if (timeoutMode == K_HANDSHAKE_TIMEOUT_MODE) {
        LOG(WARNING) << "[MockUrma-UDS] injected HELLO_ACK timeout";
        return -1;
    }

    std::vector<uint8_t> payload;
    std::vector<int> fds;
    bool recvOk = RecvUdsHelloAck(conn, &payload, &fds);
    if (timeoutMode == K_BAD_HANDSHAKE_PAYLOAD_MODE) {
        CloseFdsFrom(fds, 0);
        LOG(WARNING) << "[MockUrma-UDS] injected bad HELLO_ACK payload";
        return -1;
    }
    if (!recvOk) {
        return -1;
    }
    FillUdsHelloAckOutput(payload, outVa, outLen, outOffset);
    return TransferFirstFd(fds);
}

}  // namespace

int UdsEndpointService::ImportSegViaUds(const std::string &instanceId, uint64_t token, uint64_t *outVa,
                                        uint64_t *outLen, uint64_t requestedVa, uint64_t *outOffset) const
{
    std::string path = ResolveUdsPathForInstance(instanceId);
    return DoUdsHelloAck(path, token, outVa, outLen, requestedVa, outOffset);
}

int UdsEndpointService::ImportSegViaUdsForHost(const std::string &host, int port, uint64_t token, uint64_t *outVa,
                                               uint64_t *outLen, uint64_t requestedVa, uint64_t *outOffset) const
{
    // Receiver-side compatibility path for legacy mock tests that still register
    // a peer endpoint by host:port.
    std::string path = ResolveUdsPathForHost(host, port);
    return DoUdsHelloAck(path, token, outVa, outLen, requestedVa, outOffset);
}

}  // namespace urma_mock
}  // namespace datasystem
