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

#include "cluster/test_port_allocator.h"

#include <algorithm>
#include <chrono>
#include <csignal>
#include <cerrno>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
#include <mutex>
#include <random>
#include <sstream>
#include <thread>
#include <utility>

#include <arpa/inet.h>
#include <dirent.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

namespace datasystem {
namespace st {
namespace {
constexpr int MIN_DEFAULT_PORT = 10'000;
constexpr int MAX_DEFAULT_PORT = 32'767;
constexpr int MIN_PORT = 1'025;
constexpr size_t MAX_REAP_CHECKS_PER_BATCH = 32;

std::string StrError()
{
    return std::strerror(errno);
}

uint64_t NowMs()
{
    auto now = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());
    return static_cast<uint64_t>(now.time_since_epoch().count());
}

bool MkdirIfNeeded(const std::string &path, mode_t mode)
{
    if (mkdir(path.c_str(), mode) == 0) {
        return true;
    }
    return errno == EEXIST;
}

std::string EscapeJson(const std::string &value)
{
    std::string out;
    out.reserve(value.size());
    for (char ch : value) {
        switch (ch) {
            case '\\':
                out += "\\\\";
                break;
            case '"':
                out += "\\\"";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                out += ch;
                break;
        }
    }
    return out;
}

bool ExtractNumber(const std::string &content, const std::string &key, int64_t &value)
{
    const std::string quotedKey = "\"" + key + "\"";
    auto pos = content.find(quotedKey);
    if (pos == std::string::npos) {
        return false;
    }
    pos = content.find(':', pos + quotedKey.size());
    if (pos == std::string::npos) {
        return false;
    }
    ++pos;
    while (pos < content.size() && std::isspace(static_cast<unsigned char>(content[pos]))) {
        ++pos;
    }
    const auto begin = pos;
    if (pos < content.size() && content[pos] == '-') {
        ++pos;
    }
    while (pos < content.size() && std::isdigit(static_cast<unsigned char>(content[pos]))) {
        ++pos;
    }
    if (pos == begin) {
        return false;
    }
    try {
        value = std::stoll(content.substr(begin, pos - begin));
        return true;
    } catch (const std::exception &) {
        return false;
    }
}

std::vector<pid_t> ExtractPidArray(const std::string &content, const std::string &key)
{
    std::vector<pid_t> pids;
    const std::string quotedKey = "\"" + key + "\"";
    auto pos = content.find(quotedKey);
    if (pos == std::string::npos) {
        return pids;
    }
    pos = content.find('[', pos + quotedKey.size());
    if (pos == std::string::npos) {
        return pids;
    }
    auto end = content.find(']', pos);
    if (end == std::string::npos) {
        return pids;
    }
    std::stringstream ss(content.substr(pos + 1, end - pos - 1));
    std::string item;
    while (std::getline(ss, item, ',')) {
        try {
            auto pid = static_cast<pid_t>(std::stoi(item));
            if (pid > 0) {
                pids.emplace_back(pid);
            }
        } catch (const std::exception &) {
            continue;
        }
    }
    return pids;
}

bool IsPidAlive(pid_t pid)
{
    if (pid <= 0) {
        return false;
    }
    return kill(pid, 0) == 0 || errno == EPERM;
}

bool ReadFile(const std::string &path, std::string &content)
{
    std::ifstream in(path);
    if (!in) {
        return false;
    }
    std::stringstream ss;
    ss << in.rdbuf();
    content = ss.str();
    return true;
}

std::string EnvOrEmpty(const char *name)
{
    const char *value = std::getenv(name);
    return value == nullptr ? "" : value;
}
}  // namespace

TestPortLease::TestPortLease(int port, std::string role) : port_(port), role_(std::move(role))
{
}

int TestPortLease::Port() const
{
    return port_;
}

const std::string &TestPortLease::Role() const
{
    return role_;
}

TestPortAllocator &TestPortAllocator::Instance()
{
    static TestPortAllocator allocator;
    return allocator;
}

TestPortAllocator::~TestPortAllocator()
{
    ReleaseAll();
}

void TestPortAllocator::SetOwnerInfo(const std::string &testBinary, const std::string &testName,
                                     const std::string &rootDir)
{
    std::lock_guard<std::mutex> lock(mutex_);
    testBinary_ = testBinary.empty() ? "unknown" : testBinary;
    testName_ = testName.empty() ? "unknown" : testName;
    rootDir_ = rootDir;
}

Status TestPortAllocator::Reserve(const std::string &role, TestPortLease &lease, int maxPort)
{
    std::vector<TestPortLease> leases;
    RETURN_IF_NOT_OK(ReserveBatch({ role }, leases, maxPort));
    CHECK_FAIL_RETURN_STATUS(!leases.empty(), StatusCode::K_RUNTIME_ERROR, "ReserveBatch returned no lease.");
    lease = leases.front();
    return Status::OK();
}

Status TestPortAllocator::ReserveBatch(const std::vector<std::string> &roles, std::vector<TestPortLease> &leases,
                                       int maxPort)
{
    std::lock_guard<std::mutex> lock(mutex_);
    leases.clear();
    RETURN_OK_IF_TRUE(roles.empty());
    RETURN_IF_NOT_OK(EnsureStateDir());

    const std::string globalLockPath = StateDir() + "/global.lock";
    int globalFd = open(globalLockPath.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0600);
    CHECK_FAIL_RETURN_STATUS(globalFd >= 0, StatusCode::K_RUNTIME_ERROR,
                             "Open ST port global lock failed: " + globalLockPath + ", " + StrError());
    if (flock(globalFd, LOCK_EX) != 0) {
        int err = errno;
        close(globalFd);
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Lock ST port global lock failed: " + std::string(strerror(err)));
    }

    ReapStaleLeasesLocked(MAX_REAP_CHECKS_PER_BATCH);

    const auto range = GetPortRange(maxPort);
    std::vector<int> reservedInBatch;
    Status rc = Status::OK();
    for (const auto &role : roles) {
        TestPortLease lease;
        rc = ReserveOneLocked(role, range, reservedInBatch, lease);
        if (rc.IsError()) {
            break;
        }
        reservedInBatch.emplace_back(lease.Port());
        leases.emplace_back(lease);
    }

    if (rc.IsError()) {
        for (const auto &lease : leases) {
            ReleaseLocked(lease.Port());
        }
        leases.clear();
    }

    flock(globalFd, LOCK_UN);
    close(globalFd);
    return rc;
}

void TestPortAllocator::RegisterChildPid(int port, pid_t pid)
{
    std::lock_guard<std::mutex> lock(mutex_);
    RegisterChildPidLocked(port, pid);
}

void TestPortAllocator::Release(int port)
{
    std::lock_guard<std::mutex> lock(mutex_);
    ReleaseLocked(port);
}

void TestPortAllocator::ReleaseAll()
{
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<int> ports;
    for (const auto &item : ownedLeases_) {
        ports.emplace_back(item.first);
    }
    for (auto port : ports) {
        ReleaseLocked(port);
    }
}

Status TestPortAllocator::EnsureStateDir()
{
    RETURN_OK_IF_TRUE(stateDirReady_);
    CHECK_FAIL_RETURN_STATUS(MkdirIfNeeded(StateDir(), 0700), StatusCode::K_RUNTIME_ERROR,
                             "Create ST port state dir failed: " + StateDir() + ", " + StrError());
    CHECK_FAIL_RETURN_STATUS(chmod(StateDir().c_str(), 0700) == 0, StatusCode::K_RUNTIME_ERROR,
                             "Chmod ST port state dir failed: " + StateDir() + ", " + StrError());
    CHECK_FAIL_RETURN_STATUS(MkdirIfNeeded(PortsDir(), 0700), StatusCode::K_RUNTIME_ERROR,
                             "Create ST port locks dir failed: " + PortsDir() + ", " + StrError());
    CHECK_FAIL_RETURN_STATUS(MkdirIfNeeded(LeasesDir(), 0700), StatusCode::K_RUNTIME_ERROR,
                             "Create ST port leases dir failed: " + LeasesDir() + ", " + StrError());
    stateDirReady_ = true;
    return Status::OK();
}

TestPortAllocator::PortRange TestPortAllocator::GetPortRange(int maxPort) const
{
    PortRange range;
    const auto configured = EnvOrEmpty("DS_ST_PORT_RANGE");
    if (!configured.empty()) {
        auto dash = configured.find('-');
        if (dash != std::string::npos) {
            try {
                range.minPort = std::max(MIN_PORT, std::stoi(configured.substr(0, dash)));
                range.maxPort = std::stoi(configured.substr(dash + 1));
            } catch (const std::exception &) {
                range.minPort = MIN_DEFAULT_PORT;
                range.maxPort = MAX_DEFAULT_PORT;
            }
        }
    } else {
        std::string procRange;
        if (ReadFile("/proc/sys/net/ipv4/ip_local_port_range", procRange)) {
            std::stringstream ss(procRange);
            int ephemeralLow = 0;
            int ephemeralHigh = 0;
            if (ss >> ephemeralLow >> ephemeralHigh && ephemeralLow > MIN_DEFAULT_PORT) {
                range.minPort = MIN_DEFAULT_PORT;
                range.maxPort = ephemeralLow - 1;
            }
        }
    }
    range.maxPort = std::min(range.maxPort, maxPort - 1);
    if (range.maxPort < range.minPort) {
        range.minPort = MIN_PORT;
        range.maxPort = std::max(MIN_PORT, maxPort - 1);
    }
    return range;
}

Status TestPortAllocator::ReserveOneLocked(const std::string &role, const PortRange &range,
                                           const std::vector<int> &reservedInBatch, TestPortLease &lease)
{
    CHECK_FAIL_RETURN_STATUS(range.maxPort >= range.minPort, StatusCode::K_RUNTIME_ERROR, "Invalid ST port range.");
    const int span = range.maxPort - range.minPort + 1;
    const auto seed = static_cast<uint32_t>(NowMs() ^ static_cast<uint64_t>(getpid())
                                            ^ std::hash<std::string>()(role + testName_));
    std::mt19937 random(seed);
    std::uniform_int_distribution<int> distribution(0, span - 1);
    int offset = distribution(random);

    for (int attempt = 0; attempt < span; ++attempt) {
        int candidate = range.minPort + ((offset + attempt) % span);
        if (ownedLeases_.find(candidate) != ownedLeases_.end()
            || std::find(reservedInBatch.begin(), reservedInBatch.end(), candidate) != reservedInBatch.end()) {
            continue;
        }

        const std::string lockPath = LockPath(candidate);
        int fd = open(lockPath.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0600);
        if (fd < 0) {
            AppendEventLocked("skip_open_lock_failed", candidate, role, StrError());
            continue;
        }
        if (flock(fd, LOCK_EX | LOCK_NB) != 0) {
            AppendEventLocked("skip_locked", candidate, role, StrError());
            close(fd);
            continue;
        }

        std::string reason;
        if (!ProbePort(candidate, reason)) {
            AppendEventLocked("skip_bind_failed", candidate, role, reason);
            flock(fd, LOCK_UN);
            close(fd);
            continue;
        }

        OwnedLease owned;
        owned.fd = fd;
        owned.port = candidate;
        owned.role = role;
        ownedLeases_.emplace(candidate, owned);
        WriteLeaseFileLocked(ownedLeases_[candidate]);
        AppendEventLocked("reserve", candidate, role, "");
        lease = TestPortLease(candidate, role);
        return Status::OK();
    }
    RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                  "No free ST port found in range " + std::to_string(range.minPort) + "-"
                      + std::to_string(range.maxPort));
}

bool TestPortAllocator::ProbePort(int port, std::string &reason) const
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        reason = "socket failed: " + StrError();
        return false;
    }
    sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
        reason = "bind 127.0.0.1 failed: " + StrError();
        close(fd);
        return false;
    }
    close(fd);
    return true;
}

void TestPortAllocator::ReleaseLocked(int port)
{
    auto it = ownedLeases_.find(port);
    if (it == ownedLeases_.end()) {
        return;
    }
    AppendEventLocked("release", port, it->second.role, "");
    unlink(LeasePath(port).c_str());
    if (it->second.fd >= 0) {
        flock(it->second.fd, LOCK_UN);
        close(it->second.fd);
    }
    ownedLeases_.erase(it);
}

void TestPortAllocator::RegisterChildPidLocked(int port, pid_t pid)
{
    if (pid <= 0) {
        return;
    }
    auto it = ownedLeases_.find(port);
    if (it == ownedLeases_.end()) {
        AppendEventLocked("register_child_without_lease", port, "", std::to_string(pid));
        return;
    }
    if (std::find(it->second.childPids.begin(), it->second.childPids.end(), pid) == it->second.childPids.end()) {
        it->second.childPids.emplace_back(pid);
    }
    WriteLeaseFileLocked(it->second);
    AppendEventLocked("register_child", port, it->second.role, std::to_string(pid));
}

void TestPortAllocator::ReapStaleLeasesLocked(size_t maxChecks)
{
    DIR *dir = opendir(LeasesDir().c_str());
    if (dir == nullptr) {
        return;
    }
    size_t checked = 0;
    while (checked < maxChecks) {
        dirent *entry = readdir(dir);
        if (entry == nullptr) {
            break;
        }
        const std::string fileName = entry->d_name;
        if (fileName == "." || fileName == ".." || fileName.find(".json") == std::string::npos) {
            continue;
        }
        ++checked;
        HandleStaleLeaseFileLocked(fileName);
    }
    closedir(dir);
}

void TestPortAllocator::HandleStaleLeaseFileLocked(const std::string &fileName)
{
    const std::string path = LeasesDir() + "/" + fileName;
    std::string content;
    if (!ReadFile(path, content)) {
        return;
    }
    int64_t portValue = -1;
    int64_t ownerPidValue = -1;
    if (!ExtractNumber(content, "port", portValue) || !ExtractNumber(content, "owner_pid", ownerPidValue)) {
        const std::string corruptPath = path + ".corrupt." + std::to_string(NowMs());
        rename(path.c_str(), corruptPath.c_str());
        AppendEventLocked("corrupt_lease", static_cast<int>(portValue), "", corruptPath);
        return;
    }
    const int port = static_cast<int>(portValue);
    if (ownedLeases_.find(port) != ownedLeases_.end() || IsPidAlive(static_cast<pid_t>(ownerPidValue))) {
        return;
    }

    int fd = open(LockPath(port).c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0600);
    if (fd < 0) {
        AppendEventLocked("stale_open_lock_failed", port, "", StrError());
        return;
    }
    if (flock(fd, LOCK_EX | LOCK_NB) != 0) {
        close(fd);
        return;
    }

    bool killedChild = false;
    for (auto pid : ExtractPidArray(content, "child_pids")) {
        if (IsPidAlive(pid)) {
            kill(pid, SIGTERM);
            killedChild = true;
        }
    }
    if (killedChild) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    std::string reason;
    if (ProbePort(port, reason)) {
        unlink(path.c_str());
        AppendEventLocked("stale_reaped", port, "", "owner_pid=" + std::to_string(ownerPidValue));
    } else {
        AppendEventLocked("quarantine", port, "", reason);
    }
    flock(fd, LOCK_UN);
    close(fd);
}

void TestPortAllocator::WriteLeaseFileLocked(const OwnedLease &lease) const
{
    std::ofstream out(LeasePath(lease.port), std::ios::trunc);
    if (!out) {
        return;
    }
    out << "{\n";
    out << "  \"port\": " << lease.port << ",\n";
    out << "  \"host\": \"127.0.0.1\",\n";
    out << "  \"role\": \"" << EscapeJson(lease.role) << "\",\n";
    out << "  \"owner_pid\": " << getpid() << ",\n";
    out << "  \"owner_pgid\": " << getpgrp() << ",\n";
    out << "  \"test_binary\": \"" << EscapeJson(testBinary_) << "\",\n";
    out << "  \"test_name\": \"" << EscapeJson(testName_) << "\",\n";
    out << "  \"root_dir\": \"" << EscapeJson(rootDir_) << "\",\n";
    out << "  \"child_pids\": [";
    for (size_t i = 0; i < lease.childPids.size(); ++i) {
        if (i > 0) {
            out << ", ";
        }
        out << lease.childPids[i];
    }
    out << "],\n";
    out << "  \"created_at_unix_ms\": " << NowMs() << ",\n";
    out << "  \"last_seen_unix_ms\": " << NowMs() << ",\n";
    out << "  \"allocator_version\": 1\n";
    out << "}\n";
}

void TestPortAllocator::AppendEventLocked(const std::string &event, int port, const std::string &role,
                                          const std::string &reason) const
{
    std::ofstream out(EventPath(), std::ios::app);
    if (!out) {
        return;
    }
    out << "{\"ts_ms\":" << NowMs() << ",\"event\":\"" << EscapeJson(event) << "\",\"port\":" << port
        << ",\"role\":\"" << EscapeJson(role) << "\",\"owner_pid\":" << getpid() << ",\"test\":\""
        << EscapeJson(testName_) << "\",\"reason\":\"" << EscapeJson(reason) << "\"}\n";
}

std::string TestPortAllocator::StateDir() const
{
    std::string path = "/tmp/datasystem-st-ports-" + std::to_string(getuid());
    const auto ns = EnvOrEmpty("DS_ST_PORT_NAMESPACE");
    if (!ns.empty()) {
        path += "-" + ns;
    }
    return path;
}

std::string TestPortAllocator::PortsDir() const
{
    return StateDir() + "/ports";
}

std::string TestPortAllocator::LeasesDir() const
{
    return StateDir() + "/leases";
}

std::string TestPortAllocator::LockPath(int port) const
{
    return PortsDir() + "/" + std::to_string(port) + ".lock";
}

std::string TestPortAllocator::LeasePath(int port) const
{
    return LeasesDir() + "/" + std::to_string(port) + ".json";
}

std::string TestPortAllocator::EventPath() const
{
    return StateDir() + "/events.log";
}
}  // namespace st
}  // namespace datasystem
