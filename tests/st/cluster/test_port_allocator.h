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

#ifndef DATASYSTEM_TEST_ST_CLUSTER_TEST_PORT_ALLOCATOR_H
#define DATASYSTEM_TEST_ST_CLUSTER_TEST_PORT_ALLOCATOR_H

#include <map>
#include <mutex>
#include <string>
#include <vector>

#include <sys/types.h>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace st {
class TestPortLease {
public:
    TestPortLease() = default;

    TestPortLease(int port, std::string role);

    int Port() const;

    const std::string &Role() const;

private:
    int port_ = -1;
    std::string role_;
};

class TestPortAllocator {
public:
    static TestPortAllocator &Instance();

    void SetOwnerInfo(const std::string &testBinary, const std::string &testName, const std::string &rootDir);

    Status Reserve(const std::string &role, TestPortLease &lease, int maxPort = 65'535);

    Status ReserveBatch(const std::vector<std::string> &roles, std::vector<TestPortLease> &leases,
                        int maxPort = 65'535);

    void RegisterChildPid(int port, pid_t pid);

    void Release(int port);

    void ReleaseAll();

private:
    struct OwnedLease {
        int fd = -1;
        int port = -1;
        std::string role;
        std::vector<pid_t> childPids;
    };

    struct PortRange {
        int minPort = 10'000;
        int maxPort = 32'767;
    };

    TestPortAllocator() = default;

    ~TestPortAllocator();

    Status EnsureStateDir();

    PortRange GetPortRange(int maxPort) const;

    Status ReserveOneLocked(const std::string &role, const PortRange &range, const std::vector<int> &reservedInBatch,
                            TestPortLease &lease);

    bool ProbePort(int port, std::string &reason) const;

    void ReleaseLocked(int port);

    void RegisterChildPidLocked(int port, pid_t pid);

    void ReapStaleLeasesLocked(size_t maxChecks);

    void HandleStaleLeaseFileLocked(const std::string &fileName);

    void WriteLeaseFileLocked(const OwnedLease &lease) const;

    void AppendEventLocked(const std::string &event, int port, const std::string &role, const std::string &reason) const;

    std::string StateDir() const;

    std::string PortsDir() const;

    std::string LeasesDir() const;

    std::string LockPath(int port) const;

    std::string LeasePath(int port) const;

    std::string EventPath() const;

    std::map<int, OwnedLease> ownedLeases_;
    std::mutex mutex_;
    std::string testBinary_ = "unknown";
    std::string testName_ = "unknown";
    std::string rootDir_;
    bool stateDirReady_ = false;
};
}  // namespace st
}  // namespace datasystem

#endif  // DATASYSTEM_TEST_ST_CLUSTER_TEST_PORT_ALLOCATOR_H
