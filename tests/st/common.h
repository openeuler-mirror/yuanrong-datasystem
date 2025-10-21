/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Datasystem unit test base class, each testcases files need include this head file.
 */
#ifndef DATASYSTEM_TEST_ST_COMMON_H
#define DATASYSTEM_TEST_ST_COMMON_H

#include <atomic>
#include <map>
#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "cluster/external_cluster.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/log/log.h"
#include "kill_timer.h"

using datasystem::Status;
using datasystem::StatusCode;

#define DS_ASSERT_TRUE(_value, _expect)                           \
    do {                                                          \
        if ((_value) != (_expect)) {                                  \
            LOG(ERROR) << "assert failed, expect: " << _expect << ", actual value: " << _value; \
            ASSERT_TRUE(false);                                   \
        }                                                         \
    } while (false)

#define DS_ASSERT_OK(_s)                                  \
    do {                                                  \
        Status __rc = (_s);                               \
        if (!__rc.IsOk()) {                               \
            ASSERT_TRUE(false) << __rc.ToString() << "."; \
        }                                                 \
    } while (false)

#define DS_EXPECT_OK(_s)                                  \
    do {                                                  \
        Status __rc = (_s);                               \
        if (!__rc.IsOk()) {                               \
            EXPECT_TRUE(false) << __rc.ToString() << "."; \
        }                                                 \
    } while (false)

#define DS_ASSERT_NOT_OK(_s)                              \
    do {                                                  \
        Status __rc = (_s);                               \
        if (__rc.IsOk()) {                                \
            ASSERT_TRUE(false) << __rc.ToString() << "."; \
        }                                                 \
    } while (false)

#define DS_EXPECT_NOT_OK(_s)                              \
    do {                                                  \
        Status __rc = (_s);                               \
        if (__rc.IsOk()) {                                \
            EXPECT_TRUE(false) << __rc.ToString() << "."; \
        }                                                 \
    } while (false)

// To log possible exceptions occurred when initializing a thread pool
#define LOG_IF_EXCEPTION_OCCURS(statement_)                                                    \
    do {                                                                                       \
        try {                                                                                  \
            (statement_);                                                                      \
        } catch (std::system_error & sysErr) {                                                 \
            std::string errMsg = std::string(sysErr.what()) + ", cannot acquire resources";    \
            LOG(ERROR) << Status(K_RUNTIME_ERROR, __LINE__, __FILE__, errMsg).ToString();      \
        } catch (std::bad_alloc & badAlloc) {                                                  \
            std::string errMsg = std::string(badAlloc.what()) + ", cannot allocate resources"; \
            LOG(ERROR) << Status(K_RUNTIME_ERROR, __LINE__, __FILE__, errMsg).ToString();      \
        }                                                                                      \
    } while (false)

namespace datasystem {
namespace st {
class CommonTest : public testing::Test {
public:
    CommonTest();

    ~CommonTest() override = default;

    // every TEST_F macro will call SetUp when start
    void SetUp() override;

    // every TEST_F macro will call TearDown when end
    void TearDown() override{};
protected:
    std::string testCasePath_;
};

class ClusterTest : public CommonTest {
public:
    ClusterTest();

    ~ClusterTest() override = default;

    void SetUp() override;

    void TearDown() override;

    // get new object keys for each cases a new ID is needed
    std::string NewObjectKey();

protected:
    virtual Status Init() = 0;

    // Cluster manager, used to start and shutdown cluster.
    std::unique_ptr<BaseCluster> cluster_;

    // For master client random policy.
    mutable RandomData randomData_;
    mutable HRandomData hRandomData_;

private:
    std::atomic<bool> startShutdown_{ false };
    std::unique_ptr<KillTimer> killTimerPtr_;

    const int DEFAULT_CLIENT_TIMEOUT_SECS = 10;

    const int DEFAULT_TESTCASE_TIMEOUT_SECS = 80;  // in real test, need to change;
};

class ExternalClusterTest : public ClusterTest {
public:
    ExternalClusterTest() = default;

    ~ExternalClusterTest() override = default;

protected:
    Status Init() override;

    void SetDefaultOptions(ExternalClusterOptions &opts) const;

    virtual void SetClusterSetupOptions(ExternalClusterOptions &opts) = 0;

    /**
     * @brief Choose a free port in the range [1025, 20000]. Generally, RPC clients use 30000+
     *        as the port number for communication with the server. Therefore, we can use a
     *        port number smaller than 20000 to minimize conflicts.
     * @param[out] port A free port.
     * @param[in] maxPort Maximum port limit.
     */
    void GetFreePort(int &port, const int maxPort = 65'535) const;

    /**
     * @brief Choose a free port in the range [1025, 20000]. Generally, RPC clients use 30000+
     *        as the port number for communication with the server. Therefore, we can use a
     *        port number smaller than 20000 to minimize conflicts.
     * @param[in] maxPort Maximum port limit.
     * @return A free port.
     */
    int GetFreePort(const int maxPort = 65'535) const;
};

// Obtain the test case name.
void GetCurTestName(std::string &caseName, std::string &name);

// Obtaining Test Results
bool GetTestResult();

std::string GetTestCaseDataDir();

Status ExecuteCmd(const std::string &cmd, std::string &result, int *exitCode = nullptr);

Status ExecuteCmd(const std::string &cmd);

// calculate crc32 with polynomial 0xedb88320
uint32_t GetCrc32(const void *buf, size_t sz, uint32_t checksumbase = 0);
}  // namespace st
}  // namespace datasystem

#endif  // DATASYSTEM_TEST_ST_COMMON_H
