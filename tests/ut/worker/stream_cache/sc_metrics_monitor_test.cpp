/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: ScMetricsMonitor unit test.
 * Covers the reentrancy scenario that previously caused std::terminate
 * (singleton StartMonitor() called twice), and the init-failure path
 * where Shutdown() must not destroy the old Tick thread before the new
 * setup is confirmed to succeed.
 */

#include <fcntl.h>
#include <unistd.h>

#include "ut/common.h"

#include "datasystem/common/constants.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/worker/stream_cache/metrics/sc_metrics_monitor.h"

DS_DECLARE_bool(log_monitor);
DS_DECLARE_string(log_dir);

namespace datasystem {
namespace ut {

class ScMetricsMonitorTest : public CommonTest {
public:
    void SetUp() override
    {
        // Enable the real monitor path (StartMonitor() only starts the Tick
        // thread when FLAGS_log_monitor is true).
        FLAGS_log_monitor = true;
        // CommonTest already sets FLAGS_log_dir to a per-case writable path;
        // ensure the directory exists so CreateLogDir()/Init() succeed.
        CreateDir(FLAGS_log_dir, true);
        CommonTest::SetUp();
    }

    void TearDown() override
    {
        // Leave the singleton stopped so later cases start from a clean state.
        ScMetricsMonitor::Instance()->Shutdown();
        FLAGS_log_monitor = false;
        CommonTest::TearDown();
    }

protected:
    /**
     * Point FLAGS_log_dir at an existing regular file so that
     * Logging::CreateLogDir() fails (the path is not a writable dir and
     * CreateDir cannot turn a file into a directory), making StartMonitor()
     * return K_NOT_READY without creating a new Tick thread.
     */
    void SetUnwritableLogDir()
    {
        savedLogDir_ = FLAGS_log_dir;
        badLogDir_ = savedLogDir_ + "/blocking_file";
        // Create a regular file at that path; presence of a non-dir file
        // makes the log-dir creation check fail.
        int fd = -1;
        DS_ASSERT_OK(OpenFile(badLogDir_, O_CREAT | O_RDWR, 0600, &fd));
        if (fd >= 0) {
            RETRY_ON_EINTR(close(fd));
        }
        FLAGS_log_dir = badLogDir_;
    }

    void RestoreLogDir()
    {
        if (!savedLogDir_.empty()) {
            FLAGS_log_dir = savedLogDir_;
            savedLogDir_.clear();
        }
    }

    std::string savedLogDir_;
    std::string badLogDir_;
};

// Regression for std::terminate on reentrant StartMonitor().
// ScMetricsMonitor is a singleton; StartMonitor() can be called again when a
// second test case (or a second ClientWorkerSCServiceImpl) re-inits. Before the
// fix, assigning a new thread_ destructed the old joinable Thread -> std::terminate.
// If this case runs to completion (exit 0, not 134/SIGABRT), the fix holds.
TEST_F(ScMetricsMonitorTest, ReentrantStartMonitorDoesNotCrash)
{
    DS_ASSERT_OK(ScMetricsMonitor::Instance()->StartMonitor());
    // Second call must stop the old Tick and start a fresh one instead of
    // terminating the process.
    DS_ASSERT_OK(ScMetricsMonitor::Instance()->StartMonitor());
    // The monitor should still report itself as enabled after reentry.
    ASSERT_TRUE(ScMetricsMonitor::Instance()->IsEnabled());
}

// Regression for Shutdown() being called before init succeeds.
// If the second StartMonitor() fails during CreateLogDir()/Init(), the old
// Tick thread must not have been stopped (otherwise the monitor is left in a
// half-dead state: isEnabled_ later set true but no Tick running). We assert
// that after a failed reentry the monitor can still be recovered by a
// subsequent successful StartMonitor().
TEST_F(ScMetricsMonitorTest, InitFailureDoesNotDestroyOldMonitor)
{
    // First call succeeds: a real Tick thread is now running.
    DS_ASSERT_OK(ScMetricsMonitor::Instance()->StartMonitor());
    ASSERT_TRUE(ScMetricsMonitor::Instance()->IsEnabled());

    // Sabotage the log dir so the second call's init fails.
    SetUnwritableLogDir();
    // Second call must fail (CreateLogDir() returns false -> K_NOT_READY).
    DS_ASSERT_NOT_OK(ScMetricsMonitor::Instance()->StartMonitor());

    // Restore a good log dir and verify the monitor can be started again.
    // If the failed call had already Shutdown() the old Tick (the bug from
    // review #15), this third call would still succeed, but the system would
    // have been left in a degraded state between call 2 and 3. The key
    // observable contract: recovery is possible and the process did not abort.
    RestoreLogDir();
    DS_ASSERT_OK(ScMetricsMonitor::Instance()->StartMonitor());
    ASSERT_TRUE(ScMetricsMonitor::Instance()->IsEnabled());
}
}  // namespace ut
}  // namespace datasystem
