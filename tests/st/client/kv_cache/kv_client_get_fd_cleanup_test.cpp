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

/** Description: Reproduces failed SHM Get reference retention and subsequent Set OOM. */
#include <cstdlib>
#include <fstream>
#include <memory>
#include <optional>
#include <string>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "client/object_cache/oc_client_common.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/kv_client.h"

DS_DECLARE_string(host_id_env_name);

namespace datasystem {
namespace st {
namespace {
constexpr size_t SHM_SIZE_MB = 128;
constexpr size_t VALUE_SIZE = 8 * 1024 * 1024;
constexpr size_t READ_COUNT = 2 * SHM_SIZE_MB * 1024 * 1024 / VALUE_SIZE;
constexpr char HOST_ENV[] = "get_fd_cleanup_host_id";
constexpr char FD_MISMATCH[] = "client.shm_fd.received_request_id_mismatch";
constexpr char SKIP_WARMUP[] = "ObjectClientImpl.ClientWorkerWarmup.skip";
constexpr char QUERY[] = "client.transport.query_and_get";
constexpr char GET[] = "client.transport.worker_oc_get";
constexpr char REGISTER[] = "client.transport.register_shm_client";
}  // namespace

class KVClientGetFdCleanupTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.numEtcd = 1;
        opts.numCoordinators = 0;
        opts.workerGflagParams = " -shared_memory_size_mb=" + std::to_string(SHM_SIZE_MB)
            + " -ipc_through_shared_memory=true -arena_per_tenant=1 -enable_urma=false"
              " -shm_ref_hard_reclaim_timeout_ms=0"
              " -log_monitor=true -json_log_monitor=true -log_monitor_interval_ms=200"
              " -host_id_env_name=" + HOST_ENV;
    }

    void SetUp() override
    {
        previousHostFlag_ = FLAGS_host_id_env_name;
        if (const char *value = std::getenv(HOST_ENV)) {
            previousHostValue_ = value;
        }
        FLAGS_host_id_env_name = HOST_ENV;
        ASSERT_EQ(setenv(HOST_ENV, "get-fd-cleanup-host", 1), 0);
        DS_ASSERT_OK(inject::Set(SKIP_WARMUP, "call()"));
        DS_ASSERT_OK(inject::Set(QUERY, "call()"));
        DS_ASSERT_OK(inject::Set(GET, "call()"));
        DS_ASSERT_OK(inject::Set(REGISTER, "call()"));
        ExternalClusterTest::SetUp();
        ConnectOptions options;
        InitConnectOpt(0, options);
        options.enableLocalCache = false;
        options.dataPlacementPolicy = DataPlacementPolicy::PREFERRED_META_OWNER;
        options.requestTimeoutMs = 2000;
        writer_ = std::make_shared<KVClient>(options);
        reader_ = std::make_shared<KVClient>(options);
        DS_ASSERT_OK(writer_->Init());
        DS_ASSERT_OK(reader_->Init());
    }

    void TearDown() override
    {
        for (const auto *point : { FD_MISMATCH, SKIP_WARMUP, QUERY, GET, REGISTER }) {
            (void)inject::Clear(point);
        }
        reader_.reset();
        writer_.reset();
        ExternalClusterTest::TearDown();
        FLAGS_host_id_env_name = previousHostFlag_;
        if (previousHostValue_) {
            (void)setenv(HOST_ENV, previousHostValue_->c_str(), 1);
        } else {
            (void)unsetenv(HOST_ENV);
        }
    }

protected:
    struct RefMetrics {
        bool fileOpen = false;
        bool complete = false;
        std::optional<int64_t> adds;
        std::optional<int64_t> removes;
        std::optional<int64_t> objects;
        std::optional<int64_t> bytes;
    };

    int GetTestCaseTimeoutSecs() const override
    {
        return 120;
    }

    void ReadRefMetricPart(const nlohmann::json &summary, RefMetrics &result) const
    {
        for (const auto &metric : summary["metrics"]) {
            if (!metric.contains("total") || !metric["total"].is_number_integer()) {
                continue;
            }
            const auto name = metric.value("name", "");
            const auto total = metric["total"].get<int64_t>();
            if (name == "worker_shm_ref_add_total") {
                result.adds = total;
            } else if (name == "worker_shm_ref_remove_total") {
                result.removes = total;
            } else if (name == "worker_shm_ref_table_size") {
                result.objects = total;
            } else if (name == "worker_shm_ref_table_bytes") {
                result.bytes = total;
            }
        }
    }

    RefMetrics ReadRefMetrics() const
    {
        std::ifstream input(FormatString("%s/worker0/log/kv_metrics.log", cluster_->GetRootDir()));
        RefMetrics result;
        result.fileOpen = input.is_open();
        int64_t cycle = -1;
        int parts = 0;
        int nextPart = 1;
        std::string line;
        while (std::getline(input, line)) {
            const auto summary = nlohmann::json::parse(line, nullptr, false);
            if (summary.is_discarded() || !summary.is_object()
                || summary.value("event", "") != "metrics_summary"
                || !summary.contains("metrics") || !summary["metrics"].is_array()) {
                continue;
            }
            const auto currentCycle = summary.value("cycle", int64_t{-1});
            const int part = summary.value("part_index", 0);
            if (currentCycle != cycle || part == 1) {
                result = RefMetrics{};
                result.fileOpen = true;
                cycle = currentCycle;
                parts = summary.value("part_count", 0);
                nextPart = 1;
            }
            if (cycle < 0 || parts <= 0 || part != nextPart || part > parts
                || summary.value("part_count", 0) != parts) {
                result.complete = false;
                nextPart = 0;
                continue;
            }
            ReadRefMetricPart(summary, result);
            result.complete = part == parts;
            ++nextPart;
        }
        return result;
    }

    Status WaitForEmptyRefs(size_t minimumAdds)
    {
        RefMetrics refs;
        const auto status = cluster_->WaitForExpectedResult([this, minimumAdds, &refs]() {
            refs = ReadRefMetrics();
            // Zero gauges are omitted when both this and the previous sample are zero.
            const bool empty = refs.objects.value_or(0) == 0 && refs.bytes.value_or(0) == 0;
            return refs.fileOpen && refs.complete && empty && refs.adds && refs.removes
                       && *refs.adds >= static_cast<int64_t>(minimumAdds) && refs.adds == refs.removes
                   ? Status::OK() : Status(K_NOT_READY, "Failed GET references have not been released");
        }, 10, K_OK);
        CHECK_FAIL_RETURN_STATUS(status.IsOk(), K_RUNTIME_ERROR,
                                 FormatString("SHM ref metrics timeout: fileOpen=%d complete=%d adds=%lld removes=%lld "
                                              "objects=%lld bytes=%lld (-1 means absent), path=%s/worker0/log/kv_metrics.log",
                                              refs.fileOpen, refs.complete, refs.adds.value_or(-1),
                                              refs.removes.value_or(-1), refs.objects.value_or(-1),
                                              refs.bytes.value_or(-1), cluster_->GetRootDir()));
        return Status::OK();
    }

    std::shared_ptr<KVClient> writer_;
    std::shared_ptr<KVClient> reader_;
    std::string previousHostFlag_;
    std::optional<std::string> previousHostValue_;
};

TEST_F(KVClientGetFdCleanupTest, LEVEL2_FailedGetsDoNotExhaustWorkerShm)
{
    const std::string value(VALUE_SIZE, 'g');
    const SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE_EVICT, .ttlSecond = 0 };
    const auto queryBefore = inject::GetExecuteCount(QUERY);
    const auto getBefore = inject::GetExecuteCount(GET);
    uint64_t mismatches = 0;
    uint64_t registrations = 0;
    for (size_t i = 0; i < READ_COUNT; ++i) {
        SCOPED_TRACE("failed Get iteration " + std::to_string(i));
        const auto key = "get_fd_cleanup_" + std::to_string(i);
        // Without the fix, deleted objects retain GET refs and a later Set exhausts the quota here.
        DS_ASSERT_OK(writer_->Set(key, value, param));
        {
            DS_ASSERT_OK(inject::Set(FD_MISMATCH, "call()"));
            Raii clearMismatch([] { (void)inject::Clear(FD_MISMATCH); });
            Optional<Buffer> buffer;
            const Status rc = reader_->Get(key, buffer);
            ASSERT_EQ(rc.GetCode(), K_RUNTIME_ERROR) << rc;
            ASSERT_NE(rc.ToString().find("Received shared-memory fds do not match GetClientFd request"),
                      std::string::npos) << rc;
            ASSERT_FALSE(buffer);
            const auto count = inject::GetExecuteCount(FD_MISMATCH);
            ASSERT_GT(count, 0u) << "GET must reach FD reception after the Worker returned SHM data";
            mismatches += count;
        }
        DS_ASSERT_OK(writer_->Del(key));
        if (i == 0) {
            registrations = inject::GetExecuteCount(REGISTER);
        } else {
            ASSERT_EQ(inject::GetExecuteCount(REGISTER), registrations)
                << "Session replacement would hide leaked references through client-lost cleanup";
        }
    }
    EXPECT_GE(inject::GetExecuteCount(QUERY) - queryBefore, READ_COUNT);
    EXPECT_GE(inject::GetExecuteCount(GET) - getBefore, READ_COUNT);
    EXPECT_GE(mismatches, READ_COUNT);

    // Assert Set recovery before waiting on metrics; neither client has been disconnected.
    DS_ASSERT_OK(writer_->Set("get_fd_cleanup_final", value, param));
    {
        Optional<Buffer> buffer;
        DS_ASSERT_OK(reader_->Get("get_fd_cleanup_final", buffer));
        ASSERT_TRUE(buffer);
        ASSERT_EQ(std::string(static_cast<const char *>(buffer->ImmutableData()), buffer->GetSize()), value);
    }
    DS_ASSERT_OK(writer_->Del("get_fd_cleanup_final"));
    DS_ASSERT_OK(WaitForEmptyRefs(READ_COUNT));
    EXPECT_EQ(inject::GetExecuteCount(REGISTER), registrations);
}
}  // namespace st
}  // namespace datasystem
