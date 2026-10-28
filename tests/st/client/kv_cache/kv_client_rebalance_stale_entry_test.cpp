/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Reproduction ST for issue #1180 problem 3: a migration target that rolls back staged
 * copies of pre-existing replica entries must reclaim those entries, otherwise the worker OBJECT_COUNT
 * metric stays at the pre-migration level while the memory has already been released.
 */

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "client/kv_cache/kv_client_common.h"
#include "cluster/external_cluster.h"
#include "common.h"
#include "common_distributed_ext.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/metrics/res_metric_name.h"
#include "datasystem/common/util/format.h"
#include "datasystem/kv_client.h"
#include "datasystem/protos/cluster_topology.pb.h"

DS_DECLARE_string(log_dir);

namespace datasystem {
namespace st {
namespace {
constexpr uint32_t WORKER0 = 0;
constexpr uint32_t WORKER1 = 1;
constexpr uint32_t WORKER2 = 2;
// 38 x 1MiB objects cross the 70 percent source threshold of a 64MiB worker under allocator usage accounting;
// recalibrate the key counts when shared_memory_size_mb or the rebalance usage thresholds change.
constexpr size_t VALUE_SIZE = 1024UL * 1024UL;
constexpr size_t REPLICA_KEY_COUNT = 4;
constexpr size_t UNREAD_KEY_COUNT = 34;
constexpr size_t PRESSURE_KEY_COUNT = 10;
constexpr size_t SOURCE_KEY_COUNT = REPLICA_KEY_COUNT + UNREAD_KEY_COUNT;
constexpr size_t RESOURCE_LOG_PREFIX_FIELD_COUNT = 7;
constexpr int POLL_INTERVAL_MS = 200;
constexpr int RING_STABLE_TIMEOUT_MS = 45'000;
constexpr int REPLACE_PRIMARY_TIMEOUT_MS = 30'000;
constexpr int METRIC_SETTLE_TIMEOUT_MS = 30'000;
constexpr int COUNT_FALLBACK_TIMEOUT_MS = 40'000;
const std::string REPLACE_PRIMARY_POINT = "OCMetadataManager.ReplacePrimary";

std::vector<uint32_t> AllWorkers()
{
    return { WORKER0, WORKER1, WORKER2 };
}

std::vector<std::string> KeysRange(const std::vector<std::string> &keys, size_t begin, size_t end)
{
    return { keys.begin() + static_cast<long>(begin), keys.begin() + static_cast<long>(end) };
}

bool ParseUint64Field(const std::string &value, uint64_t &result)
{
    if (value.empty()) {
        return false;
    }
    uint64_t parsed = 0;
    for (char c : value) {
        if (c < '0' || c > '9') {
            return false;
        }
        parsed = parsed * 10u + static_cast<uint64_t>(c - '0');
    }
    result = parsed;
    return true;
}

Status ParseObjectCountFromJsonLine(const std::string &line, uint64_t &count)
{
    if (line.find("\"event\":\"resource_snapshot\"") == std::string::npos) {
        return Status(K_NOT_READY, "line is not a resource snapshot");
    }
    const std::string marker = "\"object_count\":";
    const auto pos = line.find(marker);
    if (pos == std::string::npos) {
        return Status(K_NOT_READY, "object_count is absent from the resource snapshot");
    }
    const size_t begin = pos + marker.size();
    size_t end = begin;
    while (end < line.size() && line[end] >= '0' && line[end] <= '9') {
        ++end;
    }
    if (begin == end || !ParseUint64Field(line.substr(begin, end - begin), count)) {
        return Status(K_NOT_READY, "object_count field is empty or not numeric in the resource snapshot");
    }
    return Status::OK();
}

Status ParseObjectCountFromTextLine(const std::string &line, uint64_t &count)
{
    std::vector<std::string> fields;
    size_t begin = 0;
    while (true) {
        const size_t pos = line.find(" | ", begin);
        if (pos == std::string::npos) {
            fields.emplace_back(line.substr(begin));
            break;
        }
        fields.emplace_back(line.substr(begin, pos - begin));
        begin = pos + 3;
    }
    const size_t metricIndex = RESOURCE_LOG_PREFIX_FIELD_COUNT
                               + static_cast<size_t>(ResMetricName::OBJECT_COUNT)
                               - static_cast<size_t>(ResMetricName::SHARED_MEMORY);
    if (fields.size() <= metricIndex) {
        return Status(K_NOT_READY,
                      FormatString("OBJECT_COUNT field index %zu is out of range for a resource log line with %zu "
                                   "fields",
                                   metricIndex, fields.size()));
    }
    if (fields[metricIndex].empty()) {
        return Status(K_NOT_READY, "OBJECT_COUNT field is empty in the resource log line");
    }
    if (!ParseUint64Field(fields[metricIndex], count)) {
        return Status(K_NOT_READY,
                      FormatString("OBJECT_COUNT field %s is not numeric in the resource log line",
                                   fields[metricIndex]));
    }
    return Status::OK();
}

Status ReadLastLine(const std::string &path, std::string &lastLine);

Status ParseKvMetricsGauge(const std::string &line, const std::string &metricName, uint64_t &value)
{
    const std::string nameKey = "\"name\":\"" + metricName + "\"";
    const auto namePos = line.find(nameKey);
    if (namePos == std::string::npos) {
        return Status(K_NOT_FOUND, FormatString("metric %s is absent from the kv_metrics line", metricName));
    }
    const std::string totalKey = "\"total\":";
    const auto totalPos = line.find(totalKey, namePos + nameKey.size());
    if (totalPos == std::string::npos) {
        return Status(K_NOT_READY, FormatString("metric %s has no total field in the kv_metrics line", metricName));
    }
    const size_t digitsStart = totalPos + totalKey.size();
    const size_t digitsEnd = line.find(',', digitsStart);
    const std::string digits =
        line.substr(digitsStart, digitsEnd == std::string::npos ? std::string::npos : digitsEnd - digitsStart);
    if (digits.empty() || digits.find_first_not_of("0123456789") != std::string::npos) {
        return Status(K_NOT_READY, FormatString("metric %s total %s is not numeric", metricName, digits));
    }
    value = std::stoull(digits);
    return Status::OK();
}

Status ReadWorkerKvMetricsGauge(uint32_t workerIndex, const std::string &metricName, uint64_t &value)
{
    const std::string logDir = FormatString("%s/../worker%u/log", FLAGS_log_dir.c_str(), workerIndex);
    std::ifstream input(logDir + "/kv_metrics.log");
    if (!input.is_open()) {
        return Status(K_NOT_READY, "kv_metrics.log is not ready");
    }
    // kv_metrics.log chunks one summary across several lines (part_index/part_count), so the last line is not
    // guaranteed to contain the metric; keep the latest line that carries it instead.
    const std::string nameKey = "\"name\":\"" + metricName + "\"";
    std::string line;
    std::string matched;
    while (std::getline(input, line)) {
        if (line.find(nameKey) != std::string::npos) {
            matched = line;
        }
    }
    if (matched.empty()) {
        return Status(K_NOT_READY, FormatString("kv_metrics.log has no sample for %s", metricName));
    }
    return ParseKvMetricsGauge(matched, metricName, value);
}

Status ReadLastLine(const std::string &path, std::string &lastLine)
{
    std::ifstream input(path);
    if (!input.is_open()) {
        return Status(K_NOT_READY, FormatString("log file is not ready: %s", path));
    }
    std::string line;
    lastLine.clear();
    while (std::getline(input, line)) {
        if (!line.empty()) {
            lastLine = line;
        }
    }
    if (lastLine.empty()) {
        return Status(K_NOT_READY, FormatString("log file is empty: %s", path));
    }
    return Status::OK();
}
}  // namespace

class LEVEL1_KVClientRollbackStaleEntryTest : public KVClientCommon, public CommonDistributedExt {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 3;
        opts.numEtcd = 0;
        opts.numCoordinators = 1;
        opts.numOBS = 0;
        opts.waitWorkerReady = true;
        opts.workerGflagParams =
            "-shared_memory_size_mb=64 -log_monitor=true -log_monitor_interval_ms=500 "
            "-enable_memory_rebalance=true -rebalance_source_usage_percent=70 -rebalance_usage_gap_percent=30 "
            "-rebalance_task_report_grace_ms=500 -data_migrate_rate_limit_mb=8 "
            "-enable_lossless_data_exit_mode=true -node_timeout_s=5 -node_dead_timeout_s=10";
        opts.injectActions = "NodeSelector.setInterval:call(200);"
                             "ResourceManager.setInterval:call(200);"
                             "MemoryRebalanceScheduler.CooldownSeconds:call(1);"
                             "OCMetadataManager.ReplacePrimary:100000*return(K_RUNTIME_ERROR)";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        InitTestKVClient(WORKER0, client0_);
        InitTestKVClient(WORKER1, client1_);
        InitTestKVClient(WORKER2, client2_);
    }

    void TearDown() override
    {
        client0_.reset();
        client1_.reset();
        client2_.reset();
        ExternalClusterTest::TearDown();
    }

    int GetTestCaseTimeoutSecs() const override
    {
        return 180;
    }

protected:
    BaseCluster *GetCluster() override
    {
        return cluster_.get();
    }

    uint64_t GetInjectCountIfAlive(uint32_t workerIndex, const std::string &name)
    {
        uint64_t count = 0;
        (void)cluster_->GetInjectActionExecuteCount(WORKER, workerIndex, name, count);
        return count;
    }

    uint64_t GetTotalInjectCount(const std::string &name)
    {
        uint64_t total = 0;
        for (auto workerIndex : AllWorkers()) {
            total += GetInjectCountIfAlive(workerIndex, name);
        }
        return total;
    }

    bool WaitForTotalInjectCount(const std::string &name, uint64_t expectedCount, int timeoutMs)
    {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        do {
            if (GetTotalInjectCount(name) >= expectedCount) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(POLL_INTERVAL_MS));
        } while (std::chrono::steady_clock::now() < deadline);
        return GetTotalInjectCount(name) >= expectedCount;
    }

    bool WaitForRingStable(int timeoutMs)
    {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        do {
            ClusterTopologyPb ring;
            if (cluster_->ReadClusterTopology(ring).IsOk() && ring.members_size() == 3) {
                bool allActive = true;
                for (const auto &member : ring.members()) {
                    if (member.second.state() != MembershipPb::ACTIVE) {
                        allActive = false;
                        break;
                    }
                }
                if (allActive) {
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                    return true;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(POLL_INTERVAL_MS));
        } while (std::chrono::steady_clock::now() < deadline);
        return false;
    }

    Status ReadWorkerObjectCount(uint32_t workerIndex, uint64_t &count)
    {
        const std::string logDir = FormatString("%s/../worker%u/log", FLAGS_log_dir.c_str(), workerIndex);
        std::string line;
        Status rc = ReadLastLine(logDir + "/kv_resource.log", line);
        if (rc.IsOk()) {
            rc = ParseObjectCountFromJsonLine(line, count);
            if (rc.IsOk()) {
                return Status::OK();
            }
        }
        rc = ReadLastLine(logDir + "/resource.log", line);
        if (rc.IsError()) {
            return rc;
        }
        return ParseObjectCountFromTextLine(line, count);
    }

    bool WaitForWorkerObjectCount(uint32_t workerIndex, const std::function<bool(uint64_t)> &predicate, int timeoutMs,
                                  uint64_t &observed)
    {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        Status lastRc(K_NOT_READY, "no object count sample");
        do {
            uint64_t count = 0;
            lastRc = ReadWorkerObjectCount(workerIndex, count);
            if (lastRc.IsOk()) {
                observed = count;
                if (predicate(count)) {
                    return true;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(POLL_INTERVAL_MS));
        } while (std::chrono::steady_clock::now() < deadline);
        std::cout << "[rollback_stale_entry] worker=" << workerIndex << ", lastRc=" << lastRc.ToString()
                  << ", observed=" << observed << std::endl;
        return false;
    }

    std::vector<std::string> WriteObjects(const std::shared_ptr<KVClient> &client,
                                          const std::vector<std::string> &keys, char valueChar)
    {
        const std::string value(VALUE_SIZE, valueChar);
        for (const auto &key : keys) {
            auto rc = client->Set(key, value);
            if (rc.IsError()) {
                ADD_FAILURE() << FormatString("Set %s failed: %s", key, rc.ToString());
                return {};
            }
        }
        return keys;
    }

    std::shared_ptr<KVClient> client0_;
    std::shared_ptr<KVClient> client1_;
    std::shared_ptr<KVClient> client2_;
};

TEST_F(LEVEL1_KVClientRollbackStaleEntryTest, LEVEL1_RollbackStaleReplicaEntriesKeepWorkerObjectCountHigh)
{
    ASSERT_TRUE(WaitForRingStable(RING_STABLE_TIMEOUT_MS));
    SetWorkerHashInjection({ 0, 1, 2 });

    std::vector<std::string> sourceKeys;
    GetObjectKeysHashToWorker(nullptr, WORKER0, SOURCE_KEY_COUNT, sourceKeys);
    auto replicaKeys = KeysRange(sourceKeys, 0, REPLICA_KEY_COUNT);
    auto unreadKeys = KeysRange(sourceKeys, REPLICA_KEY_COUNT, SOURCE_KEY_COUNT);
    std::vector<std::string> pressureKeys;
    GetObjectKeysHashToWorker(nullptr, WORKER2, PRESSURE_KEY_COUNT, pressureKeys);

    WriteObjects(client2_, pressureKeys, 'p');
    auto sourceBatch = WriteObjects(client0_, replicaKeys, 'r');
    ASSERT_FALSE(sourceBatch.empty());
    for (const auto &key : replicaKeys) {
        std::string value;
        DS_ASSERT_OK(client1_->Get(key, value));
        ASSERT_EQ(value.size(), VALUE_SIZE) << key;
    }

    uint64_t replicaBaseline = 0;
    ASSERT_TRUE(WaitForWorkerObjectCount(
        WORKER1, [](uint64_t count) { return count >= REPLICA_KEY_COUNT; }, METRIC_SETTLE_TIMEOUT_MS,
        replicaBaseline))
        << "the hot replicas never showed up in the WORKER1 OBJECT_COUNT metric";

    const auto replacePrimaryBaseline = GetTotalInjectCount(REPLACE_PRIMARY_POINT);

    auto bumpedBatch = WriteObjects(client0_, replicaKeys, 's');
    ASSERT_FALSE(bumpedBatch.empty());
    auto unreadBatch = WriteObjects(client0_, unreadKeys, 'u');
    ASSERT_FALSE(unreadBatch.empty());

    ASSERT_TRUE(
        WaitForTotalInjectCount(REPLACE_PRIMARY_POINT, replacePrimaryBaseline + 1, REPLACE_PRIMARY_TIMEOUT_MS))
        << "the rebalance migration never reached the master ReplacePrimary handler";

    uint64_t stagedPeak = replicaBaseline;
    uint64_t observed = replicaBaseline;
    const bool fellBack = WaitForWorkerObjectCount(
        WORKER1,
        [replicaBaseline, &stagedPeak](uint64_t count) {
            stagedPeak = std::max(stagedPeak, count);
            return count < REPLICA_KEY_COUNT;
        },
        COUNT_FALLBACK_TIMEOUT_MS, observed);
    EXPECT_TRUE(fellBack)
        << FormatString("WORKER1 OBJECT_COUNT stayed at %lu (staged peak sample %lu, pre-migration baseline %lu) "
                        "after the failed migration; the %zu rolled back stale replica entries were never "
                        "reclaimed from the object table",
                        observed, stagedPeak, replicaBaseline, REPLICA_KEY_COUNT);

    uint64_t gaugeCount = 0;
    bool gaugeFellBack = false;
    const auto gaugeDeadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(COUNT_FALLBACK_TIMEOUT_MS);
    do {
        if (ReadWorkerKvMetricsGauge(WORKER1, "worker_object_count", gaugeCount).IsOk()
            && gaugeCount < REPLICA_KEY_COUNT) {
            gaugeFellBack = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(POLL_INTERVAL_MS));
    } while (std::chrono::steady_clock::now() < gaugeDeadline);
    EXPECT_TRUE(gaugeFellBack)
        << FormatString("the WORKER_OBJECT_COUNT kv_metrics gauge stayed at %lu after the reclaimed rollback; "
                        "the collector-refresh mirror is broken", gaugeCount);
}

}  // namespace st
}  // namespace datasystem
