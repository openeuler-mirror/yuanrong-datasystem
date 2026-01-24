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

#include "kv_bench.h"

#include <numeric>
#include <random>

#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/kv_client.h"
#include "datasystem/utils/status.h"
#include "utils.h"

namespace datasystem {
namespace bench {

Status KVBench::WarmUp()
{
    std::string originalKeyPrefix = args_.keyPrefix;
    uint64_t originalKeyNum = args_.keyNum;

    args_.keyPrefix += "-pre-run";
    args_.keyNum = args_.batchNum;

    auto rc = ParallelRun();
    if (rc.IsError()) {
        std::cerr << "ERROR: WarmUp phase failed - " << rc.GetMsg() << std::endl;
        return rc;
    }

    args_.keyPrefix = originalKeyPrefix;
    args_.keyNum = originalKeyNum;

    return Status::OK();
}

Status KVBench::Prepare()
{
    if (!args_.ownerWorker.empty()) {
        RETURN_IF_NOT_OK(FetchOwnerId(args_.ownerWorker, args_.accessKey, args_.secretKey, args_.ownerId));
    }
    CHECK_FAIL_RETURN_STATUS(args_.threadNum > 0, K_INVALID, "thread_num must > 0");
    std::vector<std::string> keys;
    if (args_.action == "set") {
        GenerateSetKeys(keys);
    } else if (args_.action == "get" || args_.action == "del") {
        CHECK_FAIL_RETURN_STATUS(args_.workerNum >= 0, K_INVALID, "worker_index must >= 0");
        GenerateGetOrDelKeys(keys);
    } else {
        RETURN_STATUS(K_INVALID, "unknown action" + args_.action);
    }

    std::shuffle(keys.begin(), keys.end(), std::mt19937{ std::random_device{}() });

    perThreadKeys_.clear();
    perThreadKeys_.resize(args_.threadNum);

    for (size_t index = 0; index < keys.size(); index++) {
        auto idx = index % args_.threadNum;
        perThreadKeys_[idx].emplace_back(std::move(keys[index]));
    }

    return Status::OK();
}

void KVBench::GenerateSetKeys(std::vector<std::string> &keys)
{
    keys.reserve(args_.keyNum);
    for (size_t index = 0; index < args_.keyNum; index++) {
        std::stringstream ss;
        ss << args_.keyPrefix;
        ss << "_s" << args_.workerNum;
        ss << "_n" << index;
        if (!args_.ownerId.empty()) {
            ss << ";" << args_.ownerId;
        }
        keys.emplace_back(ss.str());
    }
}

void KVBench::GenerateGetOrDelKeys(std::vector<std::string> &keys)
{
    keys.reserve(args_.keyNum * (args_.workerNum + 1));
    for (int sid = 0; sid <= args_.workerNum; sid++) {
        for (size_t index = 0; index < args_.keyNum; index++) {
            std::stringstream ss;
            ss << args_.keyPrefix;
            ss << "_s" << sid;
            ss << "_n" << index;
            if (!args_.ownerId.empty()) {
                ss << ";" << args_.ownerId;
            }
            keys.emplace_back(ss.str());
        }
    }
}

Status KVBench::PrintBenchmarkInfo()
{
    std::cout << "BENCHMARK-RESULT:" << GetBenchCost() << std::endl;
    return Status::OK();
}

std::string KVBench::GetBenchCost()
{
    std::vector<uint64_t> costVec;
    for (const auto &costs : perThreadCostDetail_) {
        for (const auto &cost : costs) {
            costVec.emplace_back(cost);
        }
    }
    std::stringstream ss;
    ss << args_.action;
    ss << "-" << args_.threadNum;
    ss << "-" << args_.keyNum;
    ss << "-" << args_.keySize;
    ss << "-" << args_.batchNum;
    std::sort(costVec.begin(), costVec.end());
    if (costVec.empty()) {
        ss << ":empty cost";
        return ss.str();
    }
    uint64_t valueSize = 1;
    auto rc = StringToBytes(args_.keySize, valueSize);
    if (rc.IsError()) {
        ss << "invalid keySize:" << args_.keySize;
        return ss.str();
    }

    const uint64_t BYTES_TO_MEGABYTES = 1024 * 1024;
    const double MICROSECONDS_TO_MILLISECONDS = 1000.0;
    const size_t PERCENTILE_90 = 90;
    const size_t PERCENTILE_99 = 99;
    const size_t PERCENTILE_100 = 100;

    double totalTimeCost = std::accumulate(costVec.begin(), costVec.end(), 0.0);  // MicroSecond
    uint64_t totalKeyNum = args_.keyNum * args_.threadNum;
    uint64_t totalValueSize = args_.keyNum * args_.threadNum * valueSize;  // bytes

    double threadCostSum = std::accumulate(perThreadCost_.begin(), perThreadCost_.end(), 0.0);
    double timeCostPerThread =
        threadCostSum / args_.threadNum / MICROSECONDS_TO_MILLISECONDS / MICROSECONDS_TO_MILLISECONDS;  // Second

    float avg = totalTimeCost / costVec.size();
    auto count = costVec.size();

    ss << "," << avg / MICROSECONDS_TO_MILLISECONDS;                                              // avg ms
    ss << "," << costVec[0] / MICROSECONDS_TO_MILLISECONDS;                                       // min ms
    ss << "," << costVec[count * PERCENTILE_90 / PERCENTILE_100] / MICROSECONDS_TO_MILLISECONDS;  // p90 ms
    ss << "," << costVec[count * PERCENTILE_99 / PERCENTILE_100] / MICROSECONDS_TO_MILLISECONDS;  // p99 ms
    ss << "," << costVec[count - 1] / MICROSECONDS_TO_MILLISECONDS;                               // max ms
    ss << "," << totalKeyNum / timeCostPerThread;                          // tps: object count/sec
    ss << "," << totalValueSize / timeCostPerThread / BYTES_TO_MEGABYTES;  // thuoughput MB
    return ss.str();
}

Status KVBench::Run(uint64_t threadIndex, Barrier &barrier)
{
    std::unique_ptr<KVClient> client;
    auto init = [&] {
        ConnectOptions connectOptions;
        RETURN_IF_NOT_OK(bench::StrToHostPort(args_.workerAddress, connectOptions.host, connectOptions.port));
        client = std::make_unique<KVClient>(connectOptions);
        return client->Init();
    };
    auto rc = init();
    barrier.Wait();
    RETURN_IF_NOT_OK(rc);

    if (args_.action == "set") {
        RETURN_IF_NOT_OK(Set(client, threadIndex));
    } else if (args_.action == "get") {
        RETURN_IF_NOT_OK(Get(client, threadIndex));
    } else if (args_.action == "del") {
        RETURN_IF_NOT_OK(Del(client, threadIndex));
    } else {
        RETURN_STATUS(K_INVALID, "unknown action" + args_.action);
    }
    return Status::OK();
}

Status KVBench::FetchOwnerId(const std::string &ownerWorkerAddr, const std::string &accessKey,
                             const std::string &secretKey, std::string &ownerId)
{
    HostPort hostPort;
    RETURN_IF_NOT_OK(hostPort.ParseString(ownerWorkerAddr));

    ConnectOptions connectOptions = { .host = hostPort.Host(), .port = hostPort.Port(), .connectTimeoutMs = 3000 };
    connectOptions.accessKey = accessKey;
    connectOptions.secretKey = secretKey;

    KVClient client(connectOptions);
    auto initStatus = client.Init();
    if (initStatus.IsError()) {
        return Status(initStatus.GetCode(), __LINE__, __FILE__,
                      "Failed to connect to owner_worker: " + ownerWorkerAddr +
                      ". Please check if owner_worker is correct. Error: " + initStatus.GetMsg());
    }

    std::string key;
    RETURN_IF_NOT_OK(client.GenerateKey("No", key));

    auto index = key.find(';');
    if (index == std::string::npos) {
        return Status(K_INVALID, __LINE__, __FILE__, "Invalid key format: semicolon not found.");
    }
    ownerId = key.substr(index + 1);

    return Status::OK();
}

Status KVBench::Set(std::unique_ptr<KVClient> &client, uint64_t threadIndex)
{
    auto &costs = perThreadCostDetail_[threadIndex];
    auto &allKeys = perThreadKeys_[threadIndex];
    if (args_.batchNum < 1) {
        return Status(K_INVALID, "batchNum must be >= 1");
    }

    uint64_t valueSize = 1;
    RETURN_IF_NOT_OK(StringToBytes(args_.keySize, valueSize));
    std::string data(valueSize, 'a');
    Timer totalTimer;

    if (args_.batchNum == 1) {
        for (const auto &key : allKeys) {
            Timer timer;
            RETURN_IF_NOT_OK(client->Set(key, data));
            costs.emplace_back(timer.ElapsedMicroSecond());
        }
        perThreadCost_[threadIndex] = totalTimer.ElapsedMicroSecond();
        return Status::OK();
    }

    std::vector<std::string> keys;
    std::vector<StringView> valuesView;
    keys.reserve(args_.batchNum);
    valuesView.reserve(args_.batchNum);

    for (const auto &key : allKeys) {
        keys.emplace_back(key);
        valuesView.emplace_back(data);

        if (keys.size() == args_.batchNum) {
            Timer timer;
            std::vector<std::string> failedKeys;
            Status rc = client->MSet(keys, valuesView, failedKeys);
            if (!rc.IsOk()) {
                return rc;
            }
            costs.emplace_back(timer.ElapsedMicroSecond());
            keys.clear();
            valuesView.clear();
        }
    }

    if (!keys.empty()) {
        Timer timer;
        std::vector<std::string> failedKeys;
        Status rc = client->MSet(keys, valuesView, failedKeys);
        if (!rc.IsOk()) {
            return rc;
        }
        costs.emplace_back(timer.ElapsedMicroSecond());
    }

    perThreadCost_[threadIndex] = totalTimer.ElapsedMicroSecond();
    return Status::OK();
}

Status KVBench::Get(std::unique_ptr<KVClient> &client, uint64_t threadIndex)
{
    auto &costs = perThreadCostDetail_[threadIndex];
    auto &allKeys = perThreadKeys_[threadIndex];

    Timer totalTimer;

    std::vector<std::string> keys;
    keys.reserve(args_.batchNum);
    for (const auto &key : allKeys) {
        keys.emplace_back(std::move(key));
        if (keys.size() == args_.batchNum) {
            std::vector<Optional<ReadOnlyBuffer>> readOnlyBuffers;
            Timer timer;
            RETURN_IF_NOT_OK(client->Get(keys, readOnlyBuffers));
            costs.emplace_back(timer.ElapsedMicroSecond());
            keys.clear();
        }
    }
    if (!keys.empty()) {
        std::vector<Optional<ReadOnlyBuffer>> readOnlyBuffers;
        Timer timer;
        RETURN_IF_NOT_OK(client->Get(keys, readOnlyBuffers));
        costs.emplace_back(timer.ElapsedMicroSecond());
    }

    perThreadCost_[threadIndex] = totalTimer.ElapsedMicroSecond();
    return Status::OK();
}

Status KVBench::Del(std::unique_ptr<KVClient> &client, uint64_t threadIndex)
{
    auto &costs = perThreadCostDetail_[threadIndex];
    auto &allKeys = perThreadKeys_[threadIndex];

    Timer totalTimer;

    std::vector<std::string> keys;
    keys.reserve(args_.batchNum);
    for (const auto &key : allKeys) {
        keys.emplace_back(std::move(key));
        if (keys.size() == args_.batchNum) {
            std::vector<std::string> failedKeys;
            Timer timer;
            RETURN_IF_NOT_OK(client->Del(keys, failedKeys));
            costs.emplace_back(timer.ElapsedMicroSecond());
            keys.clear();
        }
    }
    if (!keys.empty()) {
        std::vector<std::string> failedKeys;
        Timer timer;
        RETURN_IF_NOT_OK(client->Del(keys, failedKeys));
        costs.emplace_back(timer.ElapsedMicroSecond());
    }
    perThreadCost_[threadIndex] = totalTimer.ElapsedMicroSecond();
    return Status::OK();
}

}  // namespace bench
}  // namespace datasystem
