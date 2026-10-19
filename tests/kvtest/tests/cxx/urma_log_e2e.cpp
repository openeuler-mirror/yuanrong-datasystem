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

#include <cmath>
#include <cstdlib>
#include <iostream>
#include <string>

#include "datasystem/context/context.h"
#include "datasystem/kv_client.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log_sampler.h"

namespace {
constexpr int kArgumentCount = 5;
constexpr int kRequestTimeoutMs = 10000;
constexpr size_t kPayloadBytes = 1024 * 1024;
constexpr int kRequestCount = 3;
constexpr int kMaxRequestCount = 1000;
constexpr int kDecimalBase = 10;

bool Check(const datasystem::Status &status, const std::string &operation)
{
    std::cout << operation << " status=" << status.ToString() << std::endl;
    return status.IsOk();
}
}  // namespace

int main(int argc, char **argv)
{
    if (argc != kArgumentCount) {
        std::cerr << "usage: urma_log_e2e host writer_port reader_port sdk_log_dir" << std::endl;
        return EXIT_FAILURE;
    }
    const char *rateEnv = std::getenv("URMA_LOG_E2E_SAMPLE_RATE");
    char *end = nullptr;
    const double sampleRate = rateEnv == nullptr ? 0.0 : std::strtod(rateEnv, &end);
    if (rateEnv != nullptr && (end == rateEnv || *end != '\0' || !std::isfinite(sampleRate)
                              || sampleRate < 0.0 || sampleRate > 1.0)) {
        std::cerr << "invalid URMA_LOG_E2E_SAMPLE_RATE" << std::endl;
        return EXIT_FAILURE;
    }
    const char *countEnv = std::getenv("URMA_LOG_E2E_REQUEST_COUNT");
    const long requestCount = countEnv == nullptr ? kRequestCount : std::strtol(countEnv, &end, kDecimalBase);
    if (requestCount <= 0 || requestCount > kMaxRequestCount
        || (countEnv != nullptr && (end == countEnv || *end != '\0'))) {
        std::cerr << "invalid URMA_LOG_E2E_REQUEST_COUNT" << std::endl;
        return EXIT_FAILURE;
    }
    datasystem::ConnectOptions writerOptions;
    writerOptions.host = argv[1];
    writerOptions.port = std::atoi(argv[2]);
    writerOptions.requestTimeoutMs = kRequestTimeoutMs;
    datasystem::ConnectOptions readerOptions = writerOptions;
    readerOptions.port = std::atoi(argv[3]);
    datasystem::KVClient writer(writerOptions);
    datasystem::KVClient reader(readerOptions);
    datasystem::KVClientConfig config;
    if (!Check(datasystem::KVClientConfig::Builder().LogDir(argv[4]).Build(config), "CONFIG")
        || !Check(writer.Init(config), "WRITER_INIT") || !Check(reader.Init(), "READER_INIT")
        || !Check(datasystem::Context::SetTraceId("UrmaLogE2E"), "TRACE_PREFIX")
        || !Check(datasystem::inject::Set("UrmaMock.ProviderLog", "call()"), "PROVIDER_LOG_INJECT")) {
        return EXIT_FAILURE;
    }
    if (!datasystem::LogSampler::Instance().UpdateConfigFromFlags({ sampleRate, 1.0, sampleRate })) {
        return EXIT_FAILURE;
    }
    std::cout << "SAMPLE_RATES request=" << sampleRate << " diagnostic=" << sampleRate << " access=1"
              << " request_count=" << requestCount << std::endl;
    const std::string payload(kPayloadBytes, 'u');
    for (long i = 0; i < requestCount; ++i) {
        const auto key = "urma-log-e2e-" + std::to_string(i);
        if (!Check(writer.Set(key, payload), "SET " + key)) {
            return EXIT_FAILURE;
        }
        std::string result;
        if (!Check(reader.Get(key, result), "REMOTE_GET " + key) || result != payload) {
            std::cerr << "PAYLOAD_MISMATCH key=" << key << " bytes=" << result.size() << std::endl;
            return EXIT_FAILURE;
        }
        std::cout << "PAYLOAD_OK key=" << key << " bytes=" << result.size() << std::endl;
        if (!Check(writer.Del(key), "DELETE " + key)) {
            return EXIT_FAILURE;
        }
    }
    if (!Check(datasystem::inject::Clear("UrmaMock.ProviderLog"), "CLEAR_INJECT")
        || !Check(reader.ShutDown(), "READER_SHUTDOWN") || !Check(writer.ShutDown(), "WRITER_SHUTDOWN")) {
        return EXIT_FAILURE;
    }
    std::cout << "URMA_LOG_E2E_PASS" << std::endl;
    return EXIT_SUCCESS;
}
