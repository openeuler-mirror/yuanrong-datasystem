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

/** Description: Measure late-timeout handling after a hard UB provider error. */

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <limits>
#include <string>
#include <thread>
#include <vector>

#include "datasystem/common/object_cache/peer_ub_admission.h"

namespace datasystem {
namespace {

constexpr size_t DEFAULT_THREAD_COUNT = 16;
constexpr uint32_t DEFAULT_REPORTS_PER_THREAD = 30'000;
constexpr int PEER_PORT = 31501;
constexpr uint64_t PROBE_TIME_MS = 10;

const HostPort PEER("127.0.0.1", PEER_PORT);

const char *StateName(UbAdmissionState state)
{
    switch (state) {
        case UbAdmissionState::AVAILABLE:
            return "AVAILABLE";
        case UbAdmissionState::SUSPECT:
            return "SUSPECT";
        case UbAdmissionState::UNAVAILABLE:
            return "UNAVAILABLE";
        case UbAdmissionState::PROBING:
            return "PROBING";
    }
    return "UNKNOWN";
}

bool ParsePositiveUnsigned(const std::string &value, uint64_t *parsed)
{
    if (value.empty()) {
        return false;
    }
    char *end = nullptr;
    errno = 0;
    const unsigned long long number = std::strtoull(value.c_str(), &end, 10);
    if (errno == ERANGE || *end != '\0' || number == 0) {
        return false;
    }
    *parsed = number;
    return true;
}

bool ParseArguments(int argc, char **argv, size_t *threadCount, uint32_t *reportsPerThread)
{
    constexpr const char *THREADS_PREFIX = "--threads=";
    constexpr const char *REPORTS_PREFIX = "--reports-per-thread=";
    for (int index = 1; index < argc; ++index) {
        const std::string argument(argv[index]);
        uint64_t parsed = 0;
        if (argument.rfind(THREADS_PREFIX, 0) == 0
            && ParsePositiveUnsigned(argument.substr(std::char_traits<char>::length(THREADS_PREFIX)), &parsed)
            && parsed <= std::numeric_limits<size_t>::max()) {
            *threadCount = static_cast<size_t>(parsed);
            continue;
        }
        if (argument.rfind(REPORTS_PREFIX, 0) == 0
            && ParsePositiveUnsigned(argument.substr(std::char_traits<char>::length(REPORTS_PREFIX)), &parsed)
            && parsed <= std::numeric_limits<uint32_t>::max()) {
            *reportsPerThread = static_cast<uint32_t>(parsed);
            continue;
        }
        std::cerr << "Usage: " << argv[0]
                  << " [--threads=positive-integer] [--reports-per-thread=positive-integer]" << std::endl;
        return false;
    }
    return true;
}

UbOpOutcome HardProviderFailure()
{
    UbOpOutcome outcome(PEER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                        Status(K_URMA_ERROR, "synthetic provider CQE error 4"));
    outcome.cqeStatus = 4;
    return outcome;
}

UbOpOutcome LateTimeout()
{
    return UbOpOutcome(PEER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                       Status(K_RPC_DEADLINE_EXCEEDED, "synthetic late timeout"));
}

}  // namespace

int RunBenchMain(int argc, char **argv)
{
    size_t threadCount = DEFAULT_THREAD_COUNT;
    uint32_t reportsPerThread = DEFAULT_REPORTS_PER_THREAD;
    if (!ParseArguments(argc, argv, &threadCount, &reportsPerThread)) {
        return 2;
    }

    PeerUbAdmission admission;
    admission.ReportOutcome(HardProviderFailure());
    const auto stateBefore = admission.GetState(PEER);
    if (!stateBefore.has_value()) {
        std::cerr << "hard provider failure did not create admission state" << std::endl;
        return 1;
    }

    const UbOpOutcome lateTimeout = LateTimeout();
    std::atomic<size_t> ready{ 0 };
    std::atomic<bool> start{ false };
    std::vector<std::thread> reporters;
    reporters.reserve(threadCount);
    for (size_t thread = 0; thread < threadCount; ++thread) {
        reporters.emplace_back([&] {
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (uint32_t report = 0; report < reportsPerThread; ++report) {
                admission.ReportOutcome(lateTimeout);
            }
        });
    }
    while (ready.load(std::memory_order_acquire) != threadCount) {
        std::this_thread::yield();
    }

    const std::clock_t cpuBegin = std::clock();
    const auto wallBegin = std::chrono::steady_clock::now();
    start.store(true, std::memory_order_release);
    for (auto &reporter : reporters) {
        reporter.join();
    }
    const auto wallEnd = std::chrono::steady_clock::now();
    const std::clock_t cpuEnd = std::clock();

    const auto stateAfter = admission.GetState(PEER);
    if (!stateAfter.has_value()) {
        std::cerr << "late timeout reports removed admission state" << std::endl;
        return 1;
    }

    PeerUbAdmission probingAdmission;
    probingAdmission.InitializeVerification(PEER, PROBE_TIME_MS);
    const auto probe = probingAdmission.TryBeginProbe(PEER, PROBE_TIME_MS);
    if (!probe.has_value()) {
        std::cerr << "unable to begin recovery probe" << std::endl;
        return 1;
    }
    probingAdmission.ReportOutcome(lateTimeout);
    const bool probeTokenSurvived = probingAdmission.CompleteProbe(*probe, Status::OK(), PROBE_TIME_MS + 1, false);

    const uint64_t totalReports = static_cast<uint64_t>(threadCount) * reportsPerThread;
    const double processCpuMs = static_cast<double>(cpuEnd - cpuBegin) * 1000.0 / CLOCKS_PER_SEC;
    const double wallMs = std::chrono::duration<double, std::milli>(wallEnd - wallBegin).count();
    std::cout << "scenario=peer_ub_admission_late_timeout\n"
              << "threads=" << threadCount << "\n"
              << "reports_per_thread=" << reportsPerThread << "\n"
              << "total_late_timeouts=" << totalReports << "\n"
              << "wall_ms=" << wallMs << "\n"
              << "process_cpu_ms=" << processCpuMs << "\n"
              << "state_before=" << StateName(stateBefore->state) << "\n"
              << "state_after=" << StateName(stateAfter->state) << "\n"
              << "epoch_before=" << stateBefore->epoch << "\n"
              << "epoch_after=" << stateAfter->epoch << "\n"
              << "epoch_delta=" << stateAfter->epoch - stateBefore->epoch << "\n"
              << "read_admission_blocked=" << admission.CheckReadSource(PEER).IsError() << "\n"
              << "probe_token_survived=" << probeTokenSurvived << std::endl;
    return 0;
}

}  // namespace datasystem

int main(int argc, char **argv)
{
    return datasystem::RunBenchMain(argc, argv);
}
