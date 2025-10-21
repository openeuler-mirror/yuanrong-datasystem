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
 * Description: ZMQ perf class
 */
#ifndef DATASYSTEM_TESTS_ST_ZMQ_PERF_RUN_H
#define DATASYSTEM_TESTS_ST_ZMQ_PERF_RUN_H

#include "zmq_perf_common.h"
#include <getopt.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <iomanip>
#include <iostream>
#include <string>
#include "datasystem/protos/zmq_perf.stub.rpc.pb.h"

namespace datasystem {
namespace st {

constexpr int kDefaultNumIterations = 10240;  // 1 MB each iteration
constexpr int kDefaultPayloadSz = 1024;       // to make up 10g of logstream
constexpr int kDefaultNumClients = 10;        // Forked client
constexpr bool kIsLocalClient = false;
constexpr bool kIsTcpDirect = false;
constexpr bool kNoMemoryCopy = false;
constexpr int kNumConnections = 3;
constexpr uint64_t NANO_TO_MICRO_UNIT = 1000;

typedef Status (PerfAgentService_Stub::*AsyncWriteFunc)(const PerfRunRqPb &, int64_t &);
typedef Status (PerfAgentService_Stub::*AsyncReadFunc)(int64_t, PerfRunResPb &, ZmqRecvFlags);

/**
 * Class for running client benchmark.
 */
class ZmqPerfRun {
public:
    ZmqPerfRun();
    ~ZmqPerfRun();

    void Print(std::ostream &out) const
    {
        out << "Number of iterations: " << numIterations_ << "\n"
            << "Payload size in K: " << payloadSz_ << "\n"
            << "Number of clients: " << numClients_ << "\n"
            << "Tcp/ip port: " << hostPort_ << "\n"
            << "Hostname " << hostName_ << "\n"
            << "Local client: " << std::boolalpha << localClient_ << "\n"
            << "Tcp direct: " << std::boolalpha << tcpDirect_ << "\n"
            << "Connection pool size: " << poolSz_ << "\n"
            << "No memory copy: " << std::boolalpha << noMemCopy_ << "\n";
    }

    friend std::ostream &operator<<(std::ostream &out, const ZmqPerfRun &cp)
    {
        cp.Print(out);
        return out;
    }

    /**
     * Process command line arguments.
     * @param argc
     * @param argv
     * @return
     */
    int ProcessArgs(int argc, char **argv);

    /**
     * Run all benchmarks
     * @return
     */
    Status RunAllBenchmarks();

private:
    void PrintHelp();
    int ProcessArgsHelper(int opt);

    /**
     * Simple Hello World run
     */
    Status SimpleGreetingAsyncRun();

    /**
     * Run ZMQ send payload benchmark
     * @return Status object
     */
    Status DirectSendPayloadRun();

    /**
     * Run ZMQ send payload benchmark with UnaryWriterReader
     * @return Status object
     */
    Status UnarySocketSendPayloadRun();

    /**
     * Run ZMQ recv payload benchmark with UnaryWriterReader
     * @return Status object
     */
    Status UnarySocketRecvPayloadRun();

    /**
     * Run ZMQ simple greeting benchmark with UnaryWriterReader
     * @return Status object
     */
    Status UnarySocketSimpleGreetingRun();

    /**
     * Tick run to track the lap time in each component
     */
    Status SimpleTickRun();

    /**
     * Print summary for each run.
     * @param lap
     */
    std::string PrintSummary(const std::string &title, std::vector<int64_t> &lap) const;
    /**
     * Queue latency test
     */
    Status MessageQueRun() const;

    /**
     * Entry point to run client tests
     */
    Status RunClientTest();

    int numIterations_;
    int payloadSz_;
    int numClients_;
    int hostPort_;
    std::string hostName_;
    bool localClient_;
    bool tcpDirect_;
    int poolSz_;
    bool noMemCopy_;
    std::string socketDir_;
    std::vector<pid_t> clients_;
    Status StartAgent(int inx);
    Status SpawnAgents();
    Status SimpleTestBatchSend(const std::string &title, std::vector<std::shared_ptr<PerfAgentService_Stub>> &stubs,
                               std::vector<int64_t> &tags, AsyncWriteFunc writeFunc, bool hasPayload);
    Status SimpleTestBatchRecv(const std::string &title,
                               const std::vector<std::shared_ptr<PerfAgentService_Stub>> &stubs,
                               const std::vector<int64_t> &tags, AsyncReadFunc readFunc, int64_t &totalElapsedTime);
    Status SimpleTest(const std::string &title, AsyncWriteFunc writeFunc, AsyncReadFunc readFunc, bool hasPayload);
};

}  // namespace st
}  // namespace datasystem
#endif  // DATASYSTEM_TESTS_ST_ZMQ_PERF_RUN_H
