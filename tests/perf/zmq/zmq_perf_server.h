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
#ifndef DATASYSTEM_TESTS_ST_ZMQ_PERF_SERVER_H
#define DATASYSTEM_TESTS_ST_ZMQ_PERF_SERVER_H

#include <getopt.h>
#include <iostream>
#include <string>
#include <thread>
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/rpc/rpc_channel.h"
#include "datasystem/common/rpc/rpc_server.h"
#include "datasystem/protos/zmq_perf.service.rpc.pb.h"
#include "datasystem/protos/utils.pb.h"
#include "zmq_perf_common.h"
namespace datasystem {
namespace st {

/**
 * Implementation of virtual function for zmq_perf.proto
 */
class PerfServiceImpl final : public PerfService {
public:
    explicit PerfServiceImpl(HostPort serverAddr);
    ~PerfServiceImpl() override;

    Status SendPayload(const RunPb &rq, ErrorInfoPb &infoPb, std::vector<RpcMessage> recvPayload) override;
    Status RecvPayload(const RunPb &rq, ErrorInfoPb &infoPb, std::vector<RpcMessage> &outPayload) override;
    Status SimpleGreeting(const MetaPb &rq, MetaPb &reply) override;
    Status UnarySocketSendPayload(
        std::shared_ptr<::datasystem::ServerUnaryWriterReader<MetaPb, MetaPb>> server_api) override;
    Status UnarySocketSimpleGreeting(
        std::shared_ptr<::datasystem::ServerUnaryWriterReader<MetaPb, MetaPb>> server_api) override;
    Status UnarySocketRecvPayload(
        std::shared_ptr<::datasystem::ServerUnaryWriterReader<MetaPb, MetaPb>> serverApi) override;

private:
    WriterPrefRWLock mux_;
    int64_t payloadSz_;
    std::unique_ptr<char[]> payloadBuffer_;
};
/**
 * Class for setting up ZMQ benchmark server
 */
class ZmqPerfServer {
public:
    ZmqPerfServer();
    ~ZmqPerfServer();

    int ProcessArgs(int argc, char **argv);

    void Print(std::ostream &out) const
    {
        out << "Tcp/ip port: " << hostPort << "\n"
            << "Hostname: " << hostName << "\n"
            << "Number of threads: " << numThreads << "\n";
    }

    friend std::ostream &operator<<(std::ostream &out, const ZmqPerfServer &cp)
    {
        cp.Print(out);
        return out;
    }

    /**
     * Bring up the server
     * @return
     */
    Status Run();

private:
    void PrintHelp();
    int ProcessArgsHelper(int opt);

    const int maxNumThreads;
    int numThreads;
    int hostPort;
    std::string hostName;
    std::unique_ptr<RpcServer> rpcServer;
    std::unique_ptr<PerfServiceImpl> rpcService;
};

}  // namespace st
}  // namespace datasystem
#endif  // DATASYSTEM_TESTS_ST_ZMQ_PERF_SERVER_H
