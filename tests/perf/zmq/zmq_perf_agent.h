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
 * Description: ZMQ perf agent class
 */
#ifndef DATASYSTEM_TESTS_ST_ZMQ_PERF_AGENT_H
#define DATASYSTEM_TESTS_ST_ZMQ_PERF_AGENT_H

#include <getopt.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/un.h>

#include <iomanip>
#include <iostream>
#include <string>

#include "datasystem/protos/zmq_perf.service.rpc.pb.h"
#include "zmq_perf_common.h"

namespace datasystem {
namespace st {

constexpr uint64_t NANO_TO_MICRO_UNIT = 1000;
class ZmqPerfAgent;
class PerfAgtSvcImpl final : public PerfAgentService {
public:
    explicit PerfAgtSvcImpl(ZmqPerfAgent *agt) : agt_(agt)
    {
    }
    ~PerfAgtSvcImpl() override = default;
    Status AgentSimpleGreeting(const PerfRunRqPb &rq, PerfRunResPb &reply) override;
    Status AgentUnarySocketSendPayloadRun(const PerfRunRqPb &pb, PerfRunResPb &resPb) override;
    Status AgentUnarySocketRecvPayloadRun(const PerfRunRqPb &pb, PerfRunResPb &resPb) override;
    Status AgentUnarySocketSimpleGreetingRun(const PerfRunRqPb &pb, PerfRunResPb &resPb) override;
    Status AgentDirectSendPayloadRun(const PerfRunRqPb &pb, PerfRunResPb &resPb) override;
    Status AgentSimpleTickRun(const PerfRunRqPb &pb, PerfTickRunResPb &resPb) override;

private:
    ZmqPerfAgent *agt_;
};

class ZmqPerfAgent {
public:
    ZmqPerfAgent() : payloadSz_(0), hostPort_(0), pid_(getpid()), poolSz_(0)
    {
    }
    ~ZmqPerfAgent() = default;
    void Print(std::ostream &out) const
    {
        out << "Run set up for PerfAgent (pid  " << pid_ << "):\n"
            << "Payload size in K: " << payloadSz_ << "\n"
            << "Tcp/ip port: " << hostPort_ << "\n"
            << "Hostname " << hostName_ << "\n";
    }
    friend std::ostream &operator<<(std::ostream &out, const ZmqPerfAgent &cp)
    {
        cp.Print(out);
        return out;
    }
    int ProcessArgs(int argc, char **argv);
    Status Run();

private:
    friend class PerfAgtSvcImpl;
    int payloadSz_;
    int hostPort_;
    int pid_;
    int poolSz_;
    std::string hostName_;
    std::string sockDir_;
    std::unique_ptr<RpcServer> rpcServer_;
    std::unique_ptr<PerfAgtSvcImpl> rpcService_;
    std::unique_ptr<char[]> payloadBuffer_;
    // A class to help maintain PerfRunLapPb
    struct PerfRunLapPbList {
        PerfRunLapPb tick;
        PerfRunLapPbList *next_{ nullptr };
    };
    std::shared_ptr<PerfService_Stub> CreateStub(const PerfRunRqPb &rq);

    Status BindAndListen();
    /**
     * Pre-allocate a payload buffer.
     * @return
     */
    Status InitBuffer();
    static Status SimpleGreetingAsyncRun(std::shared_ptr<PerfService_Stub> &stub, const PerfRunRqPb &rq,
                                         PerfRunResPb &reply);
    Status UnarySocketSendPayloadRun(std::shared_ptr<PerfService_Stub> &stub, const PerfRunRqPb &rq,
                                     PerfRunResPb &reply) const;
    Status UnarySocketRecvPayloadRun(std::shared_ptr<PerfService_Stub> &stub, const PerfRunRqPb &rq,
                                     PerfRunResPb &reply) const;
    static Status UnarySocketSimpleGreetingRun(std::shared_ptr<PerfService_Stub> &stub, const PerfRunRqPb &rq,
                                               PerfRunResPb &reply);
    Status DirectSendPayloadRun(const PerfRunRqPb &rq, PerfRunResPb &reply) const;
    Status SimpleTickRun(std::shared_ptr<ZmqStubImpl> &stubImpl, const std::string &svcName, const PerfRunRqPb &rq,
                         PerfTickRunResPb &reply) const;
    int ProcessArgsHelper(int opt);
    static void PrintHelp();
};

}  // namespace st
}  // namespace datasystem
#endif  // DATASYSTEM_TESTS_ST_ZMQ_PERF_AGENT_H
