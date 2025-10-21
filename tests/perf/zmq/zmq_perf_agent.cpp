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
 * Description: ZMQ perf agent run time
 */
#include "zmq_perf_agent.h"

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/rpc/rpc_unary_client_impl.h"
#include "datasystem/common/rpc/zmq/zmq_context.h"
#include "datasystem/common/rpc/zmq/zmq_stub_impl.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/protos/zmq_perf.stub.rpc.pb.h"
namespace datasystem {
namespace st {

void ZmqPerfAgent::PrintHelp()
{
    std::cout << "Options:\n"
              << "    -h,--help               This help message.\n"
              << "    --payload_size          Set the payload size (in K).\n"
              << "    --hostname              Set the hostname of the server.\n"
              << "    --port                  Set the tcp/ip port of the server.\n"
              << "    --unix_socket_dir       Set the unix socket directory.\n"
              << "    --connect_pool_size     Set the socket pool size.\n";
}

int ZmqPerfAgent::ProcessArgsHelper(int opt)
{
    int32_t rc = 0;
    try {
        switch (opt) {
            case 's': {
                payloadSz_ = std::stoi(optarg);
                break;
            }
            case kPortOpt: {
                hostPort_ = std::stoi(optarg);
                break;
            }
            case kHostOpt: {
                hostName_ = optarg;
                break;
            }
            case kUnixSockOpt: {
                sockDir_ = optarg;
                break;
            }
            case 'c': {
                poolSz_ = std::stoi(optarg);
                break;
            }
            case ':': {
                if (optopt == kHostOpt) {
                    std::cerr << "Missing argument for option --hostname" << std::endl;
                } else if (optopt == kPortOpt) {
                    std::cerr << "Missing argument for option --port" << std::endl;
                } else {
                    std::cerr << "Missing argument for option " << char(optopt) << std::endl;
                }
                rc = -1;
                break;
            }
            case '?':  // Unrecognized option
            default:
                std::cerr << "Unknown option " << char(optopt) << std::endl;
                PrintHelp();
                rc = -1;
                break;
        }
    } catch (const std::exception &e) {
        PrintHelp();
        rc = -1;
    }
    return rc;
}

int ZmqPerfAgent::ProcessArgs(int argc, char **argv)
{
    const char *const short_opts = ":s:h:c";
    const option long_opts[] = { { "payload_size", required_argument, nullptr, 's' },
                                 { "port", required_argument, nullptr, kPortOpt },
                                 { "hostname", required_argument, nullptr, kHostOpt },
                                 { "unix_socket_dir", required_argument, nullptr, kUnixSockOpt },
                                 { "connect_pool_size", required_argument, nullptr, 'c' },
                                 { "help", no_argument, nullptr, 'h' },
                                 { nullptr, no_argument, nullptr, 0 } };
    std::map<int32_t, int32_t> seen_opts;
    int rc = 0;
    try {
        while (rc == 0) {
            int32_t optionIndex;
            const auto opt = getopt_long(argc, argv, short_opts, long_opts, &optionIndex);
            if (opt == -1) {
                if (optind < argc) {
                    rc = -1;
                    std::cerr << "Unknown arguments: ";
                    while (optind < argc) {
                        std::cerr << argv[optind++] << " ";
                    }
                    std::cerr << std::endl;
                }
                break;
            } else if (opt > 0) {
                seen_opts[opt]++;
                if (seen_opts[opt] > 1) {
                    std::string long_name = long_opts[optionIndex].name;
                    std::cerr << "The " << long_name << " argument was given more than once." << std::endl;
                    rc = -1;
                    continue;
                }
            } else {
                continue;
            }

            rc = ProcessArgsHelper(opt);
        }
    } catch (const std::exception &e) {
        PrintHelp();
        rc = -1;
    }
    return rc;
}
Status ZmqPerfAgent::InitBuffer()
{
    auto bufSz = payloadSz_ * kOneK;
    payloadBuffer_ = std::make_unique<char[]>(bufSz);
    std::ifstream ifs("/dev/urandom", std::ios_base::in | std::ios_base::binary);
    ifs.read(payloadBuffer_.get(), bufSz);
    ifs.close();
    return Status::OK();
}

std::shared_ptr<PerfService_Stub> ZmqPerfAgent::CreateStub(const PerfRunRqPb &rq)
{
    HostPort serverAddr(hostName_, hostPort_);
    auto channel = std::make_shared<RpcChannel>(RpcChannel::TcpipEndPoint(serverAddr), RpcCredential());
    if (rq.local_client()) {
        channel->SetServiceUdsEnabled(PerfService_Stub::FullServiceName(),
                                      GetServiceSockName(ServiceSocketNames::DEFAULT_SOCK));
    }
    if (rq.tcp_direct()) {
        channel->SetServiceTcpDirect(PerfService_Stub::FullServiceName());
    }
    channel->SetServiceConnectPoolSize(PerfService_Stub::FullServiceName(), poolSz_);
    auto stub = std::make_shared<PerfService_Stub>(channel);
    return stub;
}

Status ZmqPerfAgent::BindAndListen()
{
    HostPort serverAddr(hostName_, hostPort_);
    auto sockPath = FormatString("%s_%d", RpcChannel::UnixSocketPath(sockDir_, serverAddr), pid_);
    Status rc = CreateDir(sockDir_, true);
    if (!rc.IsOk() && !FileExist(sockDir_)) {
        RETURN_STATUS(rc.GetCode(), rc.GetMsg());
    }
    rpcService_ = std::make_unique<PerfAgtSvcImpl>(this);
    auto bld = RpcServer::Builder();
    RpcServiceCfg cfg;
    cfg.numRegularSockets_ = 1;
    cfg.numStreamSockets_ = 0;
    cfg.udsEnabled_ = true;
    bld.AddEndPoint(sockPath).AddService(rpcService_.get(), cfg);
    RETURN_IF_NOT_OK(bld.InitAndStart(rpcServer_));
    return Status::OK();
}

Status ZmqPerfAgent::Run()
{
    RETURN_IF_NOT_OK(InitBuffer());
    RETURN_IF_NOT_OK(BindAndListen());
    return Status::OK();
}

inline void SetupReply(PerfRunResPb &reply, std::vector<int64_t> &elapsed, std::vector<int64_t> &lap)
{
    PerfRunLapPb lapPb;
    google::protobuf::RepeatedField<google::protobuf::int64> lap_data(lap.begin(), lap.end());
    google::protobuf::RepeatedField<google::protobuf::int64> elapsed_data(elapsed.begin(), elapsed.end());
    lapPb.mutable_lap()->Swap(&lap_data);
    reply.mutable_elapsed_time()->Swap(&elapsed_data);
    reply.mutable_time()->CopyFrom(lapPb);
}

Status ZmqPerfAgent::SimpleGreetingAsyncRun(std::shared_ptr<PerfService_Stub> &stub, const PerfRunRqPb &rq,
                                            PerfRunResPb &reply)
{
    const auto numIterations = rq.num_iterations();
    std::vector<int64_t> lap;
    lap.reserve(numIterations);
    std::vector<int64_t> elapsed;
    elapsed.reserve(numIterations);
    for (auto i = 0; i < numIterations; ++i) {
        int64_t tag = 0;
        MetaPb meta;
        StartTheClock(meta);
        auto startTick = std::chrono::steady_clock::now();
        RETURN_IF_NOT_OK(stub->SimpleGreetingAsyncWrite(meta, tag));
        MetaPb rsp;
        RETURN_IF_NOT_OK(stub->SimpleGreetingAsyncRead(tag, rsp));
        auto endTick = std::chrono::steady_clock::now();
        auto elapsedTime = std::chrono::duration_cast<std::chrono::microseconds>(endTick - startTick).count();
        elapsed.push_back(elapsedTime);
        lap.push_back(GetTotalTime(rsp) / NANO_TO_MICRO_UNIT);
    }
    SetupReply(reply, elapsed, lap);
    return Status::OK();
}

Status ZmqPerfAgent::UnarySocketSendPayloadRun(std::shared_ptr<PerfService_Stub> &stub, const PerfRunRqPb &rq,
                                               PerfRunResPb &reply) const
{
    const auto numIterations = rq.num_iterations();
    auto bufSz = payloadSz_ * kOneK;
    std::vector<int64_t> lap;
    lap.reserve(numIterations);
    std::vector<int64_t> elapsed;
    elapsed.reserve(numIterations);
    for (auto i = 0; i < numIterations; ++i) {
        MetaPb meta;
        StartTheClock(meta);
        auto startTick = std::chrono::steady_clock::now();
        MetaPb rsp;
        MemView payload(payloadBuffer_.get(), bufSz);
        RETURN_IF_NOT_OK(stub->UnarySocketSendPayload(meta, rsp, { payload }));
        auto endTick = std::chrono::steady_clock::now();
        auto elapsedTime = std::chrono::duration_cast<std::chrono::microseconds>(endTick - startTick).count();
        elapsed.push_back(elapsedTime);
        lap.push_back(GetTotalTime(rsp) / NANO_TO_MICRO_UNIT);
    }
    SetupReply(reply, elapsed, lap);
    return Status::OK();
}

Status ZmqPerfAgent::UnarySocketRecvPayloadRun(std::shared_ptr<PerfService_Stub> &stub, const PerfRunRqPb &rq,
                                               PerfRunResPb &reply) const
{
    const auto numIterations = rq.num_iterations();
    std::vector<int64_t> lap;
    lap.reserve(numIterations);
    std::vector<int64_t> elapsed;
    elapsed.reserve(numIterations);
    for (auto i = 0; i < numIterations; ++i) {
        std::unique_ptr<ClientUnaryWriterReader<MetaPb, MetaPb>> clientApi;
        RETURN_IF_NOT_OK(stub->UnarySocketRecvPayload(RpcOptions(), &clientApi));
        MetaPb meta;
        // We are going to overload the MetaPb to send additional messages.
        meta.set_payload_index(payloadSz_);
        meta.set_method_index(rq.no_memcpy() ? 1 : 0);
        StartTheClock(meta);
        auto startTick = std::chrono::steady_clock::now();
        RETURN_IF_NOT_OK(clientApi->Write(meta));
        MetaPb rsp;
        RETURN_IF_NOT_OK(clientApi->Read(rsp));
        if (clientApi->IsV2Client()) {
            RETURN_IF_NOT_OK(clientApi->ReceivePayload(payloadBuffer_.get(), payloadSz_ * kOneK));
        }
        auto endTick = std::chrono::steady_clock::now();
        auto elapsedTime = std::chrono::duration_cast<std::chrono::microseconds>(endTick - startTick).count();
        elapsed.push_back(elapsedTime);
        lap.push_back(GetTotalTime(rsp) / NANO_TO_MICRO_UNIT);
    }
    SetupReply(reply, elapsed, lap);
    return Status::OK();
}

Status ZmqPerfAgent::UnarySocketSimpleGreetingRun(std::shared_ptr<PerfService_Stub> &stub, const PerfRunRqPb &rq,
                                                  PerfRunResPb &reply)
{
    const auto numIterations = rq.num_iterations();
    std::vector<int64_t> lap;
    lap.reserve(numIterations);
    std::vector<int64_t> elapsed;
    elapsed.reserve(numIterations);
    for (auto i = 0; i < numIterations; ++i) {
        MetaPb meta;
        StartTheClock(meta);
        auto startTick = std::chrono::steady_clock::now();
        MetaPb rsp;
        RETURN_IF_NOT_OK(stub->UnarySocketSimpleGreeting(meta, rsp));
        auto endTick = std::chrono::steady_clock::now();
        auto elapsedTime = std::chrono::duration_cast<std::chrono::microseconds>(endTick - startTick).count();
        elapsed.push_back(elapsedTime);
        lap.push_back(GetTotalTime(rsp) / NANO_TO_MICRO_UNIT);
    }
    SetupReply(reply, elapsed, lap);
    return Status::OK();
}

Status ZmqPerfAgent::DirectSendPayloadRun(const PerfRunRqPb &rq, PerfRunResPb &reply) const
{
    const auto numIterations = rq.num_iterations();
    auto bufSz = payloadSz_ * kOneK;
    auto ctx = std::make_shared<ZmqContext>();
    RETURN_IF_NOT_OK(ctx->Init());
    ZmqSocket sock(ctx, ZmqSocketType::DEALER);
    HostPort serverAddr(hostName_, hostPort_);
    auto channel = std::make_shared<RpcChannel>(RpcChannel::TcpipEndPoint(serverAddr), RpcCredential());
    RETURN_IF_NOT_OK(sock.Connect(*channel));
    std::vector<int64_t> lap;
    std::vector<int64_t> elapsed;
    lap.reserve(numIterations);
    elapsed.reserve(numIterations);
    for (auto i = 0; i < numIterations; ++i) {
        size_t dummy = 0;
        // The code below is mostly taken from AsyncWrite because we mainly want to measure the payload
        MemView payload(payloadBuffer_.get(), bufSz);
        // Call another internal service which will record the duration (on the server side) for getting all
        // the packets.
        PayloadTickRqPb rqt;
        ZmqMsgFrames frames;
        ZmqPayload::AddPayloadFrames({ payload }, frames, dummy, false);
        RETURN_IF_NOT_OK(PushFrontProtobufToFrames(rqt, frames));
        MetaPb meta = CreateMetaData("", ZMQ_HEARTBEAT_METHOD, ZMQ_EMBEDDED_PAYLOAD_INX, sock.GetWorkerId());
        RETURN_IF_NOT_OK(PushFrontProtobufToFrames(meta, frames));
        auto startTick = std::chrono::steady_clock::now();
        RETURN_IF_NOT_OK(sock.SendAllFrames(frames, ZmqSendFlags::NONE));
        frames.clear();
        RETURN_IF_NOT_OK(sock.GetAllFrames(frames, ZmqRecvFlags::NONE));
        auto endTick = std::chrono::steady_clock::now();
        frames.pop_front();
        MetaPb rsp;
        ZmqMessage replyMsg = std::move(frames.front());
        frames.pop_front();
        RETURN_IF_NOT_OK(ParseFromZmqMessage(replyMsg, rsp));
        auto elapsedTime = std::chrono::duration_cast<std::chrono::microseconds>(endTick - startTick).count();
        elapsed.push_back(elapsedTime);
        lap.push_back(GetTotalTime(rsp) / NANO_TO_MICRO_UNIT);
    }
    ctx->Close();
    SetupReply(reply, elapsed, lap);
    return Status::OK();
}

Status ZmqPerfAgent::SimpleTickRun(std::shared_ptr<ZmqStubImpl> &stubImpl, const std::string &svcName,
                                   const PerfRunRqPb &rq, PerfTickRunResPb &reply) const
{
    PerfRunLapPbList *head = nullptr;
    const auto numIterations = rq.num_iterations();
    auto bufSz = payloadSz_ * kOneK;
    std::vector<int64_t> elapsed;
    elapsed.reserve(numIterations);
    std::set<std::string> tickNames;
    for (auto i = 0; i < numIterations; ++i) {
        auto startTick = std::chrono::steady_clock::now();
        MetaPb rsp;
        MemView payload(payloadBuffer_.get(), bufSz);
        RETURN_IF_NOT_OK(stubImpl->PayloadTick(svcName, rsp, { payload }));
        auto endTick = std::chrono::steady_clock::now();
        auto elapsedTime = std::chrono::duration_cast<std::chrono::microseconds>(endTick - startTick).count();
        elapsed.push_back(elapsedTime);
        // Make sure we have enough room.
        PerfRunLapPbList *curr = head;
        PerfRunLapPbList *prev = nullptr;
        for (int k = 0; k < rsp.ticks_size() - 1; ++k) {
            auto tickName = rsp.ticks(k + 1).tick_name();
            auto lapTime = (rsp.ticks(k + 1).ts() - rsp.ticks(k).ts()) / NANO_TO_MICRO_UNIT;
            auto it = tickNames.find(tickName);
            bool addEntry = it == tickNames.end();
            if (addEntry) {
                auto ele = std::make_unique<PerfRunLapPbList>();
                curr = ele.release();
                curr->tick.set_tick_name(tickName);
                if (prev != nullptr) {
                    curr->next_ = prev->next_;
                    prev->next_ = curr;
                } else {
                    head = curr;
                }
                tickNames.emplace(tickName);
            } else {
                // Move the pointer forward to the right one.
                while (curr->tick.tick_name() != tickName) {
                    prev = curr;
                    curr = curr->next_;
                }
            }
            curr->tick.mutable_lap()->Add(lapTime);
            prev = curr;
            curr = curr->next_;
        }
    }
    
    // Now put everything together
    google::protobuf::RepeatedField<std::int64_t> elapsed_data(elapsed.begin(), elapsed.end());
    reply.mutable_elapsed_time()->Swap(&elapsed_data);
    PerfRunLapPbList *curr = head;
    while (curr != nullptr) {
        reply.mutable_time()->Add(std::move(curr->tick));
        auto next = curr->next_;
        delete curr;
        curr = next;
    }
    return Status::OK();
}

Status PerfAgtSvcImpl::AgentSimpleGreeting(const PerfRunRqPb &rq, PerfRunResPb &reply)
{
    auto stub = agt_->CreateStub(rq);
    return agt_->SimpleGreetingAsyncRun(stub, rq, reply);
}
Status PerfAgtSvcImpl::AgentUnarySocketSendPayloadRun(const PerfRunRqPb &pb, PerfRunResPb &resPb)
{
    auto stub = agt_->CreateStub(pb);
    return agt_->UnarySocketSendPayloadRun(stub, pb, resPb);
}
Status PerfAgtSvcImpl::AgentUnarySocketRecvPayloadRun(const PerfRunRqPb &pb, PerfRunResPb &resPb)
{
    auto stub = agt_->CreateStub(pb);
    return agt_->UnarySocketRecvPayloadRun(stub, pb, resPb);
}
Status PerfAgtSvcImpl::AgentUnarySocketSimpleGreetingRun(const PerfRunRqPb &pb, PerfRunResPb &resPb)
{
    auto stub = agt_->CreateStub(pb);
    return agt_->UnarySocketSimpleGreetingRun(stub, pb, resPb);
}
Status PerfAgtSvcImpl::AgentDirectSendPayloadRun(const PerfRunRqPb &pb, PerfRunResPb &resPb)
{
    return agt_->DirectSendPayloadRun(pb, resPb);
}
Status PerfAgtSvcImpl::AgentSimpleTickRun(const PerfRunRqPb &pb, PerfTickRunResPb &resPb)
{
    // We will be switching to an internal stub because PayloadTick is not externalized
    HostPort serverAddr(agt_->hostName_, agt_->hostPort_);
    auto channel = std::make_shared<RpcChannel>(RpcChannel::TcpipEndPoint(serverAddr), RpcCredential());
    if (pb.local_client()) {
        channel->SetServiceUdsEnabled("datasystem.st.PerfService",
                                      GetServiceSockName(ServiceSocketNames::DEFAULT_SOCK));
    }
    if (pb.tcp_direct()) {
        channel->SetServiceTcpDirect("datasystem.st.PerfService");
    }
    channel->SetServiceConnectPoolSize("datasystem.st.PerfService", agt_->poolSz_);
    auto stubImpl = std::make_shared<ZmqStubImpl>(channel);
    // Fake a stub
    struct DummyStub : public ZmqStub {
        explicit DummyStub(std::shared_ptr<::datasystem::RpcChannel> channel) : ZmqStub(std::move(channel))
        {
        }
        ~DummyStub() override = default;
        std::string FullServiceName() const override
        {
            return "datasystem.st.PerfService";
        }
        std::string ServiceName() const override
        {
            return "PerfService";
        }
    };
    DummyStub stub(channel);
    RETURN_IF_NOT_OK(stubImpl->InitConn(&stub));
    return agt_->SimpleTickRun(stubImpl, stub.ServiceName(), pb, resPb);
}

}  // namespace st
}  // namespace datasystem
