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
#include "zmq_perf_server.h"
#include <chrono>
#include <map>
DS_DECLARE_int32(zmq_server_io_context);
namespace datasystem {
namespace st {

ZmqPerfServer::ZmqPerfServer()
    : maxNumThreads((int)std::thread::hardware_concurrency()),
      numThreads(maxNumThreads),
      hostPort(kDefaultPort),
      hostName(kDefaultHost)
{
}
ZmqPerfServer::~ZmqPerfServer()
{
    if (rpcServer) {
        rpcServer->Shutdown();
        rpcServer.reset();
    }
}

void ZmqPerfServer::PrintHelp()
{
    std::cout << "Options:\n"
                 "    -h,--help               This help message.\n"
                 "    --hostname              Set the hostname of the server."
              << " Default " << hostName
              << "\n"
                 "    --port                  Set the tcp/ip port of the server."
              << " Default " << kDefaultPort << "\n"
              << "    --zmq_io_threads        Set the ZMQ_IO_THREADS value."
              << " Default " << FLAGS_zmq_server_io_context << "\n"
              << "    -t,--num_threads        Set the number of zmq backend threads."
              << " Acceptable value is between 1 and " << maxNumThreads << "\n";
}

int ZmqPerfServer::ProcessArgsHelper(int opt)
{
    int32_t rc = 0;
    try {
        switch (opt) {
            case kPortOpt: {
                hostPort = std::stoi(optarg);
                break;
            }
            case kHostOpt: {
                hostName = optarg;
                break;
            }
            case kZmqIoOpt: {
                FLAGS_zmq_server_io_context = std::stoi(optarg);
                break;
            }
            case 't': {
                auto nThreads = std::stoi(optarg);
                if (nThreads <= 0) {
                    std::cerr << "Invalid number of backend threads: " << nThreads << std::endl;
                    PrintHelp();
                    rc = -1;
                    break;
                } else {
                    numThreads = nThreads;
                }
                break;
            }
            case ':':
                if (optopt == kHostOpt) {
                    std::cerr << "Missing argument for option --hostname" << std::endl;
                } else if (optopt == kPortOpt) {
                    std::cerr << "Missing argument for option --port" << std::endl;
                } else {
                    std::cerr << "Missing argument for option " << char(optopt) << std::endl;
                }
                rc = -1;
                break;
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

int ZmqPerfServer::ProcessArgs(int argc, char **argv)
{
    const char *const short_opts = ":t:h";
    const option long_opts[] = { { "port", required_argument, nullptr, kPortOpt },
                                 { "hostname", required_argument, nullptr, kHostOpt },
                                 { "zmq_io_threads", required_argument, nullptr, kZmqIoOpt },
                                 { "num_threads", required_argument, nullptr, 't' },
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
            }

            rc = ProcessArgsHelper(opt);
        }
    } catch (const std::exception &e) {
        PrintHelp();
        rc = -1;
    }
    return rc;
}

Status ZmqPerfServer::Run()
{
    HostPort serverPort(hostName, hostPort);
    rpcService = std::make_unique<PerfServiceImpl>(serverPort);
    auto bld = RpcServer::Builder();
    RpcServiceCfg cfg;
    cfg.numRegularSockets_ = numThreads;
    cfg.numStreamSockets_ = numThreads;
    cfg.udsEnabled_ = true;
    cfg.tcpDirect_ = "*";
    bld.AddEndPoint(RpcChannel::TcpipEndPoint(serverPort)).AddService(rpcService.get(), cfg);
    RETURN_IF_NOT_OK(bld.InitAndStart(rpcServer));
    return Status::OK();
}

PerfServiceImpl::PerfServiceImpl(HostPort serverAddr) : PerfService(std::move(serverAddr)), payloadSz_(0)
{
}

PerfServiceImpl::~PerfServiceImpl() = default;

Status PerfServiceImpl::SendPayload(const RunPb &rq, ErrorInfoPb &infoPb, std::vector<RpcMessage> recvPayload)
{
    (void)rq;
    (void)infoPb;
    (void)recvPayload;
    return datasystem::Status();
}

Status PerfServiceImpl::RecvPayload(const RunPb &rq, ErrorInfoPb &infoPb, std::vector<RpcMessage> &outPayload)
{
    (void)rq;
    (void)infoPb;
    (void)outPayload;
    return Status::OK();
}

Status PerfServiceImpl::SimpleGreeting(const MetaPb &rq, MetaPb &reply)
{
    reply = rq;
    GetLapTime(reply, "");
    return Status::OK();
}

Status PerfServiceImpl::UnarySocketSendPayload(
    std::shared_ptr<::datasystem::ServerUnaryWriterReader<MetaPb, MetaPb>> server_api)
{
    Status rc;
    MetaPb rq;
    RETURN_IF_NOT_OK(server_api->Read(rq));
    GetLapTime(rq, "");
    std::vector<RpcMessage> payload;
    RETURN_IF_NOT_OK(server_api->ReceivePayload(payload));
    GetLapTime(rq, "");
    RETURN_IF_NOT_OK(server_api->Write(rq));
    return rc;
}

Status PerfServiceImpl::UnarySocketSimpleGreeting(
    std::shared_ptr<::datasystem::ServerUnaryWriterReader<MetaPb, MetaPb>> server_api)
{
    MetaPb rq;
    RETURN_IF_NOT_OK(server_api->Read(rq));
    GetLapTime(rq, "");
    RETURN_IF_NOT_OK(server_api->Write(rq));
    return Status::OK();
}

Status PerfServiceImpl::UnarySocketRecvPayload(
    std::shared_ptr<::datasystem::ServerUnaryWriterReader<MetaPb, MetaPb>> serverApi)
{
    MetaPb rq;
    RETURN_IF_NOT_OK(serverApi->Read(rq));
    GetLapTime(rq, "");
    // We are overloading some fields to pass extra information.
    auto payloadSz = rq.payload_index() * kOneK;
    {
        WriteLock lock(&mux_);
        if (payloadBuffer_ == nullptr || payloadSz_ != payloadSz) {
            payloadSz_ = payloadSz;
            payloadBuffer_ = std::make_unique<char[]>(payloadSz_);
            std::ifstream ifs("/dev/urandom", std::ios_base::in | std::ios_base::binary);
            ifs.read(payloadBuffer_.get(), payloadSz_);
            ifs.close();
        }
    }
    MemView view{ payloadBuffer_.get(), static_cast<size_t>(payloadSz_) };
    bool tagFrame = rq.method_index() != 0;
    RETURN_IF_NOT_OK(serverApi->Write(rq));
    RETURN_IF_NOT_OK(serverApi->SendAndTagPayload({ view }, tagFrame));
    return Status::OK();
}
}  // namespace st
}  // namespace datasystem
