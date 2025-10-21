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
#include "zmq_perf_run.h"
#include <sys/epoll.h>
#include <sys/wait.h>
#include <unistd.h>
#include <chrono>
#include <fstream>
#include <limits>
#include <list>
#include <map>
#include <queue>
#include "datasystem/common/rpc/rpc_channel.h"
#include "datasystem/common/rpc/zmq/zmq_msg_queue.h"
#include "datasystem/common/rpc/zmq/zmq_payload.h"
#include "datasystem/common/rpc/zmq/zmq_socket.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/uri.h"

namespace datasystem {
namespace st {

ZmqPerfRun::ZmqPerfRun()
    : numIterations_(kDefaultNumIterations),
      payloadSz_(kDefaultPayloadSz),
      numClients_(kDefaultNumClients),
      hostPort_(kDefaultPort),
      hostName_(kDefaultHost),
      localClient_(kIsLocalClient),
      tcpDirect_(kIsTcpDirect),
      poolSz_(kNumConnections),
      noMemCopy_(kNoMemoryCopy)
{
}

ZmqPerfRun::~ZmqPerfRun()
{
    for (auto pid : clients_) {
        kill(pid, SIGINT);
    }
}

void ZmqPerfRun::PrintHelp()
{
    std::cout << "Options:\n"
                 "    -h,--help               This help message.\n"
                 "    -n,--num_iterations     Set the number of iterations."
              << " Default " << kDefaultNumIterations << "\n"
              << "    -s,--payload_size       Set the payload size (in K)."
              << " Default " << kDefaultPayloadSz << "\n"
              << "    -k,--num_clients        Set the number of clients."
              << " Default " << kDefaultNumClients
              << "\n"
                 "    --hostname              Set the hostname of the server."
              << " Default " << hostName_
              << "\n"
                 "    --port                  Set the tcp/ip port of the server."
              << " Default " << kDefaultPort << "\n"
              << "    --local_client          Local client mode. "
              << " Default " << std::boolalpha << localClient_ << "\n"
              << "    --tcp_direct            Bypass proxy mode. "
              << " Default " << std::boolalpha << tcpDirect_ << "\n"
              << "    -c,--conn_pool_size     Socket pool size."
              << " Default " << kNumConnections << "\n"
              << "    --no_memcpy             Avoid memory copy on payload get."
              << " Default " << std::boolalpha << noMemCopy_ << "\n";
}

int ZmqPerfRun::ProcessArgsHelper(int opt)
{
    int32_t rc = 0;
    try {
        switch (opt) {
            case 'n': {
                numIterations_ = std::stoi(optarg);
                break;
            }
            case 's': {
                payloadSz_ = std::stoi(optarg);
                break;
            }

            case 'k': {
                numClients_ = std::stoi(optarg);
                break;
            }

            case 'c': {
                poolSz_ = std::stoi(optarg);
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

int ZmqPerfRun::ProcessArgs(int argc, char **argv)
{
    int isLocalClient = 0;
    int isTcpDirect = 0;
    int noMemCopy = 0;
    const char *const short_opts = ":n:s:k:c:hv";
    const option long_opts[] = { { "payload_size", required_argument, nullptr, 's' },
                                 { "num_iterations", required_argument, nullptr, 'n' },
                                 { "num_clients", required_argument, nullptr, 'k' },
                                 { "port", required_argument, nullptr, kPortOpt },
                                 { "hostname", required_argument, nullptr, kHostOpt },
                                 { "local_client", no_argument, &isLocalClient, 1 },
                                 { "tcp_direct", no_argument, &isTcpDirect, 1 },
                                 { "no_memcpy", no_argument, &noMemCopy, 1 },
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
                if (long_opts[optionIndex].flag == &isLocalClient) {
                    localClient_ = true;
                }
                if (long_opts[optionIndex].flag == &isTcpDirect) {
                    tcpDirect_ = true;
                }
                if (long_opts[optionIndex].flag == &noMemCopy) {
                    noMemCopy_ = true;
                }
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

std::string ZmqPerfRun::PrintSummary(const std::string &title, std::vector<int64_t> &lap) const
{
    // Two priority queues for calculating the median
    std::priority_queue<int64_t> left;
    std::priority_queue<int64_t, std::vector<int64_t>, std::greater<>> right;

    int64_t avgMs = 0;
    int64_t medMs = 0;
    int64_t maxMs = 0;
    int64_t minMs = std::numeric_limits<int64_t>::max();
    int64_t total = 0;
    for (auto num : lap) {
        total += num;
        maxMs = std::max<int64_t>(maxMs, num);
        minMs = std::min<int64_t>(minMs, num);
        // Put them into two equal size priority queues
        // to calculate the median
        if (left.empty() || num < left.top()) {
            left.push(num);
        } else {
            right.push(num);
        }
        if (left.size() > (right.size() + 1)) {
            auto t = left.top();
            left.pop();
            right.push(t);
        } else if (right.size() > (left.size() + 1)) {
            auto t = right.top();
            right.pop();
            left.push(t);
        }
    }
    // Calculate the median
    if (left.size() == right.size()) {
        medMs = (left.top() + right.top()) / 2;
    } else if (left.size() > right.size()) {
        medMs = left.top();
    } else {
        medMs = right.top();
    }
    avgMs = total / (int)lap.size();
    return FormatString("%s : Average %d Min %d Max %d Median %d\n", title, avgMs, minMs, maxMs, medMs);
}

Status ZmqPerfRun::StartAgent(int inx)
{
    (void)inx;
    char fullpath[PATH_MAX + 1] = { 0 };
    const std::string myself = "/proc/self/exe";
    if (realpath(myself.data(), fullpath) == nullptr) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_RUNTIME_ERROR,
                                FormatString("Unable to resolve %s. Errno = %d", myself, errno));
    }
    Uri uri(fullpath);
    std::string exe = FormatString("%s/%s", uri.GetParent(), "zmq_perf_agent");
    std::string hostnameArg = "--hostname";
    std::string portArg = "--port";
    std::string payloadArg = "--payload_size";
    std::string sockDirArg = "--unix_socket_dir";
    std::string poolSizeArg = "--connect_pool_size";
    std::string portStr = std::to_string(hostPort_);
    std::string payloadStr = std::to_string(payloadSz_);
    char *argv[12];
    argv[0] = const_cast<char *>(exe.data());
    argv[1] = const_cast<char *>(hostnameArg.data());
    argv[2] = const_cast<char *>(hostName_.data());
    argv[3] = const_cast<char *>(portArg.data());
    argv[4] = const_cast<char *>(portStr.data());
    argv[5] = const_cast<char *>(payloadArg.data());
    argv[6] = const_cast<char *>(payloadStr.data());
    argv[7] = const_cast<char *>(sockDirArg.data());
    argv[8] = const_cast<char *>(socketDir_.data());
    argv[9] = const_cast<char *>(poolSizeArg.data());
    argv[10] = const_cast<char *>(std::to_string(poolSz_).data());
    argv[11] = nullptr;
    execv(exe.data(), argv);
    RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Should not run here");
}

Status ZmqPerfRun::SpawnAgents()
{
    clients_.reserve(numClients_);
    int idx = 0;
    while (idx < numClients_) {
        pid_t pid = fork();
        if (pid < 0) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Unable to fork. Errno " + std::to_string(errno));
        } else if (pid > 0) {
            clients_.push_back(pid);
        } else {
            break;
        }
        ++idx;
    }
    if (idx < numClients_) {
        // Child continue here
        if (StartAgent(idx).IsError()) {
            _exit(-1);
        }
        _exit(0);
    }
    return Status::OK();
}

Status ZmqPerfRun::SimpleTestBatchSend(const std::string &title,
                                       std::vector<std::shared_ptr<PerfAgentService_Stub>> &stubs,
                                       std::vector<int64_t> &tags, AsyncWriteFunc writeFunc, bool hasPayload)
{
    std::stringstream oss;
    oss << FormatString("\n### %s ###\n", title);
    oss << FormatString("Number of clients %d. Number of messages per client %d\n", numClients_, numIterations_);
    if (hasPayload) {
        auto bufSz = payloadSz_ * kOneK;
        oss << "Sending " << bufSz << " bytes.\n";
    }
    stubs.reserve(numClients_);
    HostPort serverAddr(hostName_, hostPort_);
    for (auto i = 0; i < numClients_; ++i) {
        auto sockPath = FormatString("%s_%d", RpcChannel::UnixSocketPath(socketDir_, serverAddr), clients_[i]);
        auto channel = std::make_shared<RpcChannel>(sockPath, RpcCredential());
        auto stub = std::make_shared<PerfAgentService_Stub>(channel);
        int64_t tag = 0;
        PerfRunRqPb rq;
        rq.set_local_client(localClient_);
        rq.set_num_iterations(numIterations_);
        rq.set_tcp_direct(tcpDirect_);
        rq.set_no_memcpy(noMemCopy_);
        RETURN_IF_NOT_OK((stub.get()->*writeFunc)(rq, tag));
        stubs.push_back(std::move(stub));
        tags.push_back(tag);
    }
    std::cout << oss.str();
    return Status::OK();
}

Status ZmqPerfRun::SimpleTestBatchRecv(const std::string &title,
                                       const std::vector<std::shared_ptr<PerfAgentService_Stub>> &stubs,
                                       const std::vector<int64_t> &tags, AsyncReadFunc readFunc,
                                       int64_t &totalElapsedTime)
{
    const int totalRq = numClients_ * numIterations_;
    std::vector<int64_t> lap;
    std::vector<int64_t> elapsed;
    lap.reserve(totalRq);
    elapsed.reserve(totalRq);
    totalElapsedTime = 0;
    for (auto i = 0; i < numClients_; ++i) {
        auto &stub = stubs[i];
        auto tag = tags[i];
        PerfRunResPb reply;
        RETURN_IF_NOT_OK((stub.get()->*readFunc)(tag, reply, ZmqRecvFlags::NONE));
        for (int k = 0; k < numIterations_; k++) {
            lap.push_back(reply.time().lap(k));
            auto elapsedTime = reply.elapsed_time(k);
            elapsed.push_back(elapsedTime);
            totalElapsedTime += elapsedTime;
        }
    }
    std::cout << PrintSummary(FormatString("%s latency", title), lap);
    std::cout << PrintSummary(FormatString("%s elapsed time", title), elapsed);
    const int oneS = 1'000'000;
    float QPS = (float)totalRq / ((float)totalElapsedTime / (float)oneS);
    std::cout << FormatString("%s Throughput: %f requests per second\n", title, QPS);
    return Status::OK();
}

Status ZmqPerfRun::SimpleTest(const std::string &title, AsyncWriteFunc writeFunc, AsyncReadFunc readFunc,
                              bool hasPayload)
{
    std::vector<std::shared_ptr<PerfAgentService_Stub>> stubs;
    std::vector<int64_t> tags;
    int64_t totalElapsedTime = 0;
    RETURN_IF_NOT_OK(SimpleTestBatchSend(title, stubs, tags, writeFunc, hasPayload));
    RETURN_IF_NOT_OK(SimpleTestBatchRecv(title, stubs, tags, readFunc, totalElapsedTime));
    if (hasPayload) {
        float totalSzInGb = (float)(payloadSz_) * (float)numIterations_ * (float)numClients_ / (float)(kOneK * kOneK);
        const int oneS = 1'000'000;
        float throughPut = totalSzInGb / ((float)totalElapsedTime / (float)oneS);
        std::cout << FormatString("%s Throughput: %f gb per second\n", title, throughPut);
    }
    return Status::OK();
}

Status ZmqPerfRun::SimpleTickRun()
{
    const int totalRq = numClients_ * numIterations_;
    const std::string title = "[SimpleTickRun]";
    std::vector<std::shared_ptr<PerfAgentService_Stub>> stubs;
    std::vector<int64_t> tags;
    RETURN_IF_NOT_OK(
        SimpleTestBatchSend(title, stubs, tags, &PerfAgentService_Stub::AgentSimpleTickRunAsyncWrite, true));
    std::vector<int64_t> elapsed;
    // We will have some number of array of int64 coming back. But we don't know how many yet
    int64_t totalElapsedTime = 0;
    std::list<std::pair<std::string, std::vector<int64_t>>> tickNames;
    for (auto i = 0; i < numClients_; ++i) {
        auto &stub = stubs[i];
        auto tag = tags[i];
        PerfTickRunResPb reply;
        RETURN_IF_NOT_OK(stub->AgentSimpleTickRunAsyncRead(tag, reply, ZmqRecvFlags::NONE));
        // The number of tickNames can vary.
        int numOfTicks = reply.time_size();
        for (int j = 0; j < numOfTicks; ++j) {
            auto &name = reply.time(j).tick_name();
            // Find a matching tick name and preserve its order
            auto it =
                std::find_if(tickNames.begin(), tickNames.end(),
                             [&name](std::pair<std::string, std::vector<int64_t>> &e) { return e.first == name; });
            if (it == tickNames.end()) {
                it = tickNames.insert(tickNames.end(), std::make_pair(name, std::vector<int64_t>()));
            }
            std::vector<int64_t> &v = it->second;
            for (int k = 0; k < reply.time(j).lap_size(); ++k) {
                v.push_back(reply.time(j).lap(k));
            }
        }
        for (int k = 0; k < numIterations_; k++) {
            auto elapsedTime = reply.elapsed_time(k);
            elapsed.push_back(elapsedTime);
            totalElapsedTime += elapsedTime;
        }
    }
    for (auto &ele : tickNames) {
        std::cout << PrintSummary(FormatString("MetaPb.ticks[%s] latency", ele.first), ele.second);
    }
    std::cout << PrintSummary(FormatString("%s elapsed time", title), elapsed);
    const int oneS = 1'000'000;
    float QPS = (float)totalRq / ((float)totalElapsedTime / (float)oneS);
    float totalSzInGb = (float)(payloadSz_) * (float)numIterations_ * (float)numClients_ / (float)(kOneK * kOneK);
    float throughPut = totalSzInGb / ((float)totalElapsedTime / (float)oneS);
    std::cout << FormatString("%s Throughput: %f requests per second\n", title, QPS);
    std::cout << FormatString("%s Throughput: %f gb per second\n", title, throughPut);
    return Status::OK();
}

Status ZmqPerfRun::RunAllBenchmarks()
{
    socketDir_ = FormatString("%s/%s", "/tmp", GetStringUuid().substr(0, ZMQ_EIGHT));
    const int slpt = 5;
    std::cout << FormatString("Sleep for %d seconds for clients to start up", slpt) << std::endl;
    RETURN_IF_NOT_OK(SpawnAgents());
    // Wait a few seconds for the child to start up
    std::this_thread::sleep_for(std::chrono::seconds(slpt));
    RETURN_IF_NOT_OK(RunClientTest());
    // parent wait for all clients to come back.
    for (auto pid : clients_) {
        kill(pid, SIGINT);
        int wStatus;
        waitpid(pid, &wStatus, 0);
    }
    return Status::OK();
}

Status ZmqPerfRun::SimpleGreetingAsyncRun()
{
    return SimpleTest("[SimpleGreetingAsyncRun]", &PerfAgentService_Stub::AgentSimpleGreetingAsyncWrite,
                      &PerfAgentService_Stub::AgentSimpleGreetingAsyncRead, false);
}

Status ZmqPerfRun::UnarySocketSendPayloadRun()
{
    return SimpleTest("[UnarySocketSendPayloadRun]", &PerfAgentService_Stub::AgentUnarySocketSendPayloadRunAsyncWrite,
                      &PerfAgentService_Stub::AgentUnarySocketSendPayloadRunAsyncRead, true);
}

Status ZmqPerfRun::UnarySocketRecvPayloadRun()
{
    return SimpleTest("[UnarySocketRecvPayloadRun]", &PerfAgentService_Stub::AgentUnarySocketRecvPayloadRunAsyncWrite,
                      &PerfAgentService_Stub::AgentUnarySocketRecvPayloadRunAsyncRead, true);
}

Status ZmqPerfRun::UnarySocketSimpleGreetingRun()
{
    return SimpleTest("[UnarySocketSimpleGreetingRun]",
                      &PerfAgentService_Stub::AgentUnarySocketSimpleGreetingRunAsyncWrite,
                      &PerfAgentService_Stub::AgentUnarySocketSimpleGreetingRunAsyncRead, false);
}

Status ZmqPerfRun::DirectSendPayloadRun()
{
    return SimpleTest("[DirectSendPayloadRun]", &PerfAgentService_Stub::AgentDirectSendPayloadRunAsyncWrite,
                      &PerfAgentService_Stub::AgentDirectSendPayloadRunAsyncRead, true);
}

Status ZmqPerfRun::RunClientTest()
{
    RETURN_IF_NOT_OK(SimpleGreetingAsyncRun());
    RETURN_IF_NOT_OK(UnarySocketSendPayloadRun());
    RETURN_IF_NOT_OK(UnarySocketRecvPayloadRun());
    RETURN_IF_NOT_OK(UnarySocketSimpleGreetingRun());
    RETURN_IF_NOT_OK(DirectSendPayloadRun());
    RETURN_IF_NOT_OK(SimpleTickRun());
    RETURN_IF_NOT_OK(MessageQueRun());
    return Status::OK();
}

Status ZmqPerfRun::MessageQueRun() const
{
    const int totalRq = numClients_ * numIterations_;
    const std::string title = "[Queue MessageQueRun]";
    std::stringstream oss;
    oss << FormatString("\n### %s ###\n", title);
    oss << FormatString("Number of clients %d. Number of messages per client %d\n", numClients_, numIterations_);
    auto queMgr = std::make_shared<ZmqMsgMgr>();
    RETURN_IF_NOT_OK(queMgr->Init());
    auto consumer = [this, &queMgr]() {
        const int loop = numIterations_ * numClients_;
        int count = 0;
        while (count < loop) {
            ZmqMetaMsgFrames p;
            std::string qID;
            RETURN_IF_NOT_OK(queMgr->GetMsg(p, qID, QUE_NO_TIMEOUT));
            GetLapTime(p.first, "");
            RETURN_IF_NOT_OK(queMgr->SendMsg(qID, p, QUE_NO_TIMEOUT));
            ++count;
        }
        return Status::OK();
    };
    std::thread backend(consumer);
    backend.detach();

    auto producer = [this, &queMgr](std::vector<int64_t> &lap, std::vector<int64_t> &createQ,
                                    std::vector<int64_t> &elapsed) {
        ZmqMsgQueRef msgQ;
        auto startTick = std::chrono::steady_clock::now();
        RETURN_IF_NOT_OK(queMgr->CreateMsgQ(msgQ));
        auto endTick = std::chrono::steady_clock::now();
        auto elapsedTime = std::chrono::duration_cast<std::chrono::microseconds>(endTick - startTick).count();
        createQ.push_back(elapsedTime);
        for (auto i = 0; i < numIterations_; ++i) {
            startTick = std::chrono::steady_clock::now();
            ZmqMetaMsgFrames p;
            StartTheClock(p.first);
            RETURN_IF_NOT_OK(msgQ.SendMsg(p, QUE_NO_TIMEOUT));
            ZmqMetaMsgFrames reply;
            RETURN_IF_NOT_OK(msgQ.ReceiveMsg(reply, QUE_NO_TIMEOUT));
            endTick = std::chrono::steady_clock::now();
            elapsed.push_back(std::chrono::duration_cast<std::chrono::microseconds>(endTick - startTick).count());
            lap.push_back(GetTotalTime(reply.first) / NANO_TO_MICRO_UNIT);
        };
        return Status::OK();
    };
    std::vector<std::thread> clients;
    std::vector<std::vector<int64_t>> laps(numClients_);
    std::vector<std::vector<int64_t>> createQs(numClients_);
    std::vector<std::vector<int64_t>> elapsed(numClients_);
    clients.reserve(numClients_);
    for (int i = 0; i < numClients_; ++i) {
        laps[i] = std::vector<int64_t>(numIterations_, 0);
        createQs[i] = std::vector<int64_t>(numIterations_, 0);
        elapsed[i] = std::vector<int64_t>(numIterations_, 0);
    }
    for (int i = 0; i < numClients_; ++i) {
        auto f = std::bind(producer, std::ref(laps[i]), std::ref(createQs[i]), std::ref(elapsed[i]));
        clients.emplace_back(f);
    }
    for (int i = 0; i < numClients_; ++i) {
        clients[i].join();
    }
    queMgr->Stop();
    std::vector<int64_t> lap;
    lap.reserve(totalRq);
    for (auto &v : laps) {
        lap.insert(lap.end(), v.begin(), v.end());
    }
    oss << PrintSummary(FormatString("%s latency", title), lap);
    std::vector<int64_t> createQ;
    createQ.reserve(totalRq);
    for (auto &v : createQs) {
        createQ.insert(createQ.end(), v.begin(), v.end());
    }
    oss << PrintSummary("[CreateQ time]", createQ);
    // Calculate the average
    int64_t totalElapsedTime = 0;
    for (auto &v : elapsed) {
        for (auto &u : v) {
            totalElapsedTime += u;
        }
    }
    oss << FormatString("%s Average elapsed time per iteration: %d\n", title, totalElapsedTime / numIterations_);
    const int oneS = 1'000'000;
    float throughPut = (float)numIterations_ * (float)numClients_ / ((float)totalElapsedTime / (float)oneS);
    oss << FormatString("%s Throughput: %f requests per second\n", title, throughPut);
    std::cout << oss.str();
    return Status::OK();
}

}  // namespace st
}  // namespace datasystem
