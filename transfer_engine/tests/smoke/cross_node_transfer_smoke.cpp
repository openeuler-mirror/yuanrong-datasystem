#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <glog/logging.h>

#include "acl_test_utils.h"
#include "internal/log/logging.h"
#include "datasystem/transfer_engine/transfer_engine.h"

namespace datasystem {
namespace {

struct Options {
    std::string role;
    std::string localIp;
    uint16_t localPort = 0;
    int32_t deviceId = -1;
    uint64_t size = 0;
    uint64_t holdSeconds = 600;
    uint8_t pattern = 7;
    std::optional<uint64_t> srcAddr;
    std::vector<uint64_t> srcAddrs;
    uint32_t registerCount = 1;
    std::optional<uint64_t> dstAddr;
    bool verifyPattern = false;
    bool autoVerifyData = false;

    std::string peerIp;
    uint16_t peerPort = 0;
    uint64_t remoteAddr = 0;
    std::vector<uint64_t> remoteAddrs;
    int32_t peerDeviceBaseId = 0;

    uint32_t requesterCount = 1;
    uint16_t requesterPortStep = 1;
    int32_t requesterDeviceStep = 1;
};

void PrintUsage(const char *prog)
{
    std::cout
        << "Usage:\n"
        << "  Owner:\n"
        << "    " << prog
        << " --role owner --local-ip <ip> --local-port <port> --device-id <id> --size <bytes>\n"
        << "       [--pattern <0-255>] [--hold-seconds <sec>]\n"
        << "       [--register-count <n>] [--src-addr <hex|dec>] [--src-addrs <a0,a1,...>]\n"
        << "  Requester:\n"
        << "    " << prog
        << " --role requester --local-ip <ip> --local-port <port> --device-id <id> --size <bytes>\n"
        << "       --peer-ip <ip> --peer-port <port> [--peer-device-base-id <id>]\n"
        << "       --remote-addrs <a0,a1,...> | --remote-addr <a0>\n"
        << "       [--dst-addr <hex|dec>] [--verify-pattern --pattern <0-255>] [--auto-verify-data]\n"
        << "       [--requester-count <n>] [--requester-port-step <n>] [--requester-device-step <n>]\n"
        << "       注意：requester 会统一走 BatchTransferSyncRead，batch 大小=remote-addrs 个数。\n";
}

bool ParseUint64(const std::string &in, uint64_t *out)
{
    if (out == nullptr || in.empty()) {
        return false;
    }
    char *end = nullptr;
    errno = 0;
    unsigned long long v = std::strtoull(in.c_str(), &end, 0);
    if (errno != 0 || end == nullptr || *end != '\0') {
        return false;
    }
    *out = static_cast<uint64_t>(v);
    return true;
}

bool ParseInt32(const std::string &in, int32_t *out)
{
    if (out == nullptr || in.empty()) {
        return false;
    }
    char *end = nullptr;
    errno = 0;
    long v = std::strtol(in.c_str(), &end, 10);
    if (errno != 0 || end == nullptr || *end != '\0' || v < std::numeric_limits<int32_t>::min() ||
        v > std::numeric_limits<int32_t>::max()) {
        return false;
    }
    *out = static_cast<int32_t>(v);
    return true;
}

bool ParseUint64List(const std::string &in, std::vector<uint64_t> *out)
{
    if (out == nullptr || in.empty()) {
        return false;
    }
    out->clear();
    size_t start = 0;
    while (start <= in.size()) {
        const size_t comma = in.find(',', start);
        const std::string item = in.substr(start, comma == std::string::npos ? std::string::npos : (comma - start));
        if (item.empty()) {
            return false;
        }
        uint64_t v = 0;
        if (!ParseUint64(item, &v) || v == 0) {
            return false;
        }
        out->push_back(v);
        if (comma == std::string::npos) {
            break;
        }
        start = comma + 1;
    }
    return !out->empty();
}

bool ParseArgs(int argc, char **argv, Options *opt, std::string *err)
{
    if (opt == nullptr || err == nullptr) {
        return false;
    }

    for (int i = 1; i < argc; ++i) {
        const std::string key = argv[i];
        auto needValue = [&](std::string *value) -> bool {
            if (i + 1 >= argc) {
                *err = "missing value for " + key;
                return false;
            }
            *value = argv[++i];
            return true;
        };

        if (key == "--verify-pattern") {
            opt->verifyPattern = true;
            continue;
        }
        if (key == "--auto-verify-data") {
            opt->autoVerifyData = true;
            continue;
        }

        std::string value;
        if (!needValue(&value)) {
            return false;
        }

        if (key == "--role") {
            opt->role = value;
        } else if (key == "--local-ip") {
            opt->localIp = value;
        } else if (key == "--local-port") {
            uint64_t v = 0;
            if (!ParseUint64(value, &v) || v == 0 || v > 65535) {
                *err = "invalid --local-port";
                return false;
            }
            opt->localPort = static_cast<uint16_t>(v);
        } else if (key == "--device-id") {
            int32_t v = -1;
            if (!ParseInt32(value, &v) || v < 0) {
                *err = "invalid --device-id";
                return false;
            }
            opt->deviceId = v;
        } else if (key == "--size") {
            if (!ParseUint64(value, &opt->size) || opt->size == 0) {
                *err = "invalid --size";
                return false;
            }
        } else if (key == "--hold-seconds") {
            if (!ParseUint64(value, &opt->holdSeconds)) {
                *err = "invalid --hold-seconds";
                return false;
            }
        } else if (key == "--pattern") {
            uint64_t v = 0;
            if (!ParseUint64(value, &v) || v > 255) {
                *err = "invalid --pattern";
                return false;
            }
            opt->pattern = static_cast<uint8_t>(v);
        } else if (key == "--src-addr") {
            uint64_t v = 0;
            if (!ParseUint64(value, &v) || v == 0) {
                *err = "invalid --src-addr";
                return false;
            }
            opt->srcAddr = v;
        } else if (key == "--src-addrs") {
            if (!ParseUint64List(value, &opt->srcAddrs)) {
                *err = "invalid --src-addrs";
                return false;
            }
        } else if (key == "p") {
            uint64_t v = 0;
            if (!ParseUint64(value, &v) || v == 0 || v > 1024) {
                *err = "invalid --register-count";
                return false;
            }
            opt->registerCount = static_cast<uint32_t>(v);
        } else if (key == "--dst-addr") {
            uint64_t v = 0;
            if (!ParseUint64(value, &v) || v == 0) {
                *err = "invalid --dst-addr";
                return false;
            }
            opt->dstAddr = v;
        } else if (key == "--peer-ip") {
            opt->peerIp = value;
        } else if (key == "--peer-port") {
            uint64_t v = 0;
            if (!ParseUint64(value, &v) || v == 0 || v > 65535) {
                *err = "invalid --peer-port";
                return false;
            }
            opt->peerPort = static_cast<uint16_t>(v);
        } else if (key == "--remote-addr") {
            if (!ParseUint64(value, &opt->remoteAddr) || opt->remoteAddr == 0) {
                *err = "invalid --remote-addr";
                return false;
            }
        } else if (key == "--remote-addrs") {
            if (!ParseUint64List(value, &opt->remoteAddrs)) {
                *err = "invalid --remote-addrs";
                return false;
            }
        } else if (key == "--peer-device-base-id") {
            int32_t v = -1;
            if (!ParseInt32(value, &v) || v < 0) {
                *err = "invalid --peer-device-base-id";
                return false;
            }
            opt->peerDeviceBaseId = v;
        } else if (key == "--requester-count") {
            uint64_t v = 0;
            if (!ParseUint64(value, &v) || v == 0 || v > 512) {
                *err = "invalid --requester-count";
                return false;
            }
            opt->requesterCount = static_cast<uint32_t>(v);
        } else if (key == "--requester-port-step") {
            uint64_t v = 0;
            if (!ParseUint64(value, &v) || v == 0 || v > 65535) {
                *err = "invalid --requester-port-step";
                return false;
            }
            opt->requesterPortStep = static_cast<uint16_t>(v);
        } else if (key == "--requester-device-step") {
            int32_t v = 0;
            if (!ParseInt32(value, &v) || v <= 0) {
                *err = "invalid --requester-device-step";
                return false;
            }
            opt->requesterDeviceStep = v;
        } else {
            *err = "unknown argument: " + key;
            return false;
        }
    }

    if (opt->role != "owner" && opt->role != "requester") {
        *err = "role must be owner or requester";
        return false;
    }
    if (opt->localIp.empty() || opt->localPort == 0 || opt->deviceId < 0 || opt->size == 0) {
        *err = "missing required local arguments";
        return false;
    }

    if (opt->role == "owner") {
        if (opt->srcAddr.has_value() && !opt->srcAddrs.empty()) {
            *err = "--src-addr conflicts with --src-addrs";
            return false;
        }
        if (!opt->srcAddrs.empty() && opt->registerCount != opt->srcAddrs.size()) {
            *err = "--register-count should equal --src-addrs count";
            return false;
        }
    } else {
        if (opt->peerIp.empty() || opt->peerPort == 0) {
            *err = "requester missing --peer-ip/--peer-port";
            return false;
        }
        if (opt->remoteAddrs.empty()) {
            if (opt->remoteAddr == 0) {
                *err = "requester missing --remote-addrs or --remote-addr";
                return false;
            }
            opt->remoteAddrs.push_back(opt->remoteAddr);
        }
        if (opt->dstAddr.has_value() && opt->remoteAddrs.size() != 1) {
            *err = "--dst-addr only supports single remote addr batch";
            return false;
        }
        if (opt->autoVerifyData && opt->verifyPattern) {
            *err = "--auto-verify-data conflicts with --verify-pattern";
            return false;
        }
    }

    return true;
}

Result InitAclAndDevice(int32_t deviceId)
{
    Result rc = testutil::EnsureAclInitialized();
    if (rc.IsError()) {
        return rc;
    }
    return testutil::SetAclDevice(deviceId);
}

Result RunOwner(const Options &opt)
{
    Result rc = InitAclAndDevice(opt.deviceId);
    if (rc.IsError()) {
        return rc;
    }

    TransferEngine engine;
    rc = engine.Initialize(opt.localIp + ":" + std::to_string(opt.localPort), "ascend",
                           "npu:" + std::to_string(opt.deviceId));
    if (rc.IsError()) {
        return rc;
    }

    const uint32_t registerCount = !opt.srcAddrs.empty() ? static_cast<uint32_t>(opt.srcAddrs.size()) :
                                   (opt.srcAddr.has_value() ? 1U : opt.registerCount);
    std::vector<void *> srcDevs;
    std::vector<bool> ownAllocs;
    std::vector<uintptr_t> bufferAddrs;
    std::vector<size_t> lengths;
    srcDevs.reserve(registerCount);
    ownAllocs.reserve(registerCount);
    bufferAddrs.reserve(registerCount);
    lengths.reserve(registerCount);

    auto cleanup = [&]() {
        (void)engine.Finalize();
        for (size_t i = 0; i < srcDevs.size(); ++i) {
            if (ownAllocs[i]) {
                (void)InitAclAndDevice(opt.deviceId);
                (void)testutil::AclFree(srcDevs[i]);
            }
        }
    };

    for (uint32_t i = 0; i < registerCount; ++i) {
        void *srcDev = nullptr;
        bool ownAlloc = false;
        if (!opt.srcAddrs.empty()) {
            srcDev = reinterpret_cast<void *>(opt.srcAddrs[i]);
        } else if (opt.srcAddr.has_value()) {
            srcDev = reinterpret_cast<void *>(opt.srcAddr.value());
        } else {
            rc = testutil::AclMalloc(static_cast<size_t>(opt.size), &srcDev);
            if (rc.IsError()) {
                cleanup();
                return rc;
            }
            ownAlloc = true;
            const uint8_t fill = static_cast<uint8_t>(opt.deviceId + 1);
            std::vector<uint8_t> host(opt.size, fill);
            rc = testutil::AclMemcpy(srcDev, static_cast<size_t>(opt.size), host.data(), host.size(), 1);
            if (rc.IsError()) {
                if (ownAlloc) {
                    (void)testutil::AclFree(srcDev);
                }
                cleanup();
                return rc;
            }
        }
        srcDevs.push_back(srcDev);
        ownAllocs.push_back(ownAlloc);
        bufferAddrs.push_back(reinterpret_cast<uintptr_t>(srcDev));
        lengths.push_back(static_cast<size_t>(opt.size));
    }

    rc = engine.BatchRegisterMemory(bufferAddrs, lengths);
    if (rc.IsError()) {
        cleanup();
        return rc;
    }

    std::ostringstream remoteAddrsOss;
    for (size_t i = 0; i < bufferAddrs.size(); ++i) {
        if (i > 0) {
            remoteAddrsOss << ",";
        }
        remoteAddrsOss << "0x" << std::hex << bufferAddrs[i] << std::dec;
    }

    std::cout << "[OWNER_READY] local_ip=" << opt.localIp
              << " local_port=" << opt.localPort
              << " device_id=" << opt.deviceId
              << " register_count=" << bufferAddrs.size()
              << " first_src_addr=0x" << std::hex << bufferAddrs.front() << std::dec
              << " size=" << opt.size
              << " fill=" << static_cast<int>(static_cast<uint8_t>(opt.deviceId + 1)) << std::endl;
    std::cout << "[OWNER_READY_FOR_REQUESTER] --peer-ip " << opt.localIp << " --peer-port " << opt.localPort
              << " --peer-device-base-id " << opt.deviceId << " --remote-addrs " << remoteAddrsOss.str() << std::endl;
    std::cout.flush();

    if (opt.holdSeconds == 0) {
        std::cout << "Press ENTER to exit owner..." << std::endl;
        std::string dummy;
        std::getline(std::cin, dummy);
    } else {
        std::this_thread::sleep_for(std::chrono::seconds(opt.holdSeconds));
    }

    cleanup();
    return Result::OK();
}

Result RunRequester(const Options &opt)
{
    Result rc = InitAclAndDevice(opt.deviceId);
    if (rc.IsError()) {
        return rc;
    }

    TransferEngine engine;
    rc = engine.Initialize(opt.localIp + ":" + std::to_string(opt.localPort), "ascend",
                           "npu:" + std::to_string(opt.deviceId));
    if (rc.IsError()) {
        return rc;
    }

    std::vector<void *> dstDevs;
    std::vector<bool> ownAllocs;
    dstDevs.reserve(opt.remoteAddrs.size());
    ownAllocs.reserve(opt.remoteAddrs.size());

    for (size_t i = 0; i < opt.remoteAddrs.size(); ++i) {
        void *dstDev = nullptr;
        bool ownAlloc = false;
        if (i == 0 && opt.dstAddr.has_value()) {
            dstDev = reinterpret_cast<void *>(opt.dstAddr.value());
        } else {
            rc = testutil::AclMalloc(static_cast<size_t>(opt.size), &dstDev);
            if (rc.IsError()) {
                for (size_t k = 0; k < dstDevs.size(); ++k) {
                    if (ownAllocs[k]) {
                        (void)testutil::AclFree(dstDevs[k]);
                    }
                }
                (void)engine.Finalize();
                return rc;
            }
            ownAlloc = true;
        }
        dstDevs.push_back(dstDev);
        ownAllocs.push_back(ownAlloc);
    }

    std::vector<uintptr_t> buffers;
    std::vector<uintptr_t> peerBufferAddresses;
    std::vector<size_t> lengths;
    buffers.reserve(opt.remoteAddrs.size());
    peerBufferAddresses.reserve(opt.remoteAddrs.size());
    lengths.reserve(opt.remoteAddrs.size());
    for (size_t i = 0; i < opt.remoteAddrs.size(); ++i) {
        buffers.push_back(reinterpret_cast<uintptr_t>(dstDevs[i]));
        peerBufferAddresses.push_back(static_cast<uintptr_t>(opt.remoteAddrs[i]));
        lengths.push_back(static_cast<size_t>(opt.size));
    }

    const std::string targetHostname = opt.peerIp + ":" + std::to_string(opt.peerPort);
    Result readRc = engine.BatchTransferSyncRead(targetHostname, buffers, peerBufferAddresses, lengths);
    if (readRc.IsError()) {
        for (size_t i = 0; i < dstDevs.size(); ++i) {
            if (ownAllocs[i]) {
                (void)testutil::AclFree(dstDevs[i]);
            }
        }
        (void)engine.Finalize();
        return readRc;
    }

    const uint8_t autoExpected = static_cast<uint8_t>(opt.peerDeviceBaseId + 1);
    for (size_t i = 0; i < dstDevs.size(); ++i) {
        std::vector<uint8_t> host(opt.size, 0);
        rc = testutil::AclMemcpy(host.data(), host.size(), dstDevs[i], static_cast<size_t>(opt.size), 2);
        if (rc.IsError()) {
            for (size_t k = 0; k < dstDevs.size(); ++k) {
                if (ownAllocs[k]) {
                    (void)testutil::AclFree(dstDevs[k]);
                }
            }
            (void)engine.Finalize();
            return rc;
        }

        if (opt.autoVerifyData) {
            for (size_t j = 0; j < host.size(); ++j) {
                if (host[j] != autoExpected) {
                    for (size_t k = 0; k < dstDevs.size(); ++k) {
                        if (ownAllocs[k]) {
                            (void)testutil::AclFree(dstDevs[k]);
                        }
                    }
                    (void)engine.Finalize();
                    return Result(ErrorCode::kRuntimeError,
                                  "auto verify failed at batch=" + std::to_string(i) + ", index=" +
                                      std::to_string(j) + ", got=" + std::to_string(host[j]) +
                                      ", expected=" + std::to_string(autoExpected));
                }
            }
        } else if (opt.verifyPattern) {
            for (size_t j = 0; j < host.size(); ++j) {
                if (host[j] != opt.pattern) {
                    for (size_t k = 0; k < dstDevs.size(); ++k) {
                        if (ownAllocs[k]) {
                            (void)testutil::AclFree(dstDevs[k]);
                        }
                    }
                    (void)engine.Finalize();
                    return Result(ErrorCode::kRuntimeError,
                                  "verify pattern failed at batch=" + std::to_string(i) + ", index=" +
                                      std::to_string(j) + ", got=" + std::to_string(host[j]) +
                                      ", expected=" + std::to_string(opt.pattern));
                }
            }
        }

        std::ostringstream oss;
        oss << "[REQUESTER_BATCH_ITEM_DONE] idx=" << i << " remote=0x" << std::hex << opt.remoteAddrs[i] << std::dec
            << " bytes=" << opt.size << " first16=";
        const size_t n = std::min<size_t>(16, host.size());
        for (size_t j = 0; j < n; ++j) {
            oss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(host[j]);
            if (j + 1 < n) {
                oss << " ";
            }
        }
        std::cout << oss.str() << std::dec << std::endl;
    }

    std::cout << "[REQUESTER_BATCH_DONE] batch_size=" << buffers.size() << " bytes_each=" << opt.size << std::endl;

    for (size_t i = 0; i < dstDevs.size(); ++i) {
        if (ownAllocs[i]) {
            (void)testutil::AclFree(dstDevs[i]);
        }
    }
    (void)engine.Finalize();
    return Result::OK();
}

Result RunRequesterConcurrent(const Options &opt)
{
    if (opt.requesterCount <= 1) {
        return RunRequester(opt);
    }

    std::vector<pid_t> pids;
    pids.reserve(opt.requesterCount);

    for (uint32_t i = 0; i < opt.requesterCount; ++i) {
        const pid_t pid = fork();
        if (pid < 0) {
            return Result(ErrorCode::kRuntimeError, "fork requester failed at index " + std::to_string(i));
        }
        if (pid == 0) {
            Options worker = opt;
            worker.requesterCount = 1;
            worker.deviceId = opt.deviceId + static_cast<int32_t>(i) * opt.requesterDeviceStep;
            const uint32_t workerPort = static_cast<uint32_t>(opt.localPort) + i * static_cast<uint32_t>(opt.requesterPortStep);
            if (workerPort == 0 || workerPort > 65535) {
                _exit(101);
            }
            worker.localPort = static_cast<uint16_t>(workerPort);
            worker.dstAddr.reset();

            Result rc = RunRequester(worker);
            if (rc.IsError()) {
                LOG(ERROR) << "concurrent requester worker failed, index=" << i
                           << ", device_id=" << worker.deviceId
                           << ", local_port=" << worker.localPort
                           << ", detail=" << rc.ToString();
                _exit(1);
            }
            _exit(0);
        }
        pids.push_back(pid);
    }

    bool allOk = true;
    std::ostringstream errStream;
    for (pid_t pid : pids) {
        int status = 0;
        if (waitpid(pid, &status, 0) < 0) {
            allOk = false;
            errStream << "[pid=" << pid << " waitpid_failed] ";
            continue;
        }
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            allOk = false;
            errStream << "[pid=" << pid << " exit=" << (WIFEXITED(status) ? WEXITSTATUS(status) : -1) << "] ";
        }
    }

    if (!allOk) {
        return Result(ErrorCode::kRuntimeError, "concurrent requester failed: " + errStream.str());
    }

    LOG(INFO) << "concurrent requester succeeded, workers=" << opt.requesterCount
              << ", peer=" << opt.peerIp << ":" << opt.peerPort
              << ", batch_size=" << opt.remoteAddrs.size();
    return Result::OK();
}

}  // namespace
}  // namespace datasystem

int main(int argc, char **argv)
{
    using namespace datasystem;
    internal::EnsureGlogInitialized(argv[0]);

    Options opt;
    std::string err;
    if (!ParseArgs(argc, argv, &opt, &err)) {
        LOG(ERROR) << "Argument error: " << err;
        PrintUsage(argv[0]);
        return 2;
    }

    Result rc = (opt.role == "owner") ? RunOwner(opt) : RunRequesterConcurrent(opt);
    if (rc.IsError()) {
        LOG(ERROR) << "Failed: " << rc.ToString();
        return 1;
    }
    return 0;
}
