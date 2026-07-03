#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cerrno>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include "acl_test_utils.h"
#include "internal/log/logging.h"
#include "datasystem/transfer_engine/transfer_engine.h"

namespace datasystem {
namespace {

volatile std::sig_atomic_t g_stopRequested = 0;

constexpr uint64_t K_MAX_TCP_PORT = 65535;
constexpr uint64_t K_MAX_REGISTER_COUNT = 65536;
constexpr uint64_t K_MAX_REQUESTER_COUNT = 512;
constexpr uint64_t K_MAX_PERF_SAMPLE_COUNT = 100000;
constexpr uint64_t K_OWNER_POLL_INTERVAL_MS = 200;
constexpr double K_NS_PER_MS = 1000000.0;
constexpr double K_NS_PER_SECOND = 1000000000.0;
constexpr double K_BYTES_PER_KIB = 1024.0;
constexpr int32_t K_ELAPSED_MS_PRECISION = 3;
constexpr int32_t K_THROUGHPUT_PRECISION = 6;
constexpr int32_t K_ACL_MEMCPY_HOST_TO_DEVICE = 1;
constexpr int32_t K_ACL_MEMCPY_DEVICE_TO_HOST = 2;
constexpr int K_OPTION_KEY_VALUE_ARG_COUNT = 2;

void HandleStopSignal(int)
{
    g_stopRequested = 1;
}

void InstallOwnerStopHandlers()
{
    (void)std::signal(SIGTERM, HandleStopSignal);
    (void)std::signal(SIGINT, HandleStopSignal);
}

bool StopRequested()
{
    return g_stopRequested != 0;
}

template <typename PointerT>
uintptr_t PtrToAddr(PointerT ptr)
{
    static_assert(std::is_pointer<PointerT>::value, "PtrToAddr expects a pointer");
    static_assert(sizeof(uintptr_t) == sizeof(void *), "uintptr_t must hold a device pointer");
    uintptr_t addr = 0;
    std::memcpy(&addr, &ptr, sizeof(addr));
    return addr;
}

void *AddrToPtr(uintptr_t addr)
{
    static_assert(sizeof(uintptr_t) == sizeof(void *), "uintptr_t must hold a device pointer");
    void *ptr = nullptr;
    std::memcpy(&ptr, &addr, sizeof(addr));
    return ptr;
}

struct DeviceAllocation {
    void *addr = nullptr;
    bool owned = false;
};

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
    bool skipReadback = false;
    bool contiguousBatch = false;
    uint32_t perfRepeats = 1;
    uint32_t perfWarmup = 0;

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
        << "       [--pattern <0-255>] [--hold-seconds <sec>] [--contiguous-batch]\n"
        << "       [--register-count <n>] [--src-addr <hex|dec>] [--src-addrs <a0,a1,...>]\n"
        << "  Requester:\n"
        << "    " << prog
        << " --role requester --local-ip <ip> --local-port <port> --device-id <id> --size <bytes>\n"
        << "       --peer-ip <ip> --peer-port <port> [--peer-device-base-id <id>]\n"
        << "       --remote-addrs <a0,a1,...> | --remote-addr <a0>\n"
        << "       [--dst-addr <hex|dec>] [--verify-pattern --pattern <0-255>] [--auto-verify-data]\n"
        << "       [--skip-readback] [--contiguous-batch] [--perf-warmup <n>] [--perf-repeats <n>]\n"
        << "       [--requester-count <n>] [--requester-port-step <n>] [--requester-device-step <n>]\n"
        << "       Note: requester performs one BatchTransferSyncRead; batch size equals remote-addrs count.\n";
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

bool CheckedMul(uint64_t left, uint64_t right, uint64_t *out)
{
    if (out == nullptr) {
        return false;
    }
    if (right != 0 && left > std::numeric_limits<uint64_t>::max() / right) {
        return false;
    }
    *out = left * right;
    return true;
}

bool ParseArgs(int argc, char **argv, Options *opt, std::string *err)
{
    if (opt == nullptr || err == nullptr) {
        return false;
    }

    int i = 1;
    while (i < argc) {
        const std::string key = argv[i];
        auto needValue = [&err, i, argc, argv, &key](std::string &value) -> bool {
            if (i + 1 >= argc) {
                *err = "missing value for " + key;
                return false;
            }
            value = argv[i + 1];
            return true;
        };

        if (key == "--verify-pattern") {
            opt->verifyPattern = true;
            ++i;
            continue;
        }
        if (key == "--auto-verify-data") {
            opt->autoVerifyData = true;
            ++i;
            continue;
        }
        if (key == "--skip-readback") {
            opt->skipReadback = true;
            ++i;
            continue;
        }
        if (key == "--contiguous-batch") {
            opt->contiguousBatch = true;
            ++i;
            continue;
        }

        std::string value;
        if (!needValue(value)) {
            return false;
        }
        i += K_OPTION_KEY_VALUE_ARG_COUNT;

        if (key == "--role") {
            opt->role = value;
        } else if (key == "--local-ip") {
            opt->localIp = value;
        } else if (key == "--local-port") {
            uint64_t v = 0;
            if (!ParseUint64(value, &v) || v == 0 || v > K_MAX_TCP_PORT) {
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
        } else if (key == "--register-count" || key == "-p") {
            uint64_t v = 0;
            if (!ParseUint64(value, &v) || v == 0 || v > K_MAX_REGISTER_COUNT) {
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
            if (!ParseUint64(value, &v) || v == 0 || v > K_MAX_TCP_PORT) {
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
            if (!ParseUint64(value, &v) || v == 0 || v > K_MAX_REQUESTER_COUNT) {
                *err = "invalid --requester-count";
                return false;
            }
            opt->requesterCount = static_cast<uint32_t>(v);
        } else if (key == "--requester-port-step") {
            uint64_t v = 0;
            if (!ParseUint64(value, &v) || v == 0 || v > K_MAX_TCP_PORT) {
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
        } else if (key == "--perf-repeats") {
            uint64_t v = 0;
            if (!ParseUint64(value, &v) || v == 0 || v > K_MAX_PERF_SAMPLE_COUNT) {
                *err = "invalid --perf-repeats";
                return false;
            }
            opt->perfRepeats = static_cast<uint32_t>(v);
        } else if (key == "--perf-warmup") {
            uint64_t v = 0;
            if (!ParseUint64(value, &v) || v > K_MAX_PERF_SAMPLE_COUNT) {
                *err = "invalid --perf-warmup";
                return false;
            }
            opt->perfWarmup = static_cast<uint32_t>(v);
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
        if (opt->contiguousBatch && (opt->srcAddr.has_value() || !opt->srcAddrs.empty())) {
            *err = "--contiguous-batch conflicts with explicit source addresses";
            return false;
        }
        if (!opt->srcAddrs.empty() && opt->registerCount != opt->srcAddrs.size()) {
            *err = "--register-count should equal --src-addrs count";
            return false;
        }
        uint64_t ignored = 0;
        if (opt->contiguousBatch && !CheckedMul(opt->size, opt->registerCount, &ignored)) {
            *err = "--size * --register-count overflow";
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
        if (opt->contiguousBatch && opt->dstAddr.has_value()) {
            *err = "--contiguous-batch conflicts with --dst-addr";
            return false;
        }
        uint64_t ignored = 0;
        if (opt->contiguousBatch && !CheckedMul(opt->size, opt->remoteAddrs.size(), &ignored)) {
            *err = "--size * remote address count overflow";
            return false;
        }
        if (opt->autoVerifyData && opt->verifyPattern) {
            *err = "--auto-verify-data conflicts with --verify-pattern";
            return false;
        }
        if (opt->skipReadback && (opt->autoVerifyData || opt->verifyPattern)) {
            *err = "--skip-readback conflicts with data verification";
            return false;
        }
        if (opt->perfWarmup > std::numeric_limits<uint32_t>::max() - opt->perfRepeats) {
            *err = "--perf-warmup + --perf-repeats overflow";
            return false;
        }
        if (opt->requesterCount > 1 && (opt->perfWarmup != 0 || opt->perfRepeats != 1)) {
            *err = "--perf-warmup/--perf-repeats only support single requester process";
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

template <typename PointerT>
Result FillDeviceBuffer(PointerT dstDev, uint64_t size, uint8_t fill)
{
    static_assert(std::is_pointer<PointerT>::value, "FillDeviceBuffer expects a pointer");
    std::vector<uint8_t> host(static_cast<size_t>(size), fill);
    return testutil::AclMemcpy(dstDev, static_cast<size_t>(size), host.data(), host.size(),
                               K_ACL_MEMCPY_HOST_TO_DEVICE);
}

Result PrepareOwnerSourceBuffer(const Options &opt, uint32_t index, DeviceAllocation &allocation)
{
    allocation = {};
    if (!opt.srcAddrs.empty()) {
        allocation.addr = AddrToPtr(static_cast<uintptr_t>(opt.srcAddrs[index]));
        return Result::OK();
    }
    if (opt.srcAddr.has_value()) {
        allocation.addr = AddrToPtr(static_cast<uintptr_t>(opt.srcAddr.value()));
        return Result::OK();
    }

    Result rc = testutil::AclMalloc(static_cast<size_t>(opt.size), &allocation.addr);
    if (rc.IsError()) {
        return rc;
    }
    allocation.owned = true;
    rc = FillDeviceBuffer(allocation.addr, opt.size, static_cast<uint8_t>(opt.deviceId + 1));
    if (rc.IsError()) {
        (void)testutil::AclFree(allocation.addr);
        allocation = {};
    }
    return rc;
}

Result PrepareRequesterDestinationBuffer(const Options &opt, size_t index, DeviceAllocation &allocation)
{
    allocation = {};
    if (index == 0 && opt.dstAddr.has_value()) {
        allocation.addr = AddrToPtr(static_cast<uintptr_t>(opt.dstAddr.value()));
        return Result::OK();
    }

    Result rc = testutil::AclMalloc(static_cast<size_t>(opt.size), &allocation.addr);
    if (rc.IsOk()) {
        allocation.owned = true;
    }
    return rc;
}

Result RunOwner(const Options &opt)
{
    InstallOwnerStopHandlers();

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
    std::vector<void *> allocAddrs;
    std::vector<bool> ownAllocs;
    std::vector<uintptr_t> advertisedAddrs;
    std::vector<uintptr_t> registeredAddrs;
    std::vector<size_t> registeredLengths;
    allocAddrs.reserve(opt.contiguousBatch ? 1U : registerCount);
    ownAllocs.reserve(opt.contiguousBatch ? 1U : registerCount);
    advertisedAddrs.reserve(registerCount);
    registeredAddrs.reserve(opt.contiguousBatch ? 1U : registerCount);
    registeredLengths.reserve(opt.contiguousBatch ? 1U : registerCount);

    auto cleanup = [&engine, &allocAddrs, &ownAllocs, &opt]() {
        (void)engine.Finalize();
        for (size_t i = 0; i < allocAddrs.size(); ++i) {
            if (ownAllocs[i]) {
                (void)InitAclAndDevice(opt.deviceId);
                (void)testutil::AclFree(allocAddrs[i]);
            }
        }
    };

    if (opt.contiguousBatch) {
        uint64_t totalBytes = 0;
        if (!CheckedMul(opt.size, registerCount, &totalBytes) ||
            totalBytes > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
            cleanup();
            return Result(ErrorCode::kInvalid, "contiguous owner allocation size overflow");
        }
        void *srcDev = nullptr;
        rc = testutil::AclMalloc(static_cast<size_t>(totalBytes), &srcDev);
        if (rc.IsError()) {
            cleanup();
            return rc;
        }
        const uint8_t fill = static_cast<uint8_t>(opt.deviceId + 1);
        std::vector<uint8_t> host(static_cast<size_t>(totalBytes), fill);
        rc = testutil::AclMemcpy(srcDev, static_cast<size_t>(totalBytes), host.data(), host.size(),
                                 K_ACL_MEMCPY_HOST_TO_DEVICE);
        if (rc.IsError()) {
            (void)testutil::AclFree(srcDev);
            cleanup();
            return rc;
        }
        allocAddrs.push_back(srcDev);
        ownAllocs.push_back(true);
        const uintptr_t base = PtrToAddr(srcDev);
        registeredAddrs.push_back(base);
        registeredLengths.push_back(static_cast<size_t>(totalBytes));
        for (uint32_t i = 0; i < registerCount; ++i) {
            uint64_t offset = 0;
            (void)CheckedMul(opt.size, i, &offset);
            advertisedAddrs.push_back(base + static_cast<uintptr_t>(offset));
        }
    } else {
        for (uint32_t i = 0; i < registerCount; ++i) {
            DeviceAllocation source;
            rc = PrepareOwnerSourceBuffer(opt, i, source);
            if (rc.IsError()) {
                cleanup();
                return rc;
            }
            allocAddrs.push_back(source.addr);
            ownAllocs.push_back(source.owned);
            advertisedAddrs.push_back(PtrToAddr(source.addr));
            registeredAddrs.push_back(PtrToAddr(source.addr));
            registeredLengths.push_back(static_cast<size_t>(opt.size));
        }
    }

    rc = engine.BatchRegisterMemory(registeredAddrs, registeredLengths);
    if (rc.IsError()) {
        cleanup();
        return rc;
    }

    std::ostringstream remoteAddrsOss;
    for (size_t i = 0; i < advertisedAddrs.size(); ++i) {
        if (i > 0) {
            remoteAddrsOss << ",";
        }
        remoteAddrsOss << "0x" << std::hex << advertisedAddrs[i] << std::dec;
    }

    std::cout << "[OWNER_READY] local_ip=" << opt.localIp
              << " local_port=" << opt.localPort
              << " device_id=" << opt.deviceId
              << " register_count=" << advertisedAddrs.size()
              << " first_src_addr=0x" << std::hex << advertisedAddrs.front() << std::dec
              << " size=" << opt.size
              << " contiguous_batch=" << (opt.contiguousBatch ? 1 : 0)
              << " fill=" << static_cast<int>(static_cast<uint8_t>(opt.deviceId + 1)) << std::endl;
    std::cout << "[OWNER_READY_FOR_REQUESTER] --peer-ip " << opt.localIp << " --peer-port " << opt.localPort
              << " --peer-device-base-id " << opt.deviceId << " --remote-addrs " << remoteAddrsOss.str() << std::endl;
    std::cout.flush();

    if (opt.holdSeconds == 0) {
        std::cout << "Press ENTER to exit owner..." << std::endl;
        std::string dummy;
        std::getline(std::cin, dummy);
    } else {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(opt.holdSeconds);
        while (!StopRequested() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(K_OWNER_POLL_INTERVAL_MS));
        }
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
    std::vector<uintptr_t> registeredAddrs;
    std::vector<size_t> registeredLengths;
    registeredAddrs.reserve(opt.contiguousBatch ? 1U : opt.remoteAddrs.size());
    registeredLengths.reserve(opt.contiguousBatch ? 1U : opt.remoteAddrs.size());

    auto cleanup = [&engine, &dstDevs, &ownAllocs]() {
        (void)engine.Finalize();
        for (size_t i = 0; i < dstDevs.size(); ++i) {
            if (ownAllocs[i]) {
                (void)testutil::AclFree(dstDevs[i]);
            }
        }
    };

    if (opt.contiguousBatch) {
        uint64_t totalBytes = 0;
        if (!CheckedMul(opt.size, opt.remoteAddrs.size(), &totalBytes) ||
            totalBytes > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
            cleanup();
            return Result(ErrorCode::kInvalid, "contiguous requester allocation size overflow");
        }
        void *dstBase = nullptr;
        rc = testutil::AclMalloc(static_cast<size_t>(totalBytes), &dstBase);
        if (rc.IsError()) {
            cleanup();
            return rc;
        }
        const uintptr_t base = PtrToAddr(dstBase);
        for (size_t i = 0; i < opt.remoteAddrs.size(); ++i) {
            uint64_t offset = 0;
            (void)CheckedMul(opt.size, static_cast<uint64_t>(i), &offset);
            dstDevs.push_back(AddrToPtr(base + static_cast<uintptr_t>(offset)));
            ownAllocs.push_back(i == 0);
        }
        registeredAddrs.push_back(base);
        registeredLengths.push_back(static_cast<size_t>(totalBytes));
    } else {
        for (size_t i = 0; i < opt.remoteAddrs.size(); ++i) {
            DeviceAllocation destination;
            rc = PrepareRequesterDestinationBuffer(opt, i, destination);
            if (rc.IsError()) {
                cleanup();
                return rc;
            }
            dstDevs.push_back(destination.addr);
            ownAllocs.push_back(destination.owned);
            registeredAddrs.push_back(PtrToAddr(destination.addr));
            registeredLengths.push_back(static_cast<size_t>(opt.size));
        }
    }
    rc = engine.BatchRegisterMemory(registeredAddrs, registeredLengths);
    if (rc.IsError()) {
        cleanup();
        return rc;
    }

    std::vector<uintptr_t> buffers;
    std::vector<uintptr_t> peerBufferAddresses;
    std::vector<size_t> lengths;
    buffers.reserve(opt.remoteAddrs.size());
    peerBufferAddresses.reserve(opt.remoteAddrs.size());
    lengths.reserve(opt.remoteAddrs.size());
    for (size_t i = 0; i < opt.remoteAddrs.size(); ++i) {
        buffers.push_back(PtrToAddr(dstDevs[i]));
        peerBufferAddresses.push_back(static_cast<uintptr_t>(opt.remoteAddrs[i]));
        lengths.push_back(static_cast<size_t>(opt.size));
    }
    uint64_t sampleBytes = 0;
    if (!CheckedMul(opt.size, static_cast<uint64_t>(buffers.size()), &sampleBytes)) {
        cleanup();
        return Result(ErrorCode::kInvalid, "perf sample byte size overflow");
    }

    const std::string targetHostname = opt.peerIp + ":" + std::to_string(opt.peerPort);
    const uint32_t totalSamples = opt.perfWarmup + opt.perfRepeats;
    for (uint32_t sample = 0; sample < totalSamples; ++sample) {
        const auto start = std::chrono::steady_clock::now();
        Result readRc = engine.BatchTransferSyncRead(targetHostname, buffers, peerBufferAddresses, lengths);
        const auto end = std::chrono::steady_clock::now();
        if (readRc.IsError()) {
            cleanup();
            return readRc;
        }

        if (sample >= opt.perfWarmup) {
            const uint32_t repeat = sample - opt.perfWarmup + 1;
            const int64_t elapsedNs = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
            const double elapsedMs = static_cast<double>(elapsedNs) / K_NS_PER_MS;
            const double throughputGibS = elapsedNs > 0 ?
                (static_cast<double>(sampleBytes) * K_NS_PER_SECOND / static_cast<double>(elapsedNs) / K_BYTES_PER_KIB /
                 K_BYTES_PER_KIB / K_BYTES_PER_KIB) :
                0.0;
            std::ostringstream oss;
            oss << "[REQUESTER_PERF] repeat=" << repeat
                << " elapsed_ms=" << std::fixed << std::setprecision(K_ELAPSED_MS_PRECISION) << elapsedMs
                << " throughput_gib_s=" << std::fixed << std::setprecision(K_THROUGHPUT_PRECISION) << throughputGibS
                << " total_bytes=" << sampleBytes
                << " batch_size=" << buffers.size()
                << " bytes_each=" << opt.size;
            std::cout << oss.str() << std::endl;
        }
    }

    const uint8_t autoExpected = static_cast<uint8_t>(opt.peerDeviceBaseId + 1);
    for (size_t i = 0; i < dstDevs.size(); ++i) {
        if (opt.skipReadback) {
            std::cout << "[REQUESTER_BATCH_ITEM_DONE] idx=" << i << " remote=0x" << std::hex << opt.remoteAddrs[i]
                      << std::dec << " bytes=" << opt.size << " first16=<skipped>" << std::endl;
            continue;
        }
        std::vector<uint8_t> host(opt.size, 0);
        rc = testutil::AclMemcpy(host.data(), host.size(), dstDevs[i], static_cast<size_t>(opt.size),
                                 K_ACL_MEMCPY_DEVICE_TO_HOST);
        if (rc.IsError()) {
            cleanup();
            return rc;
        }

        if (opt.autoVerifyData) {
            for (size_t j = 0; j < host.size(); ++j) {
                if (host[j] != autoExpected) {
                    cleanup();
                    return Result(ErrorCode::kRuntimeError,
                                  "auto verify failed at batch=" + std::to_string(i) + ", index=" +
                                      std::to_string(j) + ", got=" + std::to_string(host[j]) +
                                      ", expected=" + std::to_string(autoExpected));
                }
            }
        } else if (opt.verifyPattern) {
            for (size_t j = 0; j < host.size(); ++j) {
                if (host[j] != opt.pattern) {
                    cleanup();
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

    std::cout << "[REQUESTER_BATCH_DONE] batch_size=" << buffers.size() << " bytes_each=" << opt.size
              << " repeats=" << opt.perfRepeats << " warmup=" << opt.perfWarmup << std::endl;

    cleanup();
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
            if (workerPort == 0 || workerPort > K_MAX_TCP_PORT) {
                _exit(101);
            }
            worker.localPort = static_cast<uint16_t>(workerPort);
            worker.dstAddr.reset();

            Result rc = RunRequester(worker);
            if (rc.IsError()) {
                TE_LOG_ERROR << "concurrent requester worker failed, index=" << i
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

    TE_LOG_INFO << "concurrent requester succeeded, workers=" << opt.requesterCount
              << ", peer=" << opt.peerIp << ":" << opt.peerPort
              << ", batch_size=" << opt.remoteAddrs.size();
    return Result::OK();
}

}  // namespace
}  // namespace datasystem

int main(int argc, char **argv)
{
    using namespace datasystem;
    internal::InitializeLogging();

    Options opt;
    std::string err;
    if (!ParseArgs(argc, argv, &opt, &err)) {
        TE_LOG_ERROR << "Argument error: " << err;
        PrintUsage(argv[0]);
        return 2;
    }

    Result rc = (opt.role == "owner") ? RunOwner(opt) : RunRequesterConcurrent(opt);
    if (rc.IsError()) {
        TE_LOG_ERROR << "Failed: " << rc.ToString();
        return 1;
    }
    return 0;
}
