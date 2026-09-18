#include "cuda_workload.h"
#include "common/bthread_compat.h"
#include "common/simple_log.h"
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <dlfcn.h>
#include <limits>
#include <memory>
#include <stdexcept>

namespace kvtest {
namespace {
using Clock = std::chrono::steady_clock;
using datasystem::Status;
constexpr int kH2d = 1;
constexpr int kD2h = 2;
constexpr int kNoDevice = 100;
constexpr int kInsufficientDriver = 35;
constexpr unsigned kNonBlockingStream = 1;
constexpr unsigned kDisableEventTiming = 2;

double Milliseconds(Clock::time_point begin, Clock::time_point end) {
    return std::chrono::duration<double, std::milli>(end - begin).count();
}

// Only the stable C runtime ABI is used; no CUDA headers or link-time SDK.
struct Runtime {
    void *library = nullptr;
    int (*getDeviceCount)(int *) = nullptr;
    int (*setDevice)(int) = nullptr;
    const char *(*getErrorString)(int) = nullptr;
    int (*hostRegister)(void *, size_t, unsigned) = nullptr;
    int (*hostUnregister)(void *) = nullptr;
    int (*mallocDevice)(void **, size_t) = nullptr;
    int (*freeDevice)(void *) = nullptr;
    int (*streamCreate)(void **, unsigned) = nullptr;
    int (*streamDestroy)(void *) = nullptr;
    int (*streamSynchronize)(void *) = nullptr;
    int (*eventCreate)(void **, unsigned) = nullptr;
    int (*eventDestroy)(void *) = nullptr;
    int (*eventRecord)(void *, void *) = nullptr;
    int (*eventSynchronize)(void *) = nullptr;
    int (*memcpyAsync)(void *, const void *, size_t, int, void *) = nullptr;
    int deviceId = 0;
    bool transfers = false;
    bool pin = false;
    bool initialized = false;
    size_t stride = 0;
    size_t batchSize = 0;
    void *stream = nullptr;
    mutex lanesMutex;
    std::vector<std::unique_ptr<CudaLane>> lanes;
    std::vector<CudaLane *> available;
};

Runtime &Cuda() {
    static Runtime *runtime = new Runtime;
    return *runtime;
}

void *OpenCudaLibrary(const char *name, std::string &errors) {
    dlerror();
    void *library = dlopen(name, RTLD_NOW | RTLD_LOCAL);
    if (!library) {
        const char *error = dlerror();
        errors += std::string("; dlopen(") + name + "): " + (error ? error : "unknown loader error");
    }
    return library;
}

Status Error(int rc, const char *operation) {
    if (rc == 0) return Status::OK();
    return Status(datasystem::K_RUNTIME_ERROR, std::string(operation) + ": " + Cuda().getErrorString(rc));
}

template <typename T>
void Load(T &function, const char *name) {
    function = reinterpret_cast<T>(dlsym(Cuda().library, name));
    if (!function) throw std::runtime_error(std::string("Missing CUDA symbol: ") + name);
}

void LoadFunctions() {
    auto &r = Cuda();
    Load(r.getDeviceCount, "cudaGetDeviceCount");
    Load(r.setDevice, "cudaSetDevice");
    Load(r.getErrorString, "cudaGetErrorString");
    Load(r.hostRegister, "cudaHostRegister");
    Load(r.hostUnregister, "cudaHostUnregister");
    Load(r.mallocDevice, "cudaMalloc");
    Load(r.freeDevice, "cudaFree");
    Load(r.streamCreate, "cudaStreamCreateWithFlags");
    Load(r.streamDestroy, "cudaStreamDestroy");
    Load(r.streamSynchronize, "cudaStreamSynchronize");
    Load(r.eventCreate, "cudaEventCreateWithFlags");
    Load(r.eventDestroy, "cudaEventDestroy");
    Load(r.eventRecord, "cudaEventRecord");
    Load(r.eventSynchronize, "cudaEventSynchronize");
    Load(r.memcpyAsync, "cudaMemcpyAsync");
}

int Register(void *pointer, size_t size, unsigned flags) {
    auto &r = Cuda();
    const int rc = r.setDevice(r.deviceId);
    return rc == 0 ? r.hostRegister(pointer, size, flags) : rc;
}
int Unregister(void *pointer) {
    auto &r = Cuda();
    const int rc = r.setDevice(r.deviceId);
    return rc == 0 ? r.hostUnregister(pointer) : rc;
}
int Copy(void *dst, const void *src, size_t size, datasystem::DsCudaMemcpyKind kind, void *stream) {
    auto &r = Cuda();
    const int rc = r.setDevice(r.deviceId);
    if (rc != 0) return rc;
    if (kind == datasystem::DsCudaMemcpyKind::HOST_TO_DEVICE) return r.memcpyAsync(dst, src, size, kH2d, stream);
    if (kind == datasystem::DsCudaMemcpyKind::DEVICE_TO_HOST) return r.memcpyAsync(dst, src, size, kD2h, stream);
    return 1;  // cudaErrorInvalidValue
}

// Even a failed DsCudaMemcpyAsync may have submitted earlier fragments.
// If completion cannot be established, terminate this test process without
// unwinding live Buffer owners; continuing could unmap an in-flight pointer.
int Fence(CudaLane &lane, CudaTiming &timing) noexcept {
    auto &r = Cuda();
    const auto begin = Clock::now();
    int rc = r.setDevice(r.deviceId);
    if (rc == 0) rc = r.eventRecord(lane.event, r.stream);
    const auto recorded = Clock::now();
    if (rc == 0) rc = r.eventSynchronize(lane.event);
    if (rc != 0) {
        SLOG_ERROR("CUDA completion event failed, fencing shared stream, rc=" << rc);
        if (r.setDevice(r.deviceId) != 0 || r.streamSynchronize(r.stream) != 0) {
            SLOG_ERROR("CUDA completion unknown; terminating kvtest without releasing in-flight buffers");
            std::_Exit(EXIT_FAILURE);
        }
    }
    timing.eventMs = Milliseconds(begin, recorded);
    timing.waitMs = Milliseconds(recorded, Clock::now());
    return rc;
}

void AllocateLanes(const Config &cfg) {
    auto &r = Cuda();
    if (cfg.dataSizes.empty() || cfg.batchKeysCount <= 0 || cfg.numTotalThreads <= 0) {
        throw std::runtime_error("CUDA workload requires positive sizes, batch count and concurrency");
    }
    r.stride = *std::max_element(cfg.dataSizes.begin(), cfg.dataSizes.end());
    r.batchSize = static_cast<size_t>(cfg.batchKeysCount);
    if (r.stride == 0 || r.batchSize == 0 || r.stride > std::numeric_limits<size_t>::max() / r.batchSize) {
        throw std::runtime_error("CUDA buffer capacity overflow or zero size");
    }
    const auto check = [](int rc) { if (rc != 0) throw std::runtime_error(Error(rc, "CUDA prepare").GetMsg()); };
    check(r.streamCreate(&r.stream, kNonBlockingStream));
    const auto pattern = GeneratePatternData(r.stride, cfg.instanceId);
    const bool verify = cfg.verifyLevel == "sample" || cfg.verifyLevel == "full";
    for (int i = 0; i < cfg.numTotalThreads; ++i) {
        r.lanes.push_back(std::make_unique<CudaLane>());
        auto &lane = *r.lanes.back();
        lane.sourceSenderId = cfg.instanceId;
        check(r.eventCreate(&lane.event, kDisableEventTiming));
        check(r.mallocDevice(&lane.source, r.stride * r.batchSize));
        check(r.mallocDevice(&lane.destination, r.stride * r.batchSize));
        if (verify) lane.verification.resize(r.stride);
        int rc = 0;
        for (size_t j = 0; j < r.batchSize && rc == 0; ++j) {
            rc = r.memcpyAsync(static_cast<char *>(lane.source) + j * r.stride,
                               pattern.data(), r.stride, kH2d, r.stream);
        }
        CudaTiming timing;
        const int fenceRc = Fence(lane, timing);
        check(rc);
        check(fenceRc);
        r.available.push_back(&lane);
    }
    SLOG_INFO("CUDA prepared: lanes=" << r.lanes.size() << " batch_capacity=" << r.batchSize
              << " bytes_per_buffer=" << r.stride << " shared_stream=" << r.stream);
}
}  // namespace

Status InitCudaWorkload(const Config &cfg) {
    auto &r = Cuda();
    if (r.initialized) return Status(datasystem::K_INVALID, "CUDA workload can only be initialized once per process");
    r.initialized = true;
    if (cfg.runMode != RunMode::PIPELINE || cfg.keyPoolSize > 0 || (!cfg.cuda.pin && !cfg.cuda.transferEnabled)) {
        return Status::OK();
    }
    try {
        const auto &path = cfg.cuda.runtimeLibrary;
        std::string loadErrors;
        if (!path.empty()) r.library = OpenCudaLibrary(path.c_str(), loadErrors);
        else for (const char *name : {"libcudart.so", "libcudart.so.13", "libcudart.so.12", "libcudart.so.11.0"}) {
            r.library = OpenCudaLibrary(name, loadErrors);
            if (r.library) break;
        }
        if (!r.library) {
            const char *searchPath = std::getenv("LD_LIBRARY_PATH");
            const auto detail = std::string("Cannot load CUDA runtime library") + loadErrors
                + "; LD_LIBRARY_PATH=" + (searchPath ? searchPath : "<unset>")
                + "; Check CUDA runtime installation and dependencies, LD_LIBRARY_PATH or ldconfig cache,"
                  " or set cuda.runtime_library to an absolute library path.";
            if (cfg.cuda.transferEnabled || !path.empty()) throw std::runtime_error(detail);
            SLOG_INFO("CUDA unavailable; retaining CPU pipeline without Pin: " << detail);
            return Status::OK();
        }
        LoadFunctions();
        int count = 0;
        int rc = r.getDeviceCount(&count);
        if ((rc == kNoDevice || rc == kInsufficientDriver || (rc == 0 && count == 0)) && !cfg.cuda.transferEnabled) {
            SLOG_INFO("CUDA unavailable: device_count=" << count << " rc=" << rc << "; retaining CPU pipeline");
            return Status::OK();
        }
        if (rc != 0) throw std::runtime_error(Error(rc, "cudaGetDeviceCount").GetMsg());
        if (cfg.cuda.deviceId < 0 || cfg.cuda.deviceId >= count) throw std::runtime_error("cuda.device_id is outside visible GPU range");
        r.deviceId = cfg.cuda.deviceId;
        rc = r.setDevice(r.deviceId);
        if (rc != 0) throw std::runtime_error(Error(rc, "cudaSetDevice").GetMsg());
        if (cfg.cuda.transferEnabled) AllocateLanes(cfg);
        if (cfg.cuda.pin) datasystem::KVClient::RegisterCudaFuncs({Register, Unregister, r.getErrorString, Copy});
        r.pin = cfg.cuda.pin;
        r.transfers = cfg.cuda.transferEnabled;
        SLOG_INFO("CUDA enabled: device=" << r.deviceId << " pin=" << r.pin << " transfers=" << r.transfers);
        return Status::OK();
    } catch (const std::exception &e) {
        CloseCudaWorkload();
        return Status(datasystem::K_RUNTIME_ERROR, e.what());
    }
}

void CloseCudaWorkload() {
    auto &r = Cuda();
    if (r.lanes.empty() && !r.stream) return;
    if (r.setDevice(r.deviceId) != 0 || (r.stream && r.streamSynchronize(r.stream) != 0)) {
        SLOG_ERROR("CUDA workload cleanup cannot establish completion; resources retained until process exit");
        return;
    }
    for (auto &lane : r.lanes) {
        if (lane->source) (void)r.freeDevice(lane->source);
        if (lane->destination) (void)r.freeDevice(lane->destination);
        if (lane->event) (void)r.eventDestroy(lane->event);
    }
    r.available.clear();
    r.lanes.clear();
    if (r.stream) (void)r.streamDestroy(r.stream);
    r.stream = nullptr;
}

bool CudaTransfersEnabled() { return Cuda().transfers; }

Status PrepareCudaSource(CudaLane &lane, int senderId) {
    if (lane.sourceSenderId == senderId) return Status::OK();
    auto &r = Cuda();
    const auto pattern = GeneratePatternData(r.stride, senderId);
    int rc = r.setDevice(r.deviceId);
    for (size_t i = 0; i < r.batchSize && rc == 0; ++i) {
        rc = r.memcpyAsync(static_cast<char *>(lane.source) + i * r.stride,
                           pattern.data(), r.stride, kH2d, r.stream);
    }
    CudaTiming timing;
    const int fenceRc = Fence(lane, timing);
    if (rc != 0 || fenceRc != 0) return Error(rc != 0 ? rc : fenceRc, "CUDA source preparation");
    lane.sourceSenderId = senderId;
    return Status::OK();
}

CudaLaneGuard::CudaLaneGuard(bool required) {
    if (!required) return;
    auto &r = Cuda();
    std::lock_guard<mutex> lock(r.lanesMutex);
    if (!r.available.empty()) { lane_ = r.available.back(); r.available.pop_back(); }
}
CudaLaneGuard::~CudaLaneGuard() {
    if (!lane_) return;
    auto &r = Cuda();
    std::lock_guard<mutex> lock(r.lanesMutex);
    r.available.push_back(lane_);
}

Status CopyCudaBuffers(datasystem::KVClient &client, CudaLane &lane,
                       const std::vector<void *> &hosts, size_t size, bool h2d, CudaTiming &timing) {
    auto &r = Cuda();
    if (!r.transfers || !r.stream || hosts.empty() || hosts.size() > r.batchSize || size == 0 || size > r.stride) {
        return Status(datasystem::K_INVALID, "CUDA transfer exceeds configured buffer capacity");
    }
    if (std::find(hosts.begin(), hosts.end(), nullptr) != hosts.end()) {
        return Status(datasystem::K_INVALID, "CUDA transfer received null Host address");
    }
    const auto begin = Clock::now();
    Status result = Status::OK();
    try {
        for (size_t i = 0; i < hosts.size(); ++i) {
            auto *gpu = static_cast<char *>(h2d ? lane.destination : lane.source) + i * r.stride;
            void *dst = h2d ? gpu : hosts[i];
            const void *src = h2d ? hosts[i] : gpu;
            const auto kind = h2d ? datasystem::DsCudaMemcpyKind::HOST_TO_DEVICE : datasystem::DsCudaMemcpyKind::DEVICE_TO_HOST;
            result = r.pin ? client.DsCudaMemcpyAsync(dst, src, size, kind, r.stream)
                           : Error(Copy(dst, src, size, kind, r.stream), "cudaMemcpyAsync");
            if (!result.IsOk()) break;
        }
    } catch (...) {
        CudaTiming ignored;
        (void)Fence(lane, ignored);
        throw;
    }
    timing.enqueueMs = Milliseconds(begin, Clock::now());
    const int fenceRc = Fence(lane, timing);
    timing.totalMs = Milliseconds(begin, Clock::now());
    return result.IsOk() ? Error(fenceRc, "CUDA completion") : result;
}

Status VerifyCudaBuffers(CudaLane &lane, size_t count, size_t size, int senderId,
                         const VerifyConfig &cfg, bool &matches) {
    matches = true;
    if (cfg.level == VerifyLevel::OFF || cfg.level == VerifyLevel::SIZE) return Status::OK();
    auto &r = Cuda();
    if (lane.verification.size() < size || count > r.batchSize || size > r.stride) {
        return Status(datasystem::K_INVALID, "CUDA verification exceeds configured capacity");
    }
    for (size_t i = 0; i < count; ++i) {
        const auto *gpu = static_cast<const char *>(lane.destination) + i * r.stride;
        const int rc = Copy(lane.verification.data(), gpu, size, datasystem::DsCudaMemcpyKind::DEVICE_TO_HOST, r.stream);
        CudaTiming ignored;
        const int fenceRc = Fence(lane, ignored);
        if (rc != 0 || fenceRc != 0) return Error(rc != 0 ? rc : fenceRc, "CUDA verification readback");
        matches = VerifyBuffer(lane.verification.data(), size, size, senderId, cfg) && matches;
    }
    return Status::OK();
}

const std::vector<const char *> &GetCudaMetricNames() {
    static const std::vector<const char *> names = {
        "d2h", "h2d", "mD2h", "mH2d", "cuda_verify", "cuda_prepare",
        "d2h_enqueue", "d2h_event", "d2h_wait", "h2d_enqueue", "h2d_event", "h2d_wait",
        "mD2h_enqueue", "mD2h_event", "mD2h_wait", "mH2d_enqueue", "mH2d_event", "mH2d_wait"};
    return names;
}
}  // namespace kvtest
