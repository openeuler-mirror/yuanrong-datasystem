#pragma once

#include "common/config.h"
#include "data_pattern.h"
#include <datasystem/kv_client.h>
#include <cstddef>
#include <vector>

namespace kvtest {

struct CudaLane {
    void *source = nullptr;
    void *destination = nullptr;
    void *event = nullptr;
    int sourceSenderId = 0;
    std::vector<char> verification;
};

struct CudaTiming {
    double enqueueMs = 0;
    double eventMs = 0;
    double waitMs = 0;
    double totalMs = 0;
};

// Call before KVClient::Init. The runtime library and registered callbacks stay
// loaded until process exit, including asynchronous Datasystem cleanup.
datasystem::Status InitCudaWorkload(const Config &cfg);
void CloseCudaWorkload();
bool CudaTransfersEnabled();
datasystem::Status PrepareCudaSource(CudaLane &lane, int senderId);

// A lease belongs to a logical pipeline invocation, not to a pthread: an SDK
// call can migrate a bthread. The pool lock never covers CUDA or SDK calls.
class CudaLaneGuard {
public:
    explicit CudaLaneGuard(bool required);
    ~CudaLaneGuard();
    CudaLaneGuard(const CudaLaneGuard &) = delete;
    CudaLaneGuard &operator=(const CudaLaneGuard &) = delete;
    CudaLane *Get() const { return lane_; }
private:
    CudaLane *lane_ = nullptr;
};

datasystem::Status CopyCudaBuffers(datasystem::KVClient &client, CudaLane &lane,
                                  const std::vector<void *> &hosts, size_t size, bool h2d,
                                  CudaTiming &timing);
datasystem::Status VerifyCudaBuffers(CudaLane &lane, size_t count, size_t size, int senderId,
                                    const VerifyConfig &cfg, bool &matches);
const std::vector<const char *> &GetCudaMetricNames();

}  // namespace kvtest
