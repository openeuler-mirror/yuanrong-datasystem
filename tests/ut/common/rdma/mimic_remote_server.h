#ifndef COMMON_RDMA_MIMIC_REMOTE_SERVER_H
#define COMMON_RDMA_MIMIC_REMOTE_SERVER_H

#include "ucp/api/ucp.h"
#include "ucp/api/ucp_def.h"

#include <string>
#include <memory>
#include <cstdlib>

namespace datasystem {

class MimicRemoteServer {
public:
    MimicRemoteServer(ucp_context_h &context);
    ~MimicRemoteServer();

    void InitUcpWorker();
    void InitUcpSegment();

    std::string ReadBuffer(size_t len);

    inline ucp_worker_h GetWorker()
    {
        return worker_;
    }
    inline std::string GetWorkerAddr()
    {
        return localWorkerAddrStr_;
    }
    inline std::string &GetPackedRkey()
    {
        return packedRkey_;
    }
    inline void *GetLocalSegAddr()
    {
        return buffer_;
    }
    inline size_t GetLocalSegSize()
    {
        return buf_size_;
    }

private:
    ucp_context_h context_;

    ucp_worker_h worker_ = nullptr;
    ucp_address_t *localWorkerAddr_ = nullptr;
    std::string localWorkerAddrStr_;

    void *buffer_ = nullptr;
    size_t buf_size_ = 1024;
    ucp_mem_h memH_ = nullptr;

    // utility variables
    std::string packedRkey_;
};

}  // namespace datasystem
#endif
