#include "common/rdma/mimic_remote_server.h"

#include <cstdint>
#include <cstring>

namespace datasystem {

MimicRemoteServer::MimicRemoteServer(ucp_context_h &context) : context_(context)
{
}

MimicRemoteServer::~MimicRemoteServer()
{
    if (localWorkerAddr_) {
        ucp_worker_release_address(worker_, localWorkerAddr_);
        localWorkerAddr_ = nullptr;
    }

    if (memH_) {
        ucp_mem_unmap(context_, memH_);
        memH_ = nullptr;
    }

    if (worker_) {
        ucp_worker_destroy(worker_);
        worker_ = nullptr;
    }

    if (buffer_) {
        buffer_ = nullptr;
    }
}

void MimicRemoteServer::InitUcpWorker()
{
    ucp_worker_params_t workerParams = {};
    workerParams.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
    workerParams.thread_mode = UCS_THREAD_MODE_SINGLE;

    ucp_worker_create(context_, &workerParams, &worker_);
    size_t workerAddrLen;

    ucp_worker_get_address(worker_, &localWorkerAddr_, &workerAddrLen);

    localWorkerAddrStr_ = std::string(reinterpret_cast<const char *>(localWorkerAddr_), workerAddrLen);
}

void MimicRemoteServer::InitUcpSegment()
{
    ucp_mem_map_params_t params = {};
    params.field_mask =
        UCP_MEM_MAP_PARAM_FIELD_ADDRESS | UCP_MEM_MAP_PARAM_FIELD_LENGTH | UCP_MEM_MAP_PARAM_FIELD_FLAGS;
    params.address = NULL;
    params.length = buf_size_;
    params.flags = UCP_MEM_MAP_ALLOCATE;

    ucp_mem_map(context_, &params, &memH_);

    void *rkeyBuffer;
    size_t rkeySize;
    ucp_rkey_pack(context_, memH_, &rkeyBuffer, &rkeySize);

    ucp_mem_attr_t mem_attr = {};
    mem_attr.field_mask = UCP_MEM_ATTR_FIELD_ADDRESS | UCP_MEM_ATTR_FIELD_LENGTH;
    ucp_mem_query(memH_, &mem_attr);

    buffer_ = mem_attr.address;

    packedRkey_ = std::string(static_cast<const char *>(rkeyBuffer), rkeySize);

    ucp_rkey_buffer_release(rkeyBuffer);
}

std::string MimicRemoteServer::ReadBuffer(size_t len)
{
    const char *data = reinterpret_cast<const char *>(buffer_);
    std::string msg(data, len);
    return msg;
}
}  // namespace datasystem