#include "common/rdma/prepare_local_server.h"

namespace datasystem {

LocalBuffer::LocalBuffer(const ucp_context_h &context, const std::string &data) : context_(context)
{
    size_t alignedSize = (data.size() + 63) & ~63;
    data_.resize(alignedSize);
    std::copy(data.begin(), data.end(), data_.begin());

    ucp_mem_map_params_t params = {};
    params.field_mask =
        UCP_MEM_MAP_PARAM_FIELD_ADDRESS | UCP_MEM_MAP_PARAM_FIELD_LENGTH | UCP_MEM_MAP_PARAM_FIELD_FLAGS;
    params.address = data_.data();
    params.length = alignedSize;
    params.flags = 0;

    ucp_mem_map(context_, &params, &memH_);

    ucp_mem_attr_t mem_attr = {};
    mem_attr.field_mask = UCP_MEM_ATTR_FIELD_ADDRESS;
    ucp_mem_query(memH_, &mem_attr);

    bufferAddr_ = reinterpret_cast<uintptr_t>(mem_attr.address);
    actualSize_ = alignedSize;
    dataSize_ = data.size();
}

LocalBuffer::~LocalBuffer()
{
    if (memH_ != nullptr) {
        ucp_mem_unmap(context_, memH_);
    }
}

std::string LocalBuffer::ReadBuffer()
{
    const char *data = reinterpret_cast<const char *>(bufferAddr_);
    std::string msg(data, actualSize_);
    return msg;
}
}  // namespace datasystem