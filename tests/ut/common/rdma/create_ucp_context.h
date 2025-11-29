#ifndef COMMON_RDMA_CREATE_UCP_CONTEXT_H
#define COMMON_RDMA_CREATE_UCP_CONTEXT_H

#include <cstring>

#include "ucp/api/ucp.h"
#include "ucp/api/ucp_def.h"

namespace datasystem {

class CreateUcpContext {
public:
    CreateUcpContext();
    ~CreateUcpContext();

    inline ucp_context_h GetContext() const
    {
        return context_;
    }

private:
    void InitContext();
    ucp_context_h context_;
};

}  // namespace datasystem

#endif