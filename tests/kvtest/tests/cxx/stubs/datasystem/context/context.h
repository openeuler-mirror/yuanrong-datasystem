#pragma once

#include <string>

#include "datasystem/kv_client.h"

namespace datasystem {
class Context {
public:
    static Status SetTraceId(const std::string &)
    {
        return Status::OK();
    }
};
}  // namespace datasystem
