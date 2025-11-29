#include <string>
#include <vector>
#include <cstdint>

#include "ucp/api/ucp.h"
#include "ucp/api/ucp_def.h"

namespace datasystem {

class LocalBuffer {
public:
    LocalBuffer(const ucp_context_h &context, const std::string &data);

    ~LocalBuffer();

    std::string ReadBuffer();

    inline uintptr_t Address() const
    {
        return bufferAddr_;
    }
    inline size_t Size() const
    {
        return dataSize_;
    }

private:
    ucp_context_h context_;
    ucp_mem_h memH_ = nullptr;
    std::vector<char> data_;
    uintptr_t bufferAddr_ = 0;
    size_t actualSize_ = 0;
    size_t dataSize_ = 0;
};
}  // namespace datasystem