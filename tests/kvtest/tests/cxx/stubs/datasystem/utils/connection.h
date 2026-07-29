#pragma once

#include <cstdint>

// ConnectOptions is already defined in kv_client.h stub

namespace datasystem {
enum class DataPlacementPolicy : uint8_t {
    PREFERRED_SAME_NODE = 0,
    REQUIRED_SAME_NODE,
    PREFERRED_META_OWNER,
};
}  // namespace datasystem
