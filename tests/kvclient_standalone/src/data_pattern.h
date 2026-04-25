#pragma once
#include <cstdint>
#include <string>

inline std::string GeneratePatternData(uint64_t size, int senderId) {
    std::string data(size, '\0');
    for (uint64_t i = 0; i < size; i++) {
        data[i] = static_cast<char>((senderId + i) % 256);
    }
    return data;
}

// Verify data matches the pattern without allocating memory
inline bool VerifyPatternData(const char *data, uint64_t size, int senderId) {
    for (uint64_t i = 0; i < size; i++) {
        if (data[i] != static_cast<char>((senderId + i) % 256)) return false;
    }
    return true;
}
