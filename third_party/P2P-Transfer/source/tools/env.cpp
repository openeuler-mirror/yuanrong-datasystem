/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "tools/env.h"

// Extract the start and endport from a string of the format startPort-endPort
bool ParsePortRange(const std::string &rangeStr, uint16_t &startPort, uint16_t &endPort)
{
    size_t dashPos = rangeStr.find('-');
    if (dashPos == std::string::npos) {
        // Invalid format (no dash)
        return false;
    }

    try {
        int start = std::stoi(rangeStr.substr(0, dashPos));
        int end = std::stoi(rangeStr.substr(dashPos + 1));
        if (start > end) {
            // Invalid range (start is greater than end)
            return false;
        }

        // Check if ports are within valid range (0-65535)
        if (start > 65535 || end > 65535) {
            return false;  // Port values out of range
        }

        startPort = static_cast<uint16_t>(start);
        endPort = static_cast<uint16_t>(end);
    } catch (const std::invalid_argument &e) {
        // Error parsing integers
        return false;
    } catch (const std::out_of_range &e) {
        // Out of range for integers
        return false;
    }

    return true;
}

Status GetPortRange(uint16_t &startPort, uint16_t &endPort)
{
    const char *envPortRange = std::getenv(PORT_RANGE_ENV);
    if (envPortRange == nullptr) {
        startPort = PORT_RANGE_DEFAULT_START;
        endPort = PORT_RANGE_DEFAULT_END;
        return Status::Success();
    }

    std::string portRangeStr(envPortRange);
    bool parseSuccess = ParsePortRange(portRangeStr, startPort, endPort);
    if (!parseSuccess) {
        return Status::Error(ErrorCode::INVALID_ENV, "Invalid P2P_PORT_RANGE format");
    }

    return Status::Success();
}

Status GetRocePortRange(unsigned int &startPort, unsigned int &endPort)
{
    const char *envPortRange = std::getenv(ROCE_PORT_RANGE_ENV);
    if (envPortRange == nullptr) {
        startPort = ROCE_PORT_RANGE_DEFAULT_START;
        endPort = ROCE_PORT_RANGE_DEFAULT_END;
        return Status::Success();
    }

    std::string portRangeStr(envPortRange);
    uint16_t sp, ep;
    bool parseSuccess = ParsePortRange(portRangeStr, sp, ep);
    if (!parseSuccess) {
        return Status::Error(ErrorCode::INVALID_ENV, "Invalid P2P_ROCE_PORT_RANGE format");
    }

    startPort = sp;
    endPort = ep;

    return Status::Success();
}