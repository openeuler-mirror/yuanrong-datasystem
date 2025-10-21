/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef P2P_HOST_INTERFACE_H
#define P2P_HOST_INTERFACE_H

#include "tools/Status.h"
#include <vector>

struct InterfaceInfo {
    std::string name;
    std::string family;
    std::string address;
};

Status GetHostInterfaces(std::vector<InterfaceInfo> &external_interfaces,
                         std::vector<InterfaceInfo> &container_interfaces, std::vector<InterfaceInfo> &lo_interfaces);
Status GetHostIp(std::string &ip);

#endif  // P2P_HOST_INTERFACE_H