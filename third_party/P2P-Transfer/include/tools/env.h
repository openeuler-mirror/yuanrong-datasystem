/*
* Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef P2P_ENV_H
#define P2P_ENV_H

#include "tools/Status.h"
#include <cstdint>

// The host interface IP(s) which can be used by P2P for communicator information exchange
// Order: P2P_IF_IP > P2P_SOCKET_IFNAME > HCCL_IF_IP > HCCL_SOCKET_IFNAME
//        > docker/lo external network card (nin order appears) > docker network card > lo network card
constexpr const char *IF_IP_ENV = "P2P_IF_IP";
constexpr const char *IF_NAME_ENV = "P2P_SOCKET_IFNAME";
constexpr const char *IF_IP_ENV_HCCL = "HCCL_IF_IP";
constexpr const char *IF_NAME_ENV_HCCL = "HCCL_SOCKET_IFNAME";

// The port range on the given interface(s) used by P2P for communicator information exchange
constexpr const char *PORT_RANGE_ENV = "P2P_PORT_RANGE";
constexpr uint16_t PORT_RANGE_DEFAULT_START = 56800;
constexpr uint16_t PORT_RANGE_DEFAULT_END = 56900;

// The port range on the given interface(s) used by P2P for communicator information exchange
constexpr const char *ROCE_PORT_RANGE_ENV = "P2P_ROCE_PORT_RANGE";
constexpr unsigned int ROCE_PORT_RANGE_DEFAULT_START = 56800;
constexpr unsigned int ROCE_PORT_RANGE_DEFAULT_END = 56900;

// Determine the start and end port available for P2P transfer based on the environment variable
Status GetPortRange(uint16_t &startPort, uint16_t &endPort);

Status GetRocePortRange(unsigned int &startPort, unsigned int &endPort);

#endif  // P2P_ENV_H
