/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "datasystem/common/rdma/rdma_util.h"

#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <sys/ioctl.h>
#include <netdb.h>
#include <ifaddrs.h>
#include <unistd.h>
#include <net/if.h>
#include <dirent.h>

#include <fstream>

#include "datasystem/common/log/log.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/util/gflag/common_gflags.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"

namespace datasystem {
static constexpr int RDMA_LOG_LEVEL = 3;
const int OCTET = 8;
const int IP_NUM_OCTETS = 4;
const int IPV4_MAX_LENGTH = 16;
const std::string IB_PATH = "/sys/class/infiniband/";
Status GetDevNameFromDestIp(const std::string &ipAddr, std::string &devName)
{
    std::istringstream iss(ipAddr);
    std::string nextToken;
    int num[4];
    for (int i = 0; std::getline(iss, nextToken, '.'); i++) {
        num[i] = atoi(nextToken.c_str());
    }

    unsigned long ip = 0;
    for (int i = 0; i < IP_NUM_OCTETS; i++) {
        ip += num[i] << (OCTET * i);
    }
    VLOG(RDMA_LOG_LEVEL) << FormatString("%lu\n", ip);

    char devname[64];
    unsigned long d, g, m;
    int r, flgs, ref, use, metric, mtu, win, ir;

    std::string path("/proc/net/route");
    FILE *fp = fopen(path.c_str(), "r");
    RETURN_RUNTIME_ERROR_IF_NULL(fp);

    /* Skip the first line. */
    r = fscanf_s(fp, "%*[^\n]\n");
    /* Empty line, read error, or EOF. Yes, if routing table
     * is completely empty, /proc/net/route has no header.
     */
    CHECK_FAIL_RETURN_STATUS(r >= 0, K_RUNTIME_ERROR, "Empty line, read error, or EOF");
    bool found = false;
    while (!found) {
        r = fscanf_s(fp, "%63s%lx%lx%X%d%d%d%lx%d%d%d\n", devname, sizeof(devname), &d, &g, &flgs, &ref, &use, &metric,
                     &m, &mtu, &win, &ir);
        // Linux error code 11 is EAGAIN
        if (r != EAGAIN) {
            if ((r < 0) && feof(fp)) { /* EOF with no (nonspace) chars read. */
                break;
            }
        }
        if (!(flgs & 0x0001)) { /* Skip interfaces that are down. */
            continue;
        }
        VLOG(RDMA_LOG_LEVEL) << FormatString("devname=%s,dest=%lld,gw=%lld,mask=%lld\n", devname, d, g, m);
        // Record device name in case the exact matching one is missing
        devName = std::string(devname);
        if ((d & m) == (ip & m) && (d & m) != 0) {
            VLOG(RDMA_LOG_LEVEL) << FormatString("find success devname=%s\n", devname);
            found = true;
        }
    }
    if (!found) {
        LOG(WARNING) << FormatString("Device name of %s is not found. Will use device name: %s", ipAddr, devName);
    }
    (void)fclose(fp);
    return Status::OK();
}

void *GetInterfaceInAddr(struct ifaddrs *ifa)
{
    // AF_INET version (IPv4)
    if (ifa->ifa_addr->sa_family == AF_INET) {
        return &((reinterpret_cast<struct sockaddr_in *>(ifa->ifa_addr))->sin_addr);
    }
    // AF_INET6 version (IPv6)
    return &((reinterpret_cast<struct sockaddr_in6 *>(ifa->ifa_addr))->sin6_addr);
}

int GetDevNameFromLocalIp(const std::string &ipAddr, std::string &devName)
{
    struct ifaddrs *ifaddr, *ifa;
    int family;

    if (getifaddrs(&ifaddr) == -1) {
        perror("getifaddrs");
        return 1;
    }

    bool found = false;
    uint32_t expectedFlags = IFF_UP | IFF_BROADCAST | IFF_RUNNING | IFF_MULTICAST;
    for (ifa = ifaddr; ifa != NULL && found == false; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == NULL)
            continue;

        family = ifa->ifa_addr->sa_family;

        if ((family == AF_INET || family == AF_INET6) && (expectedFlags & ifa->ifa_flags) == expectedFlags) {
            // Record device name in case the exact matching one is missing
            devName = std::string(ifa->ifa_name);
            std::string inetRes(INET6_ADDRSTRLEN, 0);
            if (0 == strcmp(ipAddr.c_str(), inet_ntop(ifa->ifa_addr->sa_family, GetInterfaceInAddr(ifa),
                                                      const_cast<char *>(inetRes.c_str()), inetRes.size()))) {
                VLOG(RDMA_LOG_LEVEL) << FormatString("find success devname=%s\n", ifa->ifa_name);
                found = true;
            }
        }
    }

    freeifaddrs(ifaddr);

    if (!found) {
        LOG(WARNING) << FormatString(
            "Device name of %s is either not found or not matching flags. Will use device name: %s", ipAddr, devName);
    }
    return 0;
}

static void ReadGids(struct dirent *devEnt, struct dirent *portEnt,
                     std::unordered_map<std::string, std::string> &results)
{
    std::string portsPath = IB_PATH + devEnt->d_name + "/ports/";
    std::string gidsPath = portsPath + portEnt->d_name + "/gids/";
    DIR *gids = opendir(gidsPath.c_str());
    if (gids == nullptr) {
        return;
    }
    struct dirent *gidEnt;
    // for each port read gids
    while ((gidEnt = readdir(gids)) != nullptr) {
        if (strcmp(gidEnt->d_name, ".") == 0 || strcmp(gidEnt->d_name, "..") == 0) {
            continue;
        }
        std::ifstream gidFile(gidsPath + gidEnt->d_name);
        std::string gid;
        std::getline(gidFile, gid);
        gidFile.close();
        // check if gid is valid
        if (gid == "0000:0000:0000:0000:0000:0000:0000:0000" || gid == "fe80:0000:0000:0000:0000:0000:0000:0000") {
            continue;
        }
        // read ndev from port and gid
        std::string ndevPath = portsPath + portEnt->d_name + "/gid_attrs/ndevs/" + gidEnt->d_name;
        std::string ndevName;
        std::ifstream ndevFile(ndevPath);
        if (!std::getline(ndevFile, ndevName)) {
            VLOG(RDMA_LOG_LEVEL) << "Read ndev file failed on path: " << ndevPath;
        } else if (ndevName == "") {
            VLOG(RDMA_LOG_LEVEL) << "ndev name empty on path: " << ndevPath;
        } else {
            results.emplace(ndevName, devEnt->d_name);
        }
        ndevFile.close();
    }
    closedir(gids);
}

Status EthToRdmaDevName(std::string ethDevName, std::string &rdmaDevName)
{
    std::unordered_map<std::string, std::string> results;
    DIR *devs = opendir(IB_PATH.c_str());
    CHECK_FAIL_RETURN_STATUS(devs != nullptr, K_RUNTIME_ERROR, "Unable to open directory: " + IB_PATH);
    struct dirent *devEnt;
    // read devs
    while ((devEnt = readdir(devs)) != nullptr) {
        if (strcmp(devEnt->d_name, ".") == 0 || strcmp(devEnt->d_name, "..") == 0) {
            continue;
        }
        std::string portsPath = IB_PATH + devEnt->d_name + "/ports/";
        DIR *ports = opendir(portsPath.c_str());
        if (ports == nullptr) {
            continue;
        }
        struct dirent *portEnt;
        // read ports
        while ((portEnt = readdir(ports)) != nullptr) {
            if (strcmp(portEnt->d_name, ".") == 0 || strcmp(portEnt->d_name, "..") == 0) {
                continue;
            }
            ReadGids(devEnt, portEnt, results);
        }
        closedir(ports);
    }
    closedir(devs);

    rdmaDevName.clear();
    if (results.find(ethDevName) != results.end()) {
        rdmaDevName = std::move(results[ethDevName]);
    } else if (!results.empty()) {
        LOG(WARNING) << "Unable to determine the exact RDMA device from eth device name, use any RDMA device name";
        rdmaDevName = std::move(results.begin()->second);
    }
    CHECK_FAIL_RETURN_STATUS(!rdmaDevName.empty(), K_RUNTIME_ERROR, "Unable to find any RDMA device name.");
    VLOG(RDMA_LOG_LEVEL) << "Result RdmaDevName is " << rdmaDevName;
    return Status::OK();
}

UrmaMode GetUrmaMode()
{
    if (FLAGS_urma_mode == "IB") {
        return UrmaMode::IB;
    } else if (FLAGS_urma_mode == "UB") {
        return UrmaMode::UB;
    }
    return UrmaMode::UNKNOWN;
};
}  // namespace datasystem
