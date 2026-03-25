/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef P2P_TOOLS_H
#define P2P_TOOLS_H

#include <stddef.h>
#include <iostream>
#include <random>
#include <arpa/inet.h>

constexpr int MIN_CHAR = 0;
constexpr int MAX_CHAR = 256;

inline size_t RoundUp(size_t num, size_t multiple)
{
    size_t remain = num % multiple;
    if (remain == 0) {
        return num;
    }

    return num + multiple - remain;
}

inline void FillRandom(std::string &str)
{
    const std::string characters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, characters.size() - 1);

    for (char &c : str) {
        c = characters[dis(gen)];
    }
}

// Function to convert in_addr to uint32
inline uint32_t in_addr_to_uint32(const struct in_addr &addr)
{
    return ntohl(addr.s_addr);  // Network to host long
}

// Function to convert uint32 to in_addr
inline void uint32_to_in_addr(uint32_t addr, struct in_addr &result)
{
    result.s_addr = htonl(addr);  // Host to network long
}

#endif  // P2P_TOOLS_H