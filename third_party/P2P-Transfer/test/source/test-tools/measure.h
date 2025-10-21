/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef TOOLS_H
#define TOOLS_H

#define MEASURE_TIME(func, ...)                                                                               \
    do {                                                                                                      \
        auto start = std::chrono::high_resolution_clock::now();                                               \
        func;                                                                                                 \
        auto end = std::chrono::high_resolution_clock::now();                                                 \
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);                   \
        std::cout << "Time taken by " << (sizeof((char[]){#__VA_ARGS__}) != 1 ? #__VA_ARGS__ : #func) << ": " \
                  << ((double)duration.count()) / 1000 << " ms." << std::endl;                                \
                                                                                                              \
    } while (0)

#endif  // TOOLS_H
