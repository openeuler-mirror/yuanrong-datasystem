/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#include "tools/Status.h"

// Overload operator<< for easier output
std::ostream &operator<<(std::ostream &os, const Status &status)
{
    os << status.ToString();
    return os;
}