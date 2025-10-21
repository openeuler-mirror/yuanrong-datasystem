/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef TSD_H
#define TSD_H

#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>
#include <time.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Open Tsd interface
 * @param [in] logicDeviceId physical device ID
 * @param [in] rankSize: Training rank size (default 1). If greater than 1, HCCP
 * is started to for communication related operations
 * @return TSD_OK : success
 */
uint32_t TsdOpen(const uint32_t logicDeviceId, const uint32_t rankSize);

/**
* @brief Instructs the TsdClient to close resources.
* @param [in] logicDeviceId physical device ID
* @return TSD_OK : success
*/
uint32_t TsdClose(const uint32_t logicDeviceId);

#ifdef __cplusplus
}
#endif
#endif // TSD_H