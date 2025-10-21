/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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

/**
 * Description: Defines the bitmap storing attributes in object_interface
 */

#ifndef DATASYSTEM_OBJECT_CACHE_OBJECT_INTERFACE_BITMAP_H
#define DATASYSTEM_OBJECT_CACHE_OBJECT_INTERFACE_BITMAP_H

#include <bitset>
#include "datasystem/object_cache/object_enum.h"
#include "datasystem/common/util/bitmask_enum.h"

namespace datasystem {

enum class DataFormat : int {
    BINARY = 0,
    LIST = 1,
    HASH_MAP = 2,
    HETERO = 3,
};

enum class ObjectLifeState : int {
    OBJECT_INVALID = 0,
    OBJECT_PUBLISHED = 1,
    OBJECT_SEALED = 2,
};

struct Config {
    ConsistencyType consistencyType;
    DataFormat dataFormat;
    WriteMode writeMode;
};

// enum values: FLAGs for object attributes
enum class ObjectFlag : uint16_t {
    NONE = 0,
    CACHE_INVALID = 1u,
    PRIMARY_COPY = 1u << 1,
    SPILL_STATE = 1u << 2,
    WRITE_BACK_DONE = 1u << 3,
    NEED_TO_DELETE = 1u << 4,
    BINARY = 1u << 5,
    LIST = 1u << 6,
    HASH_MAP = 1u << 7,
    HETERO = 1u << 8,
    IS_EMPTY = 1u << 9,
    IS_INCOMPLETE = 1u << 10,
};

enum class ModeFlag : uint8_t {
    NONE = 0,
    NONE_L2_CACHE = 1u,
    WRITE_THROUGH_L2_CACHE = 1u << 1,
    WRITE_BACK_L2_CACHE = 1u << 2,
    NONE_L2_CACHE_EVICT = 1u << 3,
    PRAM = 1u << 4,
    CAUSAL = 1u << 5,
    MEMORY = 1u << 6,
    DISK = 1u << 7,
};

ENABLE_BITMASK_ENUM_OPS(ObjectFlag);
ENABLE_BITMASK_ENUM_OPS(ModeFlag);

class StateInfo {
public:
    /**
     * @brief default constructor for state info
     * @param initialBitmap a given bitmap
     */
    StateInfo(ObjectFlag initialBitmap = ObjectFlag::NONE) : stateBitmap_(initialBitmap){};

    /**
     * @brief constructor 1
     * @param dataFormat specified data format
     */
    StateInfo(DataFormat dataFormat)
    {
        stateBitmap_ = ObjectFlag::NONE;
        SetDataFormat(dataFormat);
    }

    /**
     * @brief Check if the object's cache is invalid.
     * @return True if the object's cache is invalid, false otherwise.
     */
    bool IsCacheInvalid() const
    {
        return TESTFLAG(stateBitmap_, ObjectFlag::CACHE_INVALID);
    }

    /**
     * @brief Check if the object is a primary copy.
     * @return True if the object is a primary copy, false otherwise.
     */
    bool IsPrimaryCopy() const
    {
        return TESTFLAG(stateBitmap_, ObjectFlag::PRIMARY_COPY);
    }

    /**
     * @brief Check if the object is spilled.
     * @return True if the object is spilled, false otherwise.
     */
    bool IsSpilled() const
    {
        return TESTFLAG(stateBitmap_, ObjectFlag::SPILL_STATE);
    }

    /**
     * @brief Check if write back is done.
     * @return True if write back is done, false otherwise.
     */
    bool IsWriteBackDone() const
    {
        return TESTFLAG(stateBitmap_, ObjectFlag::WRITE_BACK_DONE);
    }

    /**
     * @brief Check if the object need to be deleted.
     * @return True if need to delete, false otherwise.
     */
    bool IsNeedToDelete() const
    {
        return TESTFLAG(stateBitmap_, ObjectFlag::NEED_TO_DELETE);
    }

    /**
     * @brief getter function to get the data format of the object.
     * @return the data format of the object.
     */
    DataFormat GetDataFormat() const
    {
        if (TESTFLAG(stateBitmap_, ObjectFlag::BINARY)) {
            return DataFormat::BINARY;
        } else if (TESTFLAG(stateBitmap_, ObjectFlag::LIST)) {
            return DataFormat::LIST;
        } else if (TESTFLAG(stateBitmap_, ObjectFlag::HETERO)) {
            return DataFormat::HETERO;
        } else {
            return DataFormat::HASH_MAP;
        }
    }

    /**
     * @brief A setter function for CacheInvalid.
     * @param[in] cacheInvliad If the cache is invalid.
     */
    void SetCacheInvalid(bool cacheInvalid)
    {
        CLEARFLAG(stateBitmap_, ObjectFlag::CACHE_INVALID);

        if (cacheInvalid) {
            SETFLAG(stateBitmap_, ObjectFlag::CACHE_INVALID);
        }
    }

    /**
     * @brief A setter function for PrimaryCopy.
     * @param[in] primaryCopy If the object is a prime copy.
     */
    void SetPrimaryCopy(bool primaryCopy)
    {
        CLEARFLAG(stateBitmap_, ObjectFlag::PRIMARY_COPY);

        if (primaryCopy) {
            SETFLAG(stateBitmap_, ObjectFlag::PRIMARY_COPY);
        }
    }

    /**
     * @brief A setter function for SpillState.
     * @param[in] spilledState If the object is a spilled.
     */
    void SetSpillState(bool spilledState)
    {
        CLEARFLAG(stateBitmap_, ObjectFlag::SPILL_STATE);

        if (spilledState) {
            SETFLAG(stateBitmap_, ObjectFlag::SPILL_STATE);
        }
    }

    /**
     * @brief A setter function for SetWriteBackDone.
     * @param[in] writeBackDone If write back is done.
     */
    void SetWriteBackDone(bool writeBackDone)
    {
        CLEARFLAG(stateBitmap_, ObjectFlag::WRITE_BACK_DONE);

        if (writeBackDone) {
            SETFLAG(stateBitmap_, ObjectFlag::WRITE_BACK_DONE);
        }
    }

    /**
     * @brief A setter function for NeedToDelete.
     * @param[in] needToDelete If the object need to be deleted.
     */
    void SetNeedToDelete(bool needToDelete)
    {
        CLEARFLAG(stateBitmap_, ObjectFlag::NEED_TO_DELETE);

        if (needToDelete) {
            SETFLAG(stateBitmap_, ObjectFlag::NEED_TO_DELETE);
        }
    }

    /**
     * @brief A setter function for data format of the object.
     * @param[in] dataFormat data format of the object.
     */
    void SetDataFormat(DataFormat dataFormat)
    {
        switch (dataFormat) {
            case DataFormat::BINARY:
                SETFLAG(stateBitmap_, ObjectFlag::BINARY);
                break;

            case DataFormat::LIST:
                SETFLAG(stateBitmap_, ObjectFlag::LIST);
                break;

            case DataFormat::HASH_MAP:
                SETFLAG(stateBitmap_, ObjectFlag::HASH_MAP);
                break;
            case DataFormat::HETERO:
                SETFLAG(stateBitmap_, ObjectFlag::HETERO);
                break;
            default:
                break;
        }
    }

    /**
     * @brief A setter function for IsEmpty.
     * @param[in] isEmpty If the object is a empty object.
     */
    void SetEmpty(bool isEmpty)
    {
        CLEARFLAG(stateBitmap_, ObjectFlag::IS_EMPTY);
        if (isEmpty) {
            SETFLAG(stateBitmap_, ObjectFlag::IS_EMPTY);
        }
    }

    /**
     * @brief Check if the object is empty
     * @return True if the object is empty.
     */
    bool IsEmpty() const
    {
        return TESTFLAG(stateBitmap_, ObjectFlag::IS_EMPTY);
    }

    void SetIncompleted(bool isIncompleted)
    {
        CLEARFLAG(stateBitmap_, ObjectFlag::IS_INCOMPLETE);
        if (isIncompleted) {
            SETFLAG(stateBitmap_, ObjectFlag::IS_INCOMPLETE);
        }
    }

    bool IsIncomplete() const
    {
        return TESTFLAG(stateBitmap_, ObjectFlag::IS_INCOMPLETE);
    }

private:
    ObjectFlag stateBitmap_;
};

class ModeInfo {
public:
    /**
     * @brief default constructor for mode info
     * @param initialBitmap a given bitmap
     */
    ModeInfo(ModeFlag initialBitmap = ModeFlag::NONE) : modeBitmap_(initialBitmap){};

    /**
     * @brief default constructor
     * @param consistencyType a specified consistency type
     * @param writeMode a specified write mode
     * @param cacheType a specified cache type
     */
    ModeInfo(ConsistencyType consistencyType, WriteMode writeMode, CacheType cacheType = CacheType::MEMORY)
    {
        modeBitmap_ = ModeFlag::NONE;

        SetConsistencyType(consistencyType);
        SetCacheType(cacheType);
        SetWriteMode(writeMode);
    }

    /**
     * @brief getter function to get the Consistency Type of the object.
     * @return the Consistency Type of the object.
     */
    ConsistencyType GetConsistencyType() const
    {
        return TESTFLAG(modeBitmap_, ModeFlag::PRAM) ? ConsistencyType::PRAM : ConsistencyType::CAUSAL;
    }

    /**
     * @brief getter function to get the Cache Type of the object.
     * @return the Cache Type of the object.
     */
    CacheType GetCacheType() const
    {
        return TESTFLAG(modeBitmap_, ModeFlag::DISK) ? CacheType::DISK : CacheType::MEMORY;
    }

    /**
     * @brief getter function to get the Write Mode of the object.
     * @return the Write Mode of the object.
     */
    WriteMode GetWriteMode() const
    {
        if (TESTFLAG(modeBitmap_, ModeFlag::NONE_L2_CACHE)) {
            return WriteMode::NONE_L2_CACHE;
        } else if (TESTFLAG(modeBitmap_, ModeFlag::WRITE_THROUGH_L2_CACHE)) {
            return WriteMode::WRITE_THROUGH_L2_CACHE;
        } else if (TESTFLAG(modeBitmap_, ModeFlag::WRITE_BACK_L2_CACHE)) {
            return WriteMode::WRITE_BACK_L2_CACHE;
        } else {
            return WriteMode::NONE_L2_CACHE_EVICT;
        }
    }

    /**
     * @brief A setter function for Consistency Type of the object.
     * @param[in] consistencyType Consistency Type of the object.
     */
    void SetConsistencyType(ConsistencyType consistencyType)
    {
        SETFLAG(modeBitmap_, consistencyType == ConsistencyType::PRAM ? ModeFlag::PRAM : ModeFlag::CAUSAL);
    }

    /**
     * @brief A setter function for Cache Type of the object.
     * @param[in] cacheType Cache Type of the object.
     */
    void SetCacheType(CacheType cacheType)
    {
        SETFLAG(modeBitmap_, cacheType == CacheType::MEMORY ? ModeFlag::MEMORY : ModeFlag::DISK);
    }

    /**
     * @brief A setter function for Write Mode of the object.
     * @param[in] writeMode Write Mode of the object.
     */
    void SetWriteMode(WriteMode writeMode)
    {
        switch (writeMode) {
            case WriteMode::NONE_L2_CACHE:
                SETFLAG(modeBitmap_, ModeFlag::NONE_L2_CACHE);
                break;

            case WriteMode::WRITE_THROUGH_L2_CACHE:
                SETFLAG(modeBitmap_, ModeFlag::WRITE_THROUGH_L2_CACHE);
                break;

            case WriteMode::WRITE_BACK_L2_CACHE:
                SETFLAG(modeBitmap_, ModeFlag::WRITE_BACK_L2_CACHE);
                break;
            case WriteMode::NONE_L2_CACHE_EVICT:
                SETFLAG(modeBitmap_, ModeFlag::NONE_L2_CACHE_EVICT);
                break;
            default:
                break;
        }
    }

private:
    ModeFlag modeBitmap_;
};

}  // namespace datasystem
#endif  // DATASYSTEM_OBJECT_CACHE_OBJECT_INTERFACE_BITMAP_H
