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

package org.yuanrong.datasystem.kv;

import org.yuanrong.datasystem.DataSystemException;
import org.yuanrong.datasystem.WriteMode;

import java.util.Locale;
import java.util.Objects;

/**
 * The SetParam class.
 *
 * @since 2022-09-14
 */
public class SetParam {
    /**
     * Specifies the WriteMode mode.
     */
    private WriteMode writeMode = WriteMode.NONE_L2_CACHE;

    /**
     * Specifies the expired time. The data will be deleted automatically after expired.
     * If the value is greater than 0, the data will be deleted automatically after expired.
     * And if set to 0, the data need to be manually deleted.
     */
    private long ttlSecond = 0L;

    public void setWriteMode(WriteMode writeMode) {
        this.writeMode = writeMode;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        SetParam otherParam = (SetParam) other;
        return writeMode == otherParam.writeMode && ttlSecond == otherParam.ttlSecond;
    }

    public void setTtlSecond(long ttlSecond) {
        // The range should be [0, UINT32_MAX]
        long uint32Max = 4294967295L;
        if (ttlSecond < 0 || ttlSecond > uint32Max) {
            String info = String.format(Locale.ENGLISH,
                    "Invalid ttlSecond: %d, the range should be [0, 4294967295]", ttlSecond);
            throw new DataSystemException(2, info);
        }
        this.ttlSecond = ttlSecond;
    }

    @Override
    public String toString() {
        return "SetParam{"
            + "writeMode=" + writeMode
            + ", ttlSecond=" + ttlSecond
            + '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(writeMode, ttlSecond);
    }
}