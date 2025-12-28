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

package org.yuanrong.datasystem.object;

import org.yuanrong.datasystem.ConsistencyType;

import java.util.Objects;

/**
 * The CreateParam class.
 *
 * @since 2022-08-05
 */
public class CreateParam {
    /**
     * Specifies the ConsistencyType mode.
     */
    private ConsistencyType consistencyType = ConsistencyType.PRAM;

    @Override
    public String toString() {
        return super.toString();
    }

    public void setConsistencyType(ConsistencyType consistencyType) {
        this.consistencyType = consistencyType;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        CreateParam otherCreateParam = (CreateParam) other;
        return consistencyType == otherCreateParam.consistencyType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(consistencyType);
    }
}