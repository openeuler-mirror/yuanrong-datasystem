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

package org.yuanrong.datasystem;

import java.util.Objects;

/**
 * The datasystem exception.
 *
 * @since 2022-10-08
 */
public class DataSystemException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /**
     * The error code for the exception.
     */
    private int errorCode;

    /**
     * Create a DataSystemException with a given parameter
     *
     * @param errorCode The error code for the exception.
     * @param message   Exception detail message.
     */
    public DataSystemException(int errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    /**
     * Create a DataSystemException with a given parameter
     *
     * @param message Exception detail message.
     */
    public DataSystemException(String message) {
        this(-1, message);
    }

    /**
     * Create a DataSystemException with a given parameter
     *
     * @param errorCode The error code for the exception.
     * @param message   Exception detail message.
     * @param cause     Exception cause.
     */
    public DataSystemException(int errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    /**
     * Create a DataSystemException with a given parameter
     *
     * @param message Exception detail message.
     * @param cause   Exception cause.
     */
    public DataSystemException(String message, Throwable cause) {
        this(-1, message, cause);
    }

    /**
     * Get error code.
     *
     * @return The error code for the exception.
     */
    public int getErrorCode() {
        return errorCode;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        DataSystemException otherDataSystemException = (DataSystemException) other;
        return errorCode == otherDataSystemException.errorCode
                && super.getMessage().equals(otherDataSystemException.getMessage())
                && super.getCause().equals(otherDataSystemException.getCause());
    }

    @Override
    public int hashCode() {
        return Objects.hash(errorCode, super.getMessage(), super.getCause());
    }

    @Override
    public String toString() {
        return "DataSystemException{" + "errorCode=" + errorCode + ", message=" + super.getMessage() + '}';
    }
}