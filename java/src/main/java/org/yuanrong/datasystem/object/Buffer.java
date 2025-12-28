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

import java.nio.ByteBuffer;
import java.util.List;

/**
 * The Buffer interface to read and write data and publish data to the server.
 *
 * @since 2022-08-05
 */
public interface Buffer {
    /**
     * Publish mutable data to the server.
     */
    void publish();

    /**
     * Publish mutable data to the server.
     *
     * @param nestedKeys Object key of the nested object.
     */
    void publish(List<String> nestedKeys);

    /**
     * Publish immutable data to the server.
     */
    void seal();

    /**
     * Publish immutable data to the server.
     *
     * @param nestedKeys Object key of the nested object.
     */
    void seal(List<String> nestedKeys);

    /**
     * A write lock is executed on the memory to protect the memory from concurrent reads and writes.
     */
    void wLatch();

    /**
     * A write lock is executed on the memory to protect the memory from concurrent reads and writes.
     *
     * @param timeoutSec Try-lock timeout, seconds.
     */
    void wLatch(int timeoutSec);

    /**
     * A Read lock is executed on the memory to protect the memory from concurrent writes (allow concurrent reads).
     */
    void rLatch();

    /**
     * A Read lock is executed on the memory to protect the memory from concurrent writes (allow concurrent reads).
     *
     * @param timeoutSec Try-lock timeout, default value is 60 seconds.
     */
    void rLatch(int timeoutSec);

    /**
     * Unlock the write latch on memory.
     */
    void unWLatch();

    /**
     * Unlock the read latch on memory.
     */
    void unRLatch();

    /**
     * Write data to buffer.
     *
     * @param buffer the object data.
     */
    void memoryCopy(ByteBuffer buffer);

    /**
     * Gets a mutable ByteBuffer object.
     *
     * @return a ByteBuffer object.
     */
    ByteBuffer mutableData();

    /**
     * Gets a immutable ByteBuffer object.
     *
     * @return a ByteBuffer object.
     */
    ByteBuffer immutableData();

    /**
     * Invalidates data on the current host.
     */
    void invalidateBuffer();

    /**
     * Get the data size of the buffer.
     *
     * @return The data size of the buffer.
     */
    long getSize();

    /**
     * To delete buffer.
     */
    void close();
}