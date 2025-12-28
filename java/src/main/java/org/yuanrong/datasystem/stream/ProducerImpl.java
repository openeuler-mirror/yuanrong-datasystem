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

package org.yuanrong.datasystem.stream;

import org.yuanrong.datasystem.DataSystemException;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The producer implement, visible in this package.
 *
 * @since 2022-10-08
 */
class ProducerImpl implements Producer {
    // for producerPtr.
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock rLock = rwLock.readLock();
    private final Lock wLock = rwLock.writeLock();

    // Point to jni producer object.
    private long producerPtr;

    public ProducerImpl(long producerPtr) {
        this.producerPtr = producerPtr;
    }

    @Override
    public void send(ByteBuffer buffer) {
        rLock.lock();
        try {
            ensureOpen();
            if (buffer.isDirect()) {
                sendDirectBufferDefaultTimeout(producerPtr, buffer);
            } else {
                sendHeapBufferDefaultTimeout(producerPtr, buffer.array(), buffer.limit());
            }
        } finally {
            rLock.unlock();
        }
    }

    @Override
    public void send(ByteBuffer buffer, int timeoutMs) {
        rLock.lock();
        try {
            ensureOpen();
            if (buffer.isDirect()) {
                sendDirectBuffer(producerPtr, buffer, timeoutMs);
            } else {
                sendHeapBuffer(producerPtr, buffer.array(), buffer.limit(), timeoutMs);
            }
        } finally {
            rLock.unlock();
        }
    }


    /**
     * Checks to make sure that producer has not been closed.
     */
    private void ensureOpen() {
        if (producerPtr == 0) {
            throw new DataSystemException(this.getClass().getName() + ": Producer closed");
        }
    }

    @Override
    public void close() {
        wLock.lock();
        try {
            if (producerPtr == 0) {
                throw new DataSystemException("Producer has been closed");
            }
            close(this.producerPtr);
            producerPtr = 0;
        } finally {
            wLock.unlock();
        }
    }

    @Override
    protected void finalize() {
        if (producerPtr != 0) {
            freeJNIPtrNative(producerPtr);
        }
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(producerPtr);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        ProducerImpl otherProducerImpl = (ProducerImpl) other;
        return producerPtr == otherProducerImpl.producerPtr;
    }

    private static native void sendHeapBufferDefaultTimeout(long producerPtr, byte[] bytes, long len);

    private static native void sendDirectBufferDefaultTimeout(long producerPtr, ByteBuffer buffers);

    private static native void sendHeapBuffer(long producerPtr, byte[] bytes, long len, int timeoutMs);

    private static native void sendDirectBuffer(long producerPtr, ByteBuffer buffers, int timeoutMs);

    private static native void close(long producerPtr);

    private static native void freeJNIPtrNative(long producerPtr);
}