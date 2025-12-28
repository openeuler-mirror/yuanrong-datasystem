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

import org.yuanrong.datasystem.DataSystemException;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The Buffer implement, visible in this package.
 *
 * @since 2022-08-05
 */
class BufferImpl implements Buffer {
    // The default timeout seconds is 60s
    private static final int DEFAULT_TIMEOUT_SEC = 60;

    // for bufferPtr.
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock rLock = rwLock.readLock();
    private final Lock wLock = rwLock.writeLock();

    // Reference to a jni client object.
    private long bufferPtr;

    /**
     * BufferImpl Structure Method.
     *
     * @param bufferPtr The buffer pointer.
     */
    public BufferImpl(long bufferPtr) {
        this.bufferPtr = bufferPtr;
    }

    /**
     * The finalize() method is used to release the buffer pointer on the JNI side, but the operation cannot be
     * guaranteed to be executed.
     */
    @Override
    protected void finalize() {
        if (bufferPtr != 0) {
            freeBufferPtr(bufferPtr);
        }
    }

    @Override
    public void publish() {
        rLock.lock();
        try {
            ensureOpen();
            publishNative(bufferPtr, null);
        } finally {
            rLock.unlock();
        }
    }

    @Override
    public void publish(List<String> nestedKeys) {
        rLock.lock();
        try {
            ensureOpen();
            publishNative(bufferPtr, nestedKeys);
        } finally {
            rLock.unlock();
        }
    }

    @Override
    public void seal() {
        rLock.lock();
        try {
            ensureOpen();
            sealNative(bufferPtr, null);
        } finally {
            rLock.unlock();
        }
    }

    @Override
    public void seal(List<String> nestedKeys) {
        rLock.lock();
        try {
            ensureOpen();
            sealNative(bufferPtr, nestedKeys);
        } finally {
            rLock.unlock();
        }
    }

    @Override
    public void wLatch() {
        rLock.lock();
        try {
            ensureOpen();
            wLatchNative(bufferPtr, DEFAULT_TIMEOUT_SEC);
        } finally {
            rLock.unlock();
        }
    }

    @Override
    public void wLatch(int timeoutSec) {
        rLock.lock();
        try {
            ensureOpen();
            wLatchNative(bufferPtr, timeoutSec);
        } finally {
            rLock.unlock();
        }
    }

    @Override
    public void rLatch() {
        rLock.lock();
        try {
            ensureOpen();
            rLatchNative(bufferPtr, DEFAULT_TIMEOUT_SEC);
        } finally {
            rLock.unlock();
        }
    }

    @Override
    public void rLatch(int timeoutSec) {
        rLock.lock();
        try {
            ensureOpen();
            rLatchNative(bufferPtr, timeoutSec);
        } finally {
            rLock.unlock();
        }
    }

    @Override
    public void unWLatch() {
        rLock.lock();
        try {
            ensureOpen();
            unWLatchNative(bufferPtr);
        } finally {
            rLock.unlock();
        }
    }

    @Override
    public void unRLatch() {
        rLock.lock();
        try {
            ensureOpen();
            unRLatchNative(bufferPtr);
        } finally {
            rLock.unlock();
        }
    }

    @Override
    public void memoryCopy(ByteBuffer buffer) {
        rLock.lock();
        try {
            ensureOpen();
            if (buffer.isDirect()) {
                memoryCopyDirectBuffer(bufferPtr, buffer);
            } else {
                memoryCopyHeapBuffer(bufferPtr, buffer.array(), buffer.limit());
            }
        } finally {
            rLock.unlock();
        }
    }

    @Override
    public ByteBuffer mutableData() {
        rLock.lock();
        try {
            ensureOpen();
            return mutableDataNative(bufferPtr);
        } finally {
            rLock.unlock();
        }
    }

    @Override
    public ByteBuffer immutableData() {
        rLock.lock();
        try {
            ensureOpen();
            ByteBuffer data = immutableDataNative(bufferPtr);
            return data.asReadOnlyBuffer();
        } finally {
            rLock.unlock();
        }
    }


    @Override
    public void close() {
        wLock.lock();
        try {
            if (bufferPtr != 0) {
                freeBufferPtr(bufferPtr);
            }
            bufferPtr = 0;
        } finally {
            wLock.unlock();
        }
    }

    @Override
    public void invalidateBuffer() {
        rLock.lock();
        try {
            ensureOpen();
            invalidateBufferNative(bufferPtr);
        } finally {
            rLock.unlock();
        }
    }

    @Override
    public long getSize() {
        rLock.lock();
        try {
            ensureOpen();
            return getSizeNative(bufferPtr);
        } finally {
            rLock.unlock();
        }
    }

    /**
     * Checks to make sure that object client has not been closed.
     */
    private void ensureOpen() {
        if (bufferPtr == 0) {
            throw new DataSystemException(this.getClass().getName() + ": buffer closed");
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        BufferImpl otherBufferImpl = (BufferImpl) other;
        return bufferPtr == otherBufferImpl.bufferPtr;
    }

    @Override
    public int hashCode() {
        return Objects.hash(bufferPtr);
    }

    @Override
    public String toString() {
        return super.toString();
    }

    private static native void publishNative(long bufferPtr, List<String> nestedKeys);

    private static native void sealNative(long bufferPtr, List<String> nestedKeys);

    private static native void wLatchNative(long bufferPtr, int timeoutSec);

    private static native void rLatchNative(long bufferPtr, int timeoutSec);

    private static native void unWLatchNative(long bufferPtr);

    private static native void unRLatchNative(long bufferPtr);

    private static native void memoryCopyDirectBuffer(long bufferPtr, ByteBuffer buffer);

    private static native void memoryCopyHeapBuffer(long bufferPtr, byte[] bytes, long len);

    private static native ByteBuffer mutableDataNative(long bufferPtr);

    private static native ByteBuffer immutableDataNative(long bufferPtr);

    private static native void invalidateBufferNative(long bufferPtr);

    private static native long getSizeNative(long bufferPtr);

    private static native void freeBufferPtr(long bufferPtr);
}