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

import org.yuanrong.datasystem.ConnectOptions;
import org.yuanrong.datasystem.DataSystemException;
import org.yuanrong.datasystem.Utils;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The client of state.
 *
 * @since 2022-08-25
 */
public class KVClient {
    static {
        Utils.loadLibrary();
    }

    // for kvClientPtr.
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock rLock = rwLock.readLock();
    private final Lock wLock = rwLock.writeLock();

    // Reference to a jni state client object.
    private long kvClientPtr;

    /**
     * Connect to worker and create a state client instance.
     */
    public KVClient() {
        this(new ConnectOptions());
    }

    /**
     * Connect to worker and create a state client instance.
     *
     * @param connectOptions The parameters for establishing a connection.
     */
    public KVClient(ConnectOptions connectOptions) {
        this.kvClientPtr = init(connectOptions);
    }

    /**
     * The finalize() method is used to release the object client pointer on the JNI
     * side, but the operation cannot be
     * guaranteed to be executed.
     */
    @Override
    protected void finalize() {
        if (kvClientPtr != 0) {
            freeKVClientPtr(kvClientPtr);
        }
    }

    /**
     * Invoke worker client to set the value of a key.
     *
     * @param key   The key.
     * @param value The value for the key.
     */
    public void set(String key, ByteBuffer value) {
        rLock.lock();
        try {
            ensureOpen();
            if (value.isDirect()) {
                setDirectBufferNative(kvClientPtr, key, value, new SetParam());
            } else {
                setHeapBufferNative(kvClientPtr, key, value.array(), value.limit(), new SetParam());
            }
        } finally {
            rLock.unlock();
        }
    }

    /**
     * Invoke worker client to set the value of a key.
     *
     * @param key   The key.
     * @param value The value for the key.
     * @param param The set parameters.
     */
    public void set(String key, ByteBuffer value, SetParam param) {
        rLock.lock();
        try {
            ensureOpen();
            if (value.isDirect()) {
                setDirectBufferNative(kvClientPtr, key, value, param);
            } else {
                setHeapBufferNative(kvClientPtr, key, value.array(), value.limit(), param);
            }
        } finally {
            rLock.unlock();
        }
    }

    /**
     * Invoke worker client to get the value of a key.
     *
     * @param key The key.
     * @return The value for the key.
     */
    public ByteBuffer get(String key) {
        rLock.lock();
        try {
            ensureOpen();
            return getKeyNative(kvClientPtr, key, 0);
        } finally {
            rLock.unlock();
        }
    }

    /**
     * Invoke worker client to get the value of a key.
     *
     * @param key       The key.
     * @param timeoutMs TimeoutMs of waiting for the result return if object not
     *                  ready. A positive integer number
     *                  required. 0 means no waiting time allowed.
     * @return The value for the key.
     */
    public ByteBuffer get(String key, int timeoutMs) {
        rLock.lock();
        try {
            ensureOpen();
            return getKeyNative(kvClientPtr, key, timeoutMs);
        } finally {
            rLock.unlock();
        }
    }

    /**
     * Invoke worker client to get the values of all the given keys.
     *
     * @param keys The vector of the keys.
     * @return The values list.
     */
    public List<ByteBuffer> get(List<String> keys) {
        rLock.lock();
        try {
            ensureOpen();
            return getKeysNative(kvClientPtr, keys, 0);
        } finally {
            rLock.unlock();
        }
    }

    /**
     * Invoke worker client to get the values of all the given keys.
     *
     * @param keys      The vector of the keys.
     * @param timeoutMs TimeoutMs of waiting for the result return if object not
     *                  ready. A positive integer number
     *                  required. 0 means no waiting time allowed.
     * @return The values list.
     */
    public List<ByteBuffer> get(List<String> keys, int timeoutMs) {
        rLock.lock();
        try {
            ensureOpen();
            return getKeysNative(kvClientPtr, keys, timeoutMs);
        } finally {
            rLock.unlock();
        }
    }

    /**
     * Invoke worker client to delete a key.
     *
     * @param key The key.
     */
    public void del(String key) {
        rLock.lock();
        try {
            ensureOpen();
            delKeyNative(kvClientPtr, key);
        } finally {
            rLock.unlock();
        }
    }

    /**
     * Invoke worker client to delete all the given keys.
     *
     * @param keys The vector of the keys.
     * @return The failed delete keys.
     */
    public List<String> del(List<String> keys) {
        rLock.lock();
        try {
            ensureOpen();
            return delKeysNative(kvClientPtr, keys);
        } finally {
            rLock.unlock();
        }
    }

    /**
     * Generate a unique key for SET.
     *
     * @return The unique key, if the key fails to be generated, an empty string is returned.
     */
    public String generateKey() {
        rLock.lock();
        try {
            ensureOpen();
            return generateKeyNative(kvClientPtr);
        } finally {
            rLock.unlock();
        }
    }

    /**
     * Checks to make sure that state client has not been closed.
     */
    private void ensureOpen() {
        if (kvClientPtr == 0) {
            throw new DataSystemException(this.getClass().getName() + ": Client closed");
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(rwLock, rLock, wLock, kvClientPtr);
    }

    /**
     * To delete state client.
     */
    public void close() {
        wLock.lock();
        try {
            if (kvClientPtr != 0) {
                freeKVClientPtr(kvClientPtr);
            }
            kvClientPtr = 0;
        } finally {
            wLock.unlock();
        }
    }

    @Override
    public String toString() {
        return super.toString();
    }

    /**
     * Use JNI to invoke the Init interface of C++.
     *
     * @param connectOptions The parameters for establishing a connection.
     * @return The StateClient pointer value.
     */
    private static native long init(ConnectOptions connectOptions);

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        KVClient that = (KVClient) other;
        return kvClientPtr == that.kvClientPtr && Objects.equals(rwLock, that.rwLock)
                && Objects.equals(rLock, that.rLock) && Objects.equals(wLock, that.wLock);
    }

    /**
     * Use JNI to invoke the Set interface of C++.
     *
     * @param kvClientPtr The StateClient pointer.
     * @param key       The key.
     * @param value     The value for the key.
     * @param param     The set parameters.
     */
    private static native void setDirectBufferNative(long kvClientPtr, String key, ByteBuffer value, SetParam param);

    /**
     * Use JNI to invoke the Set interface of C++.
     *
     * @param kvClientPtr The StateClient pointer.
     * @param key       The key.
     * @param bufferPtr The array of ByteBuffer object of the user.
     * @param size      The length of ByteBuffer object of the user.
     * @param param     The set parameters.
     */
    private static native void setHeapBufferNative(long kvClientPtr, String key, byte[] bufferPtr, long size,
            SetParam param);

    /**
     * Use JNI to invoke the Get interface of C++.
     *
     * @param kvClientPtr The StateClient pointer.
     * @param key       The key.
     * @param timeoutMs The timeout of the get operation.
     * @return The value for the key.
     */
    private static native ByteBuffer getKeyNative(long kvClientPtr, String key, int timeoutMs);

    /**
     * Use JNI to invoke the Get interface of C++.
     *
     * @param kvClientPtr The StateClient pointer.
     * @param keys      The vector of the keys.
     * @param timeoutMs The timeout of the get operation.
     * @return The values list.
     */
    private static native List<ByteBuffer> getKeysNative(long kvClientPtr, List<String> keys, int timeoutMs);

    /**
     * Use JNI to invoke the Del interface of C++.
     *
     * @param kvClientPtr The StateClient pointer.
     * @param key       The key.
     */
    private static native void delKeyNative(long kvClientPtr, String key);

    /**
     * Use JNI to invoke the Del interface of C++.
     *
     * @param kvClientPtr The StateClient pointer.
     * @param keys      The vector of the keys.
     * @return The failed delete keys.
     */
    private static native List<String> delKeysNative(long kvClientPtr, List<String> keys);

    /**
     * This method is used to delete the client pointer that is released at the JNI
     * layer.
     *
     * @param kvClientPtr The StateClient pointer.
     */
    private static native void freeKVClientPtr(long kvClientPtr);

    /**
     * Use JNI to invoke the GenerateKey interface of C++.
     *
     * @param kvClientPtr The StateClient pointer.
     * @return The unique key, if the key fails to be generated, an empty string is returned.
     */
    private static native String generateKeyNative(long kvClientPtr);
}