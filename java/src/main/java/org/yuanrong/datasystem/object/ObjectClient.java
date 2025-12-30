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

import org.yuanrong.datasystem.ConnectOptions;
import org.yuanrong.datasystem.DataSystemException;
import org.yuanrong.datasystem.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The client of object.
 *
 * @since 2022-08-05
 */
public class ObjectClient {
    static {
        Utils.loadLibrary();
    }

    // for clientPtr.
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock rLock = rwLock.readLock();
    private final Lock wLock = rwLock.writeLock();

    // Reference to a jni client object.
    private long clientPtr;

    /**
     * Empty constructors for connect to worker and create a object cache instance.
     */
    public ObjectClient() {
        this.clientPtr = init(new ConnectOptions());
    }

    /**
     * Connect to worker and create a object cache instance.
     *
     * @param connectOptions The parameters for establishing a connection.
     */
    public ObjectClient(ConnectOptions connectOptions) {
        this.clientPtr = init(connectOptions);
    }

    /**
     * The finalize() method is used to release the object client pointer on the JNI side, but the operation cannot be
     * guaranteed to be executed.
     */
    @Override
    protected void finalize() {
        if (clientPtr != 0) {
            freeObjectClientPtr(clientPtr);
        }
    }

    /**
     * Invoke worker client to create an object.
     *
     * @param objectKey The ID of the object to create.
     * @param size     The size in bytes of object.
     * @param param    The create parameters.
     * @return The buffer for the object.
     */
    public Buffer create(String objectKey, int size, CreateParam param) {
        rLock.lock();
        try {
            ensureOpen();
            long bufferPtr = createNative(clientPtr, objectKey, size, param);
            return new BufferImpl(bufferPtr);
        } finally {
            rLock.unlock();
        }
    }

    /**
     * Invoke worker client to put an object (publish semantics).
     *
     * @param objectKey        The ID of the object.
     * @param buffer          The ByteBuffer object of the user.
     * @param param           The create parameters.
     * @param nestedObjectKeys Objects that depend on objectKey.
     */
    public void put(String objectKey, ByteBuffer buffer, CreateParam param, List<String> nestedObjectKeys) {
        rLock.lock();
        try {
            ensureOpen();
            if (buffer.isDirect()) {
                putDirectBufferNative(clientPtr, objectKey, buffer, param, nestedObjectKeys);
            } else {
                putHeapBufferNative(clientPtr, objectKey, buffer.array(), buffer.limit(), param, nestedObjectKeys);
            }
        } finally {
            rLock.unlock();
        }
    }

    /**
     * Invoke worker client to put an object (publish semantics).
     *
     * @param objectKey        The ID of the object.
     * @param buffer          The ByteBuffer object of the user.
     * @param param           The create parameters.
     */
    public void put(String objectKey, ByteBuffer buffer, CreateParam param) {
        rLock.lock();
        try {
            ensureOpen();
            if (buffer.isDirect()) {
                putDirectBufferNative(clientPtr, objectKey, buffer, param, new ArrayList<>());
            } else {
                putHeapBufferNative(clientPtr, objectKey, buffer.array(), buffer.limit(), param, new ArrayList<>());
            }
        } finally {
            rLock.unlock();
        }
    }

    /**
     * Invoke worker client to get all buffers of all the given object keys.
     *
     * @param objectKeys The vector of the object key.
     * @param timeoutMs   The timeout of the get operation.
     * @return The objects List.
     */
    public List<Buffer> get(List<String> objectKeys, int timeoutMs) {
        rLock.lock();
        try {
            ensureOpen();
            return getNative(clientPtr, objectKeys, timeoutMs);
        } finally {
            rLock.unlock();
        }
    }

    /**
     * Increase the global reference count to objects in the data system.
     *
     * @param objectKeys The object keys to increase, it cannot be empty.
     * @return Increase failed object keys list.
     */
    public List<String> gIncreaseRef(List<String> objectKeys) {
        rLock.lock();
        try {
            ensureOpen();
            return gIncreaseRefNative(clientPtr, objectKeys);
        } finally {
            rLock.unlock();
        }
    }

    /**
     * Decrease the global reference count to objects in the data system.
     *
     * @param objectKeys The object keys to decrease, it cannot be empty.
     * @return Decrease failed object keys list.
     */
    public List<String> gDecreaseRef(List<String> objectKeys) {
        rLock.lock();
        try {
            ensureOpen();
            return gDecreaseRefNative(clientPtr, objectKeys);
        } finally {
            rLock.unlock();
        }
    }

    @Override
    public String toString() {
        return super.toString();
    }


    /**
     * Checks to make sure that object client has not been closed.
     */
    private void ensureOpen() {
        if (clientPtr == 0) {
            throw new DataSystemException(this.getClass().getName() + ": Client closed");
        }
    }

    /**
     * Obtains the number of global reference counting of a specified object.
     *
     * @param objectKeys The object keys to decrease, it cannot be empty.
     * @return Number of reference counting.
     */
    public int queryGlobalRefNum(String objectKey) {
        rLock.lock();
        try {
            ensureOpen();
            return queryGlobalRefNumNative(clientPtr, objectKey);
        } finally {
            rLock.unlock();
        }
    }

    /**
     * To delete object client.
     */
    public void close() {
        wLock.lock();
        try {
            if (clientPtr != 0) {
                freeObjectClientPtr(clientPtr);
            }
            clientPtr = 0;
        } finally {
            wLock.unlock();
        }
    }

    /**
     * To update token for the object client.
     *
     * @param tokenBytes The token bytes for the object client.
     */
    public void updateToken(byte[] tokenBytes) {
        rLock.lock();
        try {
            ensureOpen();
            updateTokenNative(clientPtr, tokenBytes);
        } finally {
            rLock.unlock();
        }
    }

    /**
     * To update aksk for the object client.
     *
     * @param accessKey The accessKey of the object client.
     * @param secretKeyBytes The secretKey bytes of the object client.
     */
    public void updateAkSk(String accessKey, byte[] secretKeyBytes) {
        rLock.lock();
        try {
            ensureOpen();
            updateAkSkNative(clientPtr, accessKey, secretKeyBytes);
        } finally {
            rLock.unlock();
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientPtr);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        ObjectClient otherObjectClient = (ObjectClient) other;
        return clientPtr == otherObjectClient.clientPtr;
    }

    /**
     * Use JNI to invoke the Init interface of C++.
     *
     * @param connectOptions The parameters for establishing a connection.
     * @return The ObjectClient pointer value.
     */
    private static native long init(ConnectOptions connectOptions);

    /**
     * Use JNI to invoke the Create interface of C++.
     *
     * @param clientPtr The ObjectClient pointer.
     * @param objectKey  The ID of the object to create.
     * @param size      The size in bytes of object.
     * @param param     The create parameters.
     * @return The Buffer pointer value.
     */
    private static native long createNative(long clientPtr, String objectKey, int size, CreateParam param);

    /**
     * Use JNI to invoke the Put interface of C++.
     *
     * @param clientPtr       The ObjectClient pointer.
     * @param objectKey        The ID of the object.
     * @param buffer          The ByteBuffer object of the user.
     * @param param           The create parameters.
     * @param nestedObjectKeys Objects that depend on objectKey.
     */
    private static native void putDirectBufferNative(long clientPtr, String objectKey, ByteBuffer buffer,
    CreateParam param, List<String> nestedObjectKeys);

    /**
     * Use JNI to invoke the Put interface of C++.
     *
     * @param clientPtr       The ObjectClient pointer.
     * @param objectKey        The ID of the object.
     * @param bufferPtr       The array of ByteBuffer object of the user.
     * @param len             The length of ByteBuffer object of the user.
     * @param param           The create parameters.
     * @param nestedObjectKeys Objects that depend on objectKey.
     */
    private static native void putHeapBufferNative(long clientPtr, String objectKey, byte[] bufferPtr, long len,
    CreateParam param, List<String> nestedObjectKeys);

    /**
     * Use JNI to invoke the Get interface of C++.
     *
     * @param clientPtr The ObjectClient pointer.
     * @param objectKeys The vector of the object key.
     * @param timeoutMs   The timeout of the get operation.
     * @return The objects List.
     */
    private static native List<Buffer> getNative(long clientPtr, List<String> objectKeys, int timeoutMs);

    /**
     * Use JNI to invoke the GIncreaseRef interface of C++.
     *
     * @param clientPtr The ObjectClient pointer.
     * @param objectKeys The object keys to increase, it cannot be empty.
     * @return Increase failed object keys list.
     */
    private static native List<String> gIncreaseRefNative(long clientPtr, List<String> objectKeys);

    /**
     * Use JNI to invoke the GDecreaseRef interface of C++.
     *
     * @param clientPtr The ObjectClient pointer.
     * @param objectKeys The object keys to decrease, it cannot be empty.
     * @return Decrease failed object keys list.
     */
    private static native List<String> gDecreaseRefNative(long clientPtr, List<String> objectKeys);

    /**
     * Use JNI to invoke the QueryGlobalRefNum interface of C++.
     *
     * @param clientPtr The ObjectClient pointer.
     * @param objectKeys The object keys to decrease, it cannot be empty.
     * @return Number of reference counting.
     */
    private static native int queryGlobalRefNumNative(long clientPtr, String objectKey);

    /**
     * This method is used to delete the client pointer that is released at the JNI layer.
     *
     * @param clientPtr The ObjectClient pointer.
     */
    private static native void freeObjectClientPtr(long clientPtr);

    /**
     * Use JNI to invoke the UpdateToken interface of C++.
     *
     * @param clientPtr The ObjectClient pointer.
     * @param tokenBytes The token bytes for ObjectClient.
     */
    private static native void updateTokenNative(long clientPtr, byte[] tokenBytes);

    /**
     * Use JNI to invoke the UpdateAkSk interface of C++.
     *
     * @param clientPtr The ObjectClient pointer.
     * @param accessKeyJO The accessKey for ObjectClient.
     * @param secretKeyBytes The secretKey bytes for ObjectClient.
     */
    private static native void updateAkSkNative(long clientPtr, String accessKey, byte[] secretKeyBytes);
}