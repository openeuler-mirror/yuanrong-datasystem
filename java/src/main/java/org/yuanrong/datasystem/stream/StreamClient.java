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

import org.yuanrong.datasystem.ConnectOptions;
import org.yuanrong.datasystem.DataSystemException;
import org.yuanrong.datasystem.Utils;

import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The client of stream
 *
 * @since 2022-05-07
 */
public class StreamClient {
    // Default config to create producer
    private static final long DEFAULT_FLUSH_TIMEOUT_MS = 5L; // 5ms
    private static final long DEFAULT_PAGE_SIZE_BYTE = 1024 * 1024L; // 1MB
    private static final long DEFAULT_MAX_STREAM_SIZE_BYTE = 1024 * 1024 * 1024L; // 1GB
    private static final long DEFAULT_RETAIN_FOR_NUM_CONSUMERS = 0L;
    private static final long DEFAULT_RESERVE_SIZE = 0L;

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
     * Connect to worker and create a stream cache instance
     *
     * @param connectOptions The parameters for establishing a connection.
     */
    public StreamClient(ConnectOptions connectOptions) {
        this(connectOptions, false);
    }

    /**
     * Connect to worker and create a stream cache instance
     *
     * @param connectOptions The parameters for establishing a connection.
     * @param shouldReportWorkerLost Whether to report to the caller when worker had crashed or worker lost the client.
     */
    public StreamClient(ConnectOptions connectOptions, boolean shouldReportWorkerLost) {
        this.clientPtr = init(connectOptions, shouldReportWorkerLost);
    }

    /**
     * The finalize() method is used to release the object client pointer on the JNI side, but the operation cannot be
     * guaranteed to be executed.
     */
    @Override
    protected void finalize() {
        if (clientPtr != 0) {
            freeJNIPtrNative(clientPtr);
        }
    }

    /**
     * Create one Producer to send element.
     *
     * @param streamName The name of the stream.
     * @return The output Producer interface that user can use it to send element.
     */
    public Producer createProducer(String streamName) {
        return this.createProducer(streamName, DEFAULT_FLUSH_TIMEOUT_MS, DEFAULT_PAGE_SIZE_BYTE,
                DEFAULT_MAX_STREAM_SIZE_BYTE, false, DEFAULT_RETAIN_FOR_NUM_CONSUMERS, false,
                DEFAULT_RESERVE_SIZE);
    }

    /**
     * Create one Producer to send element.
     *
     * @param streamName       The name of the stream.
     * @param delayFlushTimeMs The time used in automatic flush after send.
     * @return The output Producer interface that user can use it to send element.
     */
    public Producer createProducer(String streamName, long delayFlushTimeMs) {
        return this.createProducer(streamName, delayFlushTimeMs, DEFAULT_PAGE_SIZE_BYTE,
                DEFAULT_MAX_STREAM_SIZE_BYTE, false, DEFAULT_RETAIN_FOR_NUM_CONSUMERS, false,
                DEFAULT_RESERVE_SIZE);
    }

    /**
     * Create one Producer to send element.
     *
     * @param streamName       The name of the stream.
     * @param delayFlushTimeMs The time used in automatic flush after send.
     * @param pageSizeByte     The size used in allocate page, default size 1MB,
     *                         must be a multiple of 4KB.
     * @return The output Producer interface that user can use it to send element.
     */
    public Producer createProducer(String streamName, long delayFlushTimeMs, long pageSizeByte) {
        return createProducer(streamName, delayFlushTimeMs, pageSizeByte,
                DEFAULT_MAX_STREAM_SIZE_BYTE, false, DEFAULT_RETAIN_FOR_NUM_CONSUMERS, false,
                DEFAULT_RESERVE_SIZE);
    }

    /**
     * Create one Producer to send element.
     *
     * @param streamName       The name of the stream.
     * @param delayFlushTimeMs The time used in automatic flush after send.
     * @param pageSizeByte     The size used in allocate page, default size 1MB,
     *                         must be a multiple of 4KB.
     * @param maxStreamSize    The max stream size in worker, default size 1GB,
     *                         must greater then 64KB and less than the
     *                         shared memory size.
     * @return The output Producer interface that user can use it to send element.
     */
    public Producer createProducer(String streamName, long delayFlushTimeMs, long pageSizeByte, long maxStreamSize) {
        return createProducer(streamName, delayFlushTimeMs, pageSizeByte, maxStreamSize,
                false, DEFAULT_RETAIN_FOR_NUM_CONSUMERS, false,
                DEFAULT_RESERVE_SIZE);
    }

    /**
     * Create one Producer to send element.
     *
     * @param streamName       The name of the stream.
     * @param delayFlushTimeMs The time used in automatic flush after send.
     * @param pageSizeByte     The size used in allocate page, default size 1MB,
     *                         must be a multiple of 4KB.
     * @param maxStreamSize    The max stream size in worker, default size 1GB,
     *                         must greater then 64KB and less than the
     *                         shared memory size.
     * @param autoCleanup    Should auto delete when last consumer/producer exit, default false
     * @return The output Producer interface that user can use it to send element.
     */
    public Producer createProducer(String streamName, long delayFlushTimeMs, long pageSizeByte,
                                   long maxStreamSize, boolean autoCleanup) {
        rLock.lock();
        try {
            ensureOpen();
            long producerPtr = createProducer(clientPtr, streamName, delayFlushTimeMs, pageSizeByte,
                maxStreamSize, autoCleanup, DEFAULT_RETAIN_FOR_NUM_CONSUMERS, false, DEFAULT_RESERVE_SIZE);
            return new ProducerImpl(producerPtr);
        } finally {
            rLock.unlock();
        }
    }

    /**
     * Create one Producer to send element.
     *
     * @param streamName            The name of the stream.
     * @param delayFlushTimeMs      The time used in automatic flush after send.
     * @param pageSizeByte          The size used in allocate page, default size 1MB,
     *                              must be a multiple of 4KB.
     * @param maxStreamSize         The max stream size in worker, default size 1GB,
     *                              must greater then 64KB and less than the
     *                              shared memory size.
     * @param autoCleanup         Should auto delete when last consumer/producer exit, default false
     * @param retainForNumConsumers The number of consumers to retain data for, default to 0.
     * @return The output Producer interface that user can use it to send element.
     */
    public Producer createProducer(String streamName, long delayFlushTimeMs, long pageSizeByte,
                                   long maxStreamSize, boolean autoCleanup, long retainForNumConsumers) {
        rLock.lock();
        try {
            ensureOpen();
            long producerPtr = createProducer(clientPtr, streamName, delayFlushTimeMs, pageSizeByte,
                maxStreamSize, autoCleanup, retainForNumConsumers, false, DEFAULT_RESERVE_SIZE);
            return new ProducerImpl(producerPtr);
        } finally {
            rLock.unlock();
        }
    }

    /**
     * Create one Producer to send element.
     *
     * @param streamName            The name of the stream.
     * @param delayFlushTimeMs      The time used in automatic flush after send.
     * @param pageSizeByte          The size used in allocate page, default size 1MB,
     *                              must be a multiple of 4KB.
     * @param maxStreamSize         The max stream size in worker, default size 1GB,
     *                              must greater then 64KB and less than the
     *                              shared memory size.
     * @param autoCleanup         Should auto delete when last consumer/producer exit, default false
     * @param retainForNumConsumers The number of consumers to retain data for, default to 0.
     * @param encryptStream         Enable stream data encryption between workers, default to false.
     * @return The output Producer interface that user can use it to send element.
     */
    public Producer createProducer(String streamName, long delayFlushTimeMs, long pageSizeByte,
                                   long maxStreamSize, boolean autoCleanup, long retainForNumConsumers,
                                   boolean encryptStream) {
        rLock.lock();
        try {
            ensureOpen();
            long producerPtr = createProducer(clientPtr, streamName, delayFlushTimeMs, pageSizeByte,
                maxStreamSize, autoCleanup, retainForNumConsumers, encryptStream, DEFAULT_RESERVE_SIZE);
            return new ProducerImpl(producerPtr);
        } finally {
            rLock.unlock();
        }
    }

    /**
     * Create one Producer to send element.
     *
     * @param streamName            The name of the stream.
     * @param delayFlushTimeMs      The time used in automatic flush after send.
     * @param pageSizeByte          The size used in allocate page, default size 1MB,
     *                              must be a multiple of 4KB.
     * @param maxStreamSize         The max stream size in worker, default size 1GB,
     *                              must greater then 64KB and less than the
     *                              shared memory size.
     * @param autoCleanup         Should auto delete when last consumer/producer exit, default false
     * @param retainForNumConsumers The number of consumers to retain data for, default to 0.
     * @param encryptStream         Enable stream data encryption between workers, default to false.
     * @param reserveSize           default reserve size to page size, must be a multiple of page size.
     * @return The output Producer interface that user can use it to send element.
     */
    public Producer createProducer(String streamName, long delayFlushTimeMs, long pageSizeByte,
                                   long maxStreamSize, boolean autoCleanup, long retainForNumConsumers,
                                   boolean encryptStream, long reserveSize) {
        rLock.lock();
        try {
            ensureOpen();
            long producerPtr = createProducer(clientPtr, streamName, delayFlushTimeMs, pageSizeByte,
                maxStreamSize, autoCleanup, retainForNumConsumers, encryptStream, reserveSize);
            return new ProducerImpl(producerPtr);
        } finally {
            rLock.unlock();
        }
    }

    /**
     * Subscribe a new consumer onto master request
     *
     * @param streamName       The name of the stream.
     * @param subName          The name of subscription.
     * @param subscriptionType The type of SubscriptionType.
     * @return The consumer.
     */
    public Consumer subscribe(String streamName, String subName, SubscriptionType subscriptionType) {
        rLock.lock();
        try {
            ensureOpen();
            long outConsumer = subscribe(clientPtr, streamName, subName, subscriptionType, false);
            return new ConsumerImpl(outConsumer);
        } finally {
            rLock.unlock();
        }
    }

    /**
     * Subscribe a new consumer onto master request
     *
     * @param streamName       The name of the stream.
     * @param subName          The name of subscription.
     * @param subscriptionType The type of SubscriptionType.
     * @param shouldAutoAck    Should AutoAck be enabled for this subscriber or not.
     * @return The consumer.
     */
    public Consumer subscribe(String streamName, String subName, SubscriptionType subscriptionType,
                              boolean shouldAutoAck) {
        rLock.lock();
        try {
            ensureOpen();
            long outConsumer = subscribe(clientPtr, streamName, subName, subscriptionType, shouldAutoAck);
            return new ConsumerImpl(outConsumer);
        } finally {
            rLock.unlock();
        }
    }

    /**
     * Query number of producer in global worker node
     *
     * @param streamName The name of the target stream.
     * @return Query result.
     */
    public long queryGlobalProducerNum(String streamName) {
        rLock.lock();
        try {
            ensureOpen();
            return queryGlobalProducerNum(clientPtr, streamName);
        } finally {
            rLock.unlock();
        }
    }

    /**
     * Query number of consumer in global worker node
     *
     * @param streamName The name of the target stream.
     * @return Query result.
     */
    public long queryGlobalConsumerNum(String streamName) {
        rLock.lock();
        try {
            ensureOpen();
            return queryGlobalConsumerNum(clientPtr, streamName);
        } finally {
            rLock.unlock();
        }
    }

    /**
     * To delete Stream client.
     */
    public void close() {
        wLock.lock();
        try {
            if (clientPtr != 0) {
                freeJNIPtrNative(clientPtr);
            }
            clientPtr = 0;
        } finally {
            wLock.unlock();
        }
    }

    /**
     * Checks to make sure that stream client has not been closed.
     */
    private void ensureOpen() {
        if (clientPtr == 0) {
            throw new DataSystemException(this.getClass().getName() + ": Client closed");
        }
    }

    /**
     * To delete Stream.
     *
     * @param streamName The name of the target stream.
     */
    public void deleteStream(String streamName) {
        rLock.lock();
        try {
            ensureOpen();
            deleteStream(clientPtr, streamName);
        } finally {
            rLock.unlock();
        }
    }

    /**
     * To update token for the stream client.
     *
     * @param tokenBytes The token bytes of the stream client.
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
     * To update aksk for the stream client.
     *
     * @param accessKey The accessKey of the stream client.
     * @param secretKeyBytes The secretKey bytes of the stream client.
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
    public String toString() {
        return super.toString();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        StreamClient otherStreamClient = (StreamClient) other;
        return clientPtr == otherStreamClient.clientPtr;
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientPtr);
    }

    /**
     * Use jni to init streamclient.
     *
     * @param connectOptions The parameters for establishing a connection.
     * @param shouldReportWorkerLost Whether to report to the caller when worker had crashed or worker lost the client.
     * @return StreamClient pointer.
     */
    private static native long init(ConnectOptions connectOptions, boolean shouldReportWorkerLost);

    /**
     * Use jni to create producer.
     *
     * @param clientPtr             StreamClient pointer.
     * @param streamName            The name of the stream.
     * @param delayFlushTimeMs      The time used in automatic flush after send.
     * @param pageSizeByte          The size used in allocate page.
     * @param maxStreamSize         The max stream size in worker.
     * @param autoCleanup         Should auto delete when last producer/consumer exit
     * @param retainForNumConsumers The number of consumers to retain data for, default to 0.
     * @param encryptStream         Enable stream data encryption between workers, default to false.
     * @param reserveSize           default reserve size to page size, must be a multiple of page size.
     * @return Producer pointer.
     */
    private static native long createProducer(long clientPtr, String streamName, long delayFlushTimeMs,
                                              long pageSizeByte, long maxStreamSize, boolean autoCleanup,
                                              long retainForNumConsumers, boolean encryptStream,
                                              long reserveSize);

    /**
     * Use jni to subscribe a new consumer onto master request
     *
     * @param clientPtr        StreamClient pointer.
     * @param streamName       The name of the stream.
     * @param subName          The name of subscription.
     * @param subscriptionType The type of SubscriptionType.
     * @param shouldAutoAck    Should AutoAck be enabled for this subscriber or not.
     * @return The consumer.
     */
    private static native long subscribe(long clientPtr, String streamName, String subName,
                                         SubscriptionType subscriptionType, boolean shouldAutoAck);

    /**
     * Use jni to query number of producer in global worker node
     *
     * @param clientPtr  StreamClient pointer.
     * @param streamName The name of the target stream.
     * @return Query result.
     */
    private static native long queryGlobalProducerNum(long clientPtr, String streamName);

    /**
     * Use jni to query number of consumer in global worker node
     *
     * @param clientPtr  StreamClient pointer.
     * @param streamName The name of the target stream.
     * @return Query result.
     */
    private static native long queryGlobalConsumerNum(long clientPtr, String streamName);

    /**
     * Use jni to delete the stream
     *
     * @param clientPtr  StreamClient pointer.
     * @param streamName The name of the target stream.
     */
    private static native void deleteStream(long clientPtr, String streamName);

    /**
     * This method is used to delete the client pointer that is released at the JNI layer.
     *
     * @param clientPtr The StreamClient pointer.
     */
    private static native void freeJNIPtrNative(long clientPtr);

    /**
     * Use JNI to invoke the UpdateToken interface of C++.
     *
     * @param clientPtr The StreamClient pointer.
     * @param tokenBytes The token bytes for StreamClient.
     */
    private static native void updateTokenNative(long clientPtr, byte[] tokenBytes);

    /**
     * Use JNI to invoke the UpdateAkSk interface of C++.
     *
     * @param clientPtr The StreamClient pointer.
     * @param accessKey The accessKey for StreamClient.
     * @param secretKeyBytes The secretKey bytes for StreamClient.
     */
    private static native void updateAkSkNative(long clientPtr, String accessKey, byte[] secretKeyBytes);
}