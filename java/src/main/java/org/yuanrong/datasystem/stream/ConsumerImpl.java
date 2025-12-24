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

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The consumer implement, visible in this package.
 *
 * @since 2022-05-07
 */
class ConsumerImpl implements Consumer {
    private static StatisticsMessage statisticsMessage = new StatisticsMessage();

    // for consumerPtr.
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock rLock = rwLock.readLock();
    private final Lock wLock = rwLock.writeLock();

    // Point to jni consumer object.
    private long consumerPtr;

    public ConsumerImpl(long consumerPtr) {
        this.consumerPtr = consumerPtr;
    }

    @Override
    public List<Element> receive(long expectNum, int timeoutMs) {
        rLock.lock();
        try {
            ensureOpen();
            return receive(consumerPtr, expectNum, timeoutMs, true);
        } finally {
            rLock.unlock();
        }
    }

    @Override
    public List<Element> receive(int timeoutMs) {
        rLock.lock();
        try {
            ensureOpen();
            return receive(consumerPtr, 0L, timeoutMs, false);
        } finally {
            rLock.unlock();
        }
    }

    @Override
    public void ack(long elementId) {
        rLock.lock();
        try {
            ensureOpen();
            ack(consumerPtr, elementId);
        } finally {
            rLock.unlock();
        }
    }

    /**
     * Checks to make sure that producer has not been closed.
     */
    private void ensureOpen() {
        if (consumerPtr == 0) {
            throw new DataSystemException(this.getClass().getName() + ": Consumer closed");
        }
    }

    @Override
    public void close() {
        wLock.lock();
        try {
            if (consumerPtr == 0) {
                throw new DataSystemException("Consumer has been closed");
            }
            close(consumerPtr);
            consumerPtr = 0;
        } finally {
            wLock.unlock();
        }
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    protected void finalize() {
        if (consumerPtr != 0) {
            freeJNIPtrNative(consumerPtr);
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
        ConsumerImpl otherConsumerImpl = (ConsumerImpl) other;
        return consumerPtr == otherConsumerImpl.consumerPtr;
    }

    @Override
    public void getStatisticsMessage(StatisticsMessage statistics) {
        try {
            getStatisticsMessageImpl(this.consumerPtr, statistics);
        } catch (NullPointerException e) {
            e.printStackTrace();
        }
    }

    private static synchronized void getStatisticsMessageImpl(
        long consumerPtr, StatisticsMessage statistics) throws NullPointerException {
        if (statistics == null) {
            throw new NullPointerException("StatisticsMessage is null.");
        }
        getStatisticsMessage(consumerPtr);
        statistics.setTotalElements(statisticsMessage.getTotalElements());
        statistics.setNotProcessedElements(statisticsMessage.getNotProcessedElements());
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumerPtr);
    }

    private static BigInteger longToBigInteger(long uint64) {
        if (uint64 >= 0) {
            return BigInteger.valueOf(uint64);
        } else {
            ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
            buf.putLong(uint64);
            return new BigInteger(1, buf.array());
        }
    }

    /**
     * Called by jni from c++ to store statistics message. Should not be called through consumer interface.
     *
     * @param totalElements The number of total elements of c++ uint64_t originally.
     * @param notProcessedElements The number of not processed elements of c++ uint64_t originally.
     * @throws Exception NullPointerException, IllegalArgumentException, BufferOverflowException,
     * ReadOnlyBufferException, NumberFormatException
     */
    public static void storeStatisticsMessage(long totalElements, long notProcessedElements) throws Exception {
        // totalElements and notProcessedElements are originally of type uint64_t in c++.
        BigInteger total = longToBigInteger(totalElements);
        BigInteger notProcessed = longToBigInteger(notProcessedElements);
        if (statisticsMessage != null) {
            statisticsMessage.setTotalElements(total);
            statisticsMessage.setNotProcessedElements(notProcessed);
        } else {
            throw new NullPointerException("No buffer available to store statistics message.");
        }
    }

    private static native List<Element> receive(long consumerPtr, long expectNum, int timeoutMs,
                                                boolean hasExpectedNum);

    private static native void ack(long consumerPtr, long elementId);

    private static native void close(long consumerPtr);

    private static native void freeJNIPtrNative(long clientPtr);

    private static native void getStatisticsMessage(long consumerPtr);
}