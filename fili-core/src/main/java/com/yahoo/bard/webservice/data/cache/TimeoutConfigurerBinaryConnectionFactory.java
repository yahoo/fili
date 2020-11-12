// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.cache;

import net.spy.memcached.BinaryConnectionFactory;

/**
 * A binary connection factory with a configurable timeout.
 */
public class TimeoutConfigurerBinaryConnectionFactory extends BinaryConnectionFactory {

    long operationTimeout;

    /**
     * Create a TimeoutConfigurerBinaryConnectionFactory with the default parameters.
     *
     */
    public TimeoutConfigurerBinaryConnectionFactory() {
        this(DEFAULT_OPERATION_TIMEOUT);
    }

    /**
     * Create a TimeoutConfigurerBinaryConnectionFactory with the default parameters.
     *
     * @param operationTimeout  The number of milliseconds to wait for timeout,
     */
    public TimeoutConfigurerBinaryConnectionFactory(Long operationTimeout) {
        super();
        this.operationTimeout = operationTimeout;
    }

    /**
     * Create a BinaryConnectionFactory with the given maximum operation queue length, and the given read buffer size
     * and timeout in milliseconds.
     *
     * @param len  the length of the queue
     * @param bufSize the size of the queue
     * @param operationTimeout  The number of milliseconds to wait for timeout,
     */
    TimeoutConfigurerBinaryConnectionFactory(int len, int bufSize, Long operationTimeout) {
        super(len, bufSize);
        this.operationTimeout = operationTimeout;
    }

    @Override
    public long getOperationTimeout() {
        return  operationTimeout;
    }
}
