// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.data.cache;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigException;
import com.yahoo.bard.webservice.config.SystemConfigProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;

import java.io.IOException;
import java.io.Serializable;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.validation.constraints.NotNull;

/**
 * MemCached client implementation of DataCache.  Internally uses hashed key to keep under 250 character limit.
 * @param <T> Type of data
 */
@Singleton
public class MemDataCache<T extends Serializable> implements DataCache<T> {
    private static final Logger LOG = LoggerFactory.getLogger(MemDataCache.class);
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    private static final @NotNull String SERVER_CONFIG_KEY = SYSTEM_CONFIG.getPackageVariableName("memcached_servers");
    private static final String SERVER_CONFIG = SYSTEM_CONFIG.getStringProperty(SERVER_CONFIG_KEY);

    /**
     * Memcached uses the actual value sent, and it may either be Unix time (number of seconds since January 1, 1970, as
     * a 32-bit value), or a number of seconds starting from current time. In the latter case, this number of seconds
     * may not exceed 60*60*24*30 (number of seconds in 30 days); if the number sent by a client is larger than that,
     * the server will consider it to be real Unix time value rather than an offset from current time.
     */
    private static final int EXPIRATION_MAX_VALUE = 60 * 60 * 24 * 30;
    private static final @NotNull String EXPIRATION_KEY =
            SYSTEM_CONFIG.getPackageVariableName("memcached_expiration_seconds");
    private static final int EXPIRATION_DEFAULT_VALUE = 3600;
    private static final int EXPIRATION = SYSTEM_CONFIG.getIntProperty(EXPIRATION_KEY, EXPIRATION_DEFAULT_VALUE);

    final private MemcachedClient client;

    /**
     * Constructor using a default Memcached Client.
     *
     * @throws IOException if the memcached client fails to start
     */
    @Inject
    public MemDataCache() throws IOException {
        this(new MemcachedClient(new BinaryConnectionFactory(), AddrUtil.getAddresses(SERVER_CONFIG)));
    }

    /**
     * Constructor.
     *
     * @param client  The Memcached client to support this cache
     */
    public MemDataCache(MemcachedClient client) {
        // validate expiration value
        if (EXPIRATION > EXPIRATION_MAX_VALUE) {
            throw new SystemConfigException("memcached_expiration_seconds exceeds " + EXPIRATION_MAX_VALUE);
        }
        this.client = client;
    }

    @Override
    public T get(String key) {
        try {
            @SuppressWarnings("unchecked")
            T value = (T) client.get(key);
            return value;
        } catch (RuntimeException warnThenIgnore) {
            LOG.warn(warnThenIgnore.getMessage(), warnThenIgnore);
            return null;
        }
    }

    @Override
    public boolean set(String key, T value) throws IllegalStateException {
        try {
            // Omitting null checking for key since it should be rare.
            // An exception will be thrown by the memcached client.
            return client.set(key, EXPIRATION, value).get();
        } catch (Exception e) {
            LOG.warn("set failed {} {}", key, e.toString());
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void clear() {
        client.flush();
    }
}
