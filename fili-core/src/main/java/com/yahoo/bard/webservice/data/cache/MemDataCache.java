// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.cache;

import static net.spy.memcached.DefaultConnectionFactory.DEFAULT_OPERATION_TIMEOUT;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigException;
import com.yahoo.bard.webservice.config.SystemConfigProvider;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.spy.memcached.AddrUtil;
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

    private static final @NotNull String OPERATION_TIMEOUT_CONFIG_KEY = SYSTEM_CONFIG.getPackageVariableName(
            "memcached_operation_timeout"
    );

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

    final private MemcachedClient client;

    /**
     * Constructor using a default Memcached Client.
     *
     * @throws IOException if the memcached client fails to start
     */
    @Inject
    public MemDataCache() throws IOException {
        this(
                new MemcachedClient(new TimeoutConfigurerBinaryConnectionFactory(
                        SYSTEM_CONFIG.getLongProperty(OPERATION_TIMEOUT_CONFIG_KEY, DEFAULT_OPERATION_TIMEOUT)),
                        AddrUtil.getAddresses(SERVER_CONFIG)
                )
        );
    }

    /**
     * Constructor.
     *
     * @param client  The Memcached client to support this cache
     */
    public MemDataCache(MemcachedClient client) {
        // validate expiration value
        int expiration = SYSTEM_CONFIG.getIntProperty(EXPIRATION_KEY, EXPIRATION_DEFAULT_VALUE);
        if (expiration > EXPIRATION_MAX_VALUE) {
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

    //(Deprecate this return type to be void)
    @Override
    public boolean set(String key, T value) throws IllegalStateException {
        int expiration = SYSTEM_CONFIG.getIntProperty(EXPIRATION_KEY, EXPIRATION_DEFAULT_VALUE);
        return setInSeconds(key, value, expiration);
    }

    //(Deprecate this return type to be void)
    @Override
    public boolean set(String key, T value, DateTime expiration) throws IllegalStateException {
        return setInSeconds(key, value, (int) (expiration.getMillis() / 1000L));
    }

    /**
     * Assigns the specified value to the specified key.
     *
     * @param key  the key under which this object should be added.
     * @param value  the object to store
     * @param expirationInSeconds The expiration time in seconds. Memcached
     * uses the actual value sent, and it may either be Unix time (number of
     * seconds since January 1, 1970, as a 32-bit value), or a number of
     * seconds starting from current time. In the latter case, this number of
     * seconds may not exceed 60*60*24*30 (number of seconds in 30 days); if
     * the number sent by a client is larger than that, the server will
     * consider it to be real Unix time value rather than an offset from
     * current time.
     *
     * (Deprecate this return type to be void)
     * @return a boolean representing success of this operation
     * @throws IllegalStateException if we fail to add the key-value to the cache because of an error
     */
    private boolean setInSeconds(String key, T value, int expirationInSeconds) throws IllegalStateException {
        try {
            // Omitting null checking for key since it should be rare.
            // An exception will be thrown by the memcached client.
            return client.set(key, expirationInSeconds, value) != null;
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
