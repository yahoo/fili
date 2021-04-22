// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.cache;

import static net.spy.memcached.DefaultConnectionFactory.DEFAULT_OPERATION_TIMEOUT;

import com.yahoo.bard.webservice.application.MetricRegistryFactory;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigException;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.logging.blocks.BardCacheInfo;
import com.yahoo.bard.webservice.logging.blocks.BardQueryInfo;
import com.yahoo.bard.webservice.web.responseprocessors.CacheV2ResponseProcessor;
import com.yahoo.bard.webservice.web.util.QuerySignedCacheService;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
    private static final MetricRegistry REGISTRY = MetricRegistryFactory.getRegistry();
    public static final Meter CACHE_SET_TIMEOUT_FAILURES = REGISTRY.meter("queries.meter.cache.put.timeout.failures");
    public static final String LOG_CACHE_SET_TIMEOUT = "cacheSetTimedOut";

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
    private static final @NotNull String WAIT_FOR_FUTURE =
            SYSTEM_CONFIG.getPackageVariableName("wait_for_future_from_memcached");

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
            LOG.warn("get failed for key {} with cksum {} , {}",
                    key,
                    CacheV2ResponseProcessor.getMD5Cksum(key),
                    warnThenIgnore.getMessage(),
                    warnThenIgnore);
            BardQueryInfo.getBardQueryInfo().addCacheInfo(
                    CacheV2ResponseProcessor.getMD5Cksum(key),
                    new BardCacheInfo(
                            QuerySignedCacheService.LOG_CACHE_READ_FAILURES,
                            key.length(),
                            CacheV2ResponseProcessor.getMD5Cksum(key),
                            null,
                            0
                    )
            );
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
        return setInSeconds(key, value, (int) TimeUnit.MILLISECONDS.toSeconds(expiration.getMillis()));
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
            if (SYSTEM_CONFIG.getBooleanProperty(WAIT_FOR_FUTURE, false)) {
                return client.set(key, expirationInSeconds, value).get();
            } else {
                return client.set(key, expirationInSeconds, value) != null;
            }
        } catch (Exception e) {
            Throwable exception = e.getClass() == RuntimeException.class ? e.getCause() : e;
            if (exception instanceof TimeoutException) {
                //mark and log the timeout errors on cache set
                CACHE_SET_TIMEOUT_FAILURES.mark(1);
                BardQueryInfo.getBardQueryInfo().incrementCountCacheSetTimeoutFailures();
                BardQueryInfo.getBardQueryInfo().addCacheInfo(
                        CacheV2ResponseProcessor.getMD5Cksum(key),
                        new BardCacheInfo(
                                LOG_CACHE_SET_TIMEOUT,
                                key.length(),
                                CacheV2ResponseProcessor.getMD5Cksum(key),
                                null,
                                value.toString().length()
                        )
                );
            }
            LOG.warn("set failed for key: {} ,cksum: {} {}",
                    key, CacheV2ResponseProcessor.getMD5Cksum(key), e.toString());
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void clear() {
        client.flush();
    }
}
