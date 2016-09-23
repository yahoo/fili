// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * Redis Store Manager
 * <p>
 * Though not needed, the singleton pattern is used to namespace keys. Each instance has a name; Given a name, only one
 * instance with that name exists.
 */
public class RedisStoreManager {
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    // prefix for all of the keys
    private static final String REDIS_NAMESPACE = SYSTEM_CONFIG.getStringProperty(
            SYSTEM_CONFIG.getPackageVariableName("redis_namespace"),
            "bard"
    );

    // Redis server details
    private static final String REDIS_HOST = SYSTEM_CONFIG.getStringProperty(
            SYSTEM_CONFIG.getPackageVariableName("redis_host"),
            "localhost"
    );
    private static final int REDIS_PORT = SYSTEM_CONFIG.getIntProperty(
            SYSTEM_CONFIG.getPackageVariableName("redis_port"),
            6379
    );
    // Timeout is in milliseconds
    private static final int REDIS_TIMEOUT_MS = SYSTEM_CONFIG.getIntProperty(
            SYSTEM_CONFIG.getPackageVariableName("redis_timeout_ms"),
            5000
    );

    // A single connection pool is shared between all instances
    protected static final JedisPool POOL = new JedisPool(
            new JedisPoolConfig(), REDIS_HOST, REDIS_PORT, REDIS_TIMEOUT_MS
    );

    // Hold singleton instances by name
    private static final Map<String, RedisStore> REDIS_STORES = new HashMap<>();

    /**
     * Factory for singleton instances by name.
     * <p>
     * Only a single instance can exist for each name.
     *
     * @param storeName Name for the singleton instance
     *
     * @return The singleton instance for the given name
     */
    public static synchronized RedisStore getInstance(String storeName) {
        RedisStore redisStore = REDIS_STORES.get(storeName);

        if (redisStore == null) {
            redisStore = new RedisStore(storeName, POOL, REDIS_NAMESPACE);
            REDIS_STORES.put(storeName, redisStore);
        }
        return redisStore;
    }

    /**
     * Delete the named singleton instance.
     * <p>
     * Also deletes all keys created by the instance.
     *
     * @param storeName Name of the singleton instance to delete
     */
    public static synchronized void removeInstance(String storeName) {
        if (storeName != null) {
            RedisStore redisStore = REDIS_STORES.get(storeName);
            if (redisStore != null) {
                redisStore.removeAllKeys();
                REDIS_STORES.remove(storeName);
            }
        }
    }
}
