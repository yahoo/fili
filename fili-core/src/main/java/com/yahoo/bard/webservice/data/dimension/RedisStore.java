// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

/**
 * A Redis-based implementation of KeyValueStore.
 */
public class RedisStore implements KeyValueStore {
    private static final Logger LOG = LoggerFactory.getLogger(RedisStore.class);

    private boolean redisIsHealthy;
    private final JedisPool pool;
    private final String storeName;
    private final String redisNamespace;

    /**
     * Build a connection to a Redis provider.
     *
     * @param storeName  The second part of the prefix to keyspace names for this store
     * @param pool  A pool of Jedis connection instances
     * @param redisNamespace  The first part of the prefix to the keyspace names for this store
     */
    public RedisStore(String storeName, JedisPool pool, String redisNamespace) {
        this.pool = pool;
        this.storeName = storeName;
        this.redisIsHealthy = true;
        this.redisNamespace = redisNamespace;
        open();
    }

    @Override
    public void open() {
        ping();
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public boolean isOpen() {
        return !pool.isClosed();
    }

    /**
     * Remove all keys associated with the store.
     * <p>
     * This is mainly used for tests; the Redis command used to get the associated keys (KEYS) is rather expensive and
     * shouldn't be run frequently.
     */
    public void removeAllKeys() {
        removeAllKeys(storeName);
    }

    @Override
    public String remove(@NotNull String key) {
        if (key == null) {
            throw new IllegalArgumentException("Cannot remove null key");
        }

        String rKey = redisKey(storeName, key);

        try (Jedis jedis = pool.getResource()) {
            String previousValue = jedis.get(rKey);
            jedis.del(rKey);
            return previousValue;
        } catch (JedisException e) {
            redisIsHealthy = false;
            String msg = String.format("Unable to remove key %s (Redis key %s)", key, rKey);
            LOG.error(msg);
            throw new RuntimeException(msg, e);
        }
    }

    @Override
    public String get(@NotNull String key) {
        if (key == null) {
            throw new IllegalArgumentException("Cannot get null key");
        }

        String rKey = redisKey(storeName, key);

        try (Jedis jedis = pool.getResource()) {
            return jedis.get(rKey);
        } catch (JedisException e) {
            redisIsHealthy = false;
            String msg = String.format("Unable to get key %s (Redis key %s)", key, rKey);
            LOG.error(msg);
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isHealthy() {
        // If we know we're not healthy, don't bother pinging.
        if (!redisIsHealthy) {
            return false;
        }
        ping();
        return redisIsHealthy;
    }

    @Override
    public String put(@NotNull String key, String value) {
        // Put our single key as a map, and get the value of that one key out of the returned map
        return putAll(Collections.singletonMap(key, value)).get(key);
    }

    @Override
    public Map<String, String> putAll(@NotNull Map<String, String> entries) {
        if (entries == null) {
            throw new IllegalArgumentException("Cannot set null entries");
        }

        try (Jedis jedis = pool.getResource()) {

            Map<String, String> oldValues = new HashMap<>(entries.size());
            for (String key : entries.keySet()) {
                if (key == null) {
                    throw new IllegalArgumentException("Cannot set null key");
                }

                String rKey = redisKey(storeName, key);

                String oldValue = jedis.get(rKey);
                String newValue = entries.get(key);

                oldValues.put(key, oldValue);

                if (newValue == null) {
                    if (oldValue != null) {
                        jedis.del(rKey);
                    }
                } else {
                    String result = jedis.set(rKey, entries.get(key));
                    if (!"OK".equals(result)) {
                        redisIsHealthy = false;
                        String msg = "Redis failed to store key";
                        LOG.error(msg);
                        throw new RuntimeException(msg);
                    }
                }
            }
            return oldValues;

        } catch (JedisException e) {
            redisIsHealthy = false;
            String msg = "Unable to put keys into Redis";
            LOG.error(msg);
            throw new RuntimeException(msg, e);
        }
    }

    /**
     * Ping the Redis server and update redisIsHealthy.
     */
    private void ping() {
        try (Jedis jedis = pool.getResource()) {
            String pong = jedis.ping();
            redisIsHealthy = "PONG".equals(pong);
        } catch (JedisException ignored) {
            redisIsHealthy = false;
        }
    }

    /**
     * Remove all keys in the store.
     *
     * @param storeName  Name of the store from which to remove keys
     */
    private synchronized void removeAllKeys(String storeName) {
        if (storeName != null) {
            // According to http://redis.io/commands/keys, the special
            // characters allowed in KEYS are: '?', '*', '[', ']'. They are
            // escaped with '\'.  Escape '\' first to prevent
            // double-escaping; then escape the storeName.
            String escapedStoreName = storeName;
            escapedStoreName = escapedStoreName.replace("\\", "\\\\"); // escape '\'
            escapedStoreName = escapedStoreName.replace("*", "\\*");   // escape '*'
            escapedStoreName = escapedStoreName.replace("[", "\\[");   // escape '['
            escapedStoreName = escapedStoreName.replace("]", "\\]");   // escape ']'
            escapedStoreName = escapedStoreName.replace("?", "\\?");   // escape '?'
            String pattern = redisKey(escapedStoreName, "*");   // e.g., bard-escaped_dimension-*

            try (Jedis jedis = pool.getResource()) {
                Set<String> keys = jedis.keys(pattern);

                // Jedis crashes when passed an empty array, so only run del if
                // there are actually things to delete.
                if (!keys.isEmpty()) {
                    jedis.del(keys.toArray(new String[keys.size()]));
                }
            } catch (JedisException e) {
                redisIsHealthy = false;
                String msg = "Unable to remove all keys";
                LOG.error(msg);
                throw new RuntimeException(msg, e);
            }
        }
    }

    /**
     * Build the namespace- and store-specific redis key for the given key.
     *
     * @param storeName  Name of the store (used as part of the prefix)
     * @param key  Name of the key to generate the name for
     *
     * @return the prefixed key
     */
    private String redisKey(@NotNull String storeName, @NotNull String key) {
        return redisNamespace + "-" + storeName + "-" + key;
    }
}
