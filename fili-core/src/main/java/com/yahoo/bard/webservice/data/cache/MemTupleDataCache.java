// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.spy.memcached.MemcachedClient;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import javax.inject.Singleton;

/**
 * Memcached client implementation of TupleDataCache.
 *
 * @param <T>  The meta data type of the cache.
 * @param <V>  The raw data type of the cache.
 */
@Singleton
public class MemTupleDataCache<T extends Serializable, V extends Serializable>
        extends MemDataCache<TupleDataCache.DataEntry<String, T, V>>
        implements TupleDataCache<String, T, V> {

    private static final Logger LOG = LoggerFactory.getLogger(MemTupleDataCache.class);
    private static final String DEFAULT_HASH_ALGORITHM = "SHA-512";

    // To reuse a MessageDigest and avoiding locking, every thread puts its hash generator in thread local storage
    private static final ThreadLocal<MessageDigest> HASH_GENERATOR = ThreadLocal.withInitial(
            () -> {
                try {
                    return MessageDigest.getInstance(DEFAULT_HASH_ALGORITHM);
                } catch (NoSuchAlgorithmException nsae) {
                    String msg = "Unable to initialize hash generator with default algorithm: " +
                            DEFAULT_HASH_ALGORITHM;

                    LOG.warn(msg, nsae);
                    throw new IllegalStateException(msg, nsae);
                }
            }
    );

    private static final Base64.Encoder ENCODER = Base64.getEncoder();

    /**
     * Constructor using a default memcached client.
     *
     * @throws IOException if the memcached client fails to start.
     */
    public MemTupleDataCache() throws IOException {
        super();
    }

    /**
     * Constructor.
     *
     * @param client  The memcached client to support this cache.
     */
    public MemTupleDataCache(MemcachedClient client) {
        super(client);
    }

    /**
     * Get the hash encoding of a key string.
     *
     * @param key  The input string to encode.
     *
     * @return The hash encoding of the key as string. Returns null if the method fails to produce a hash.
     */
    protected String hash(String key) {
        try {
            MessageDigest gen = HASH_GENERATOR.get();
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            gen.update(keyBytes, 0, keyBytes.length);
            byte[] binaryhash = gen.digest();

            return ENCODER.encodeToString(binaryhash);
        } catch (Exception e) {
            LOG.warn("Failed to get hash encoding for key: {}", key, e);
        }

        return null;
    }

    @Override
    public V getDataValue(String key) {
        TupleDataCache.DataEntry<String, T, V> result = get(key);

        if (result == null) {
            return null;
        } else if (result.getKey().equals(key)) {
            return result.getValue();
        }

        LOG.warn(
                "Cache entry collision detected for hash code: {} with existing key: {} and requested key {}",
                hash(key),
                result.getKey(),
                key
        );

        return null;
    }

    @Override
    public TupleDataCache.DataEntry<String, T, V> get(String key) {
        return super.get(hash(key));
    }

    //(Deprecate this return type to be void)
    @Override
    public boolean set(String key, T meta, V value) {
        return set(hash(key), new DataEntry<>(key, meta, value));
    }

    /**
     * Memcached implementation of the data cache entry of the tuple data cache.
     *
     * @param <T>  The meta data type of the cache.
     * @param <V>  The raw data type of the cache.
     */
    public static class DataEntry<T extends Serializable, V extends Serializable>
            implements TupleDataCache.DataEntry<String, T, V>, Serializable {
        private static final long serialVersionUID = 1630228312720546277L;
        private final String key;
        private final T meta;
        private final V value;

        /**
         * Constructor.
         *
         * @param key  The key of this data cache entry.
         * @param meta  The metadata associated with this data cache entry.
         * @param value  The raw data associated with this data cache entry.
         */
        public DataEntry(String key, T meta, V value) {
            this.key = key;
            this.meta = meta;
            this.value = value;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public T getMeta() {
            return meta;
        }

        @Override
        public V getValue() {
            return value;
        }
    }
}
