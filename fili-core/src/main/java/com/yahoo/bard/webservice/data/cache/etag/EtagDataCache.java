// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.cache.etag;

import com.yahoo.bard.webservice.data.cache.MemDataCache;
import com.yahoo.bard.webservice.data.cache.TupleDataCache;

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
 * Etag cache.
 *
 * @param <V>  The raw data type of the cached etag value
 */
@Singleton
public class EtagDataCache<V extends Serializable>
        extends MemDataCache<TupleDataCache.DataEntry<String, String, V>>
        implements TupleDataCache<String, String, V> {

    private static final Logger LOG = LoggerFactory.getLogger(EtagDataCache.class);
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
    public EtagDataCache() throws IOException {
        super();
    }

    /**
     * Constructor.
     *
     * @param client  The memcached client to support this cache.
     */
    public EtagDataCache(MemcachedClient client) {
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
        TupleDataCache.DataEntry<String, String, V> result = get(key);

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
    public TupleDataCache.DataEntry<String, String, V> get(String key) {
        return super.get(hash(key));
    }

    @Override
    public boolean set(String key, String meta, V value) {
        return set(hash(key), new DataEntry<>(key, meta, value));
    }

    /**
     * Etag cache entry.
     *
     * @param <V>  The raw data type of the cached etag value
     */
    public static class DataEntry<V extends Serializable>
            implements TupleDataCache.DataEntry<String, String, V>, Serializable {
        private static final long serialVersionUID = 1630228312720546277L;
        private final String key;
        private final String etagMeta;
        private final V etagValue;

        /**
         * Constructor.
         *
         * @param key  The key of this data cache entry.
         * @param etagMeta  The etag metadata associated with this data cache entry.
         * @param etagValue  The etag value associated with this data cache entry.
         */
        public DataEntry(String key, String etagMeta, V etagValue) {
            this.key = key;
            this.etagMeta = etagMeta;
            this.etagValue = etagValue;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public String getMeta() {
            return etagMeta;
        }

        @Override
        public V getValue() {
            return etagValue;
        }
    }
}
