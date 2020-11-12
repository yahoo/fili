// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.cache;

import com.yahoo.bard.webservice.config.SystemConfigException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.AbstractMap;
import java.util.Objects;
import java.util.Base64;

/**
 * DataCache using hashed keys to reduce the key length for the provided underlying cache.
 *
 * @param <T> the type of values maintained by this map
 */
public class HashDataCache<T extends Serializable> implements DataCache<T> {
    private static final Logger LOG = LoggerFactory.getLogger(HashDataCache.class);

    final private DataCache<Pair<String, T>> cache;
    final private String algorithm;

    /**
     * Creates cache hashing with given algorithm.
     *
     * @param cache  provided underlying cache
     * @param md  the MessageDigest to extract hash algorithm
     *
     * @see MessageDigest#getInstance(String)
     */
    public HashDataCache(DataCache<Pair<String, T>> cache, MessageDigest md) {
        this(cache, md.getAlgorithm());
    }

    /**
     * Creates cache hashing with default SHA-512 algorithm.
     *
     * @param cache  provided underlying cache
     */
    public HashDataCache(DataCache<Pair<String, T>> cache) {
        this(cache, "SHA-512");
    }

    /**
     * Creates cache hashing with provided algorithm.
     *
     * @param cache  provided underlying cache
     * @param algorithm  provided MessageDigest hash algorithm
     */
    private HashDataCache(DataCache<Pair<String, T>> cache, String algorithm) {
        this.cache = cache;
        this.algorithm = algorithm;
        try {
            // verify algorithm availability at construction
            MessageDigest.getInstance(algorithm);
        } catch (NoSuchAlgorithmException e) {
            String msg = "Unable to get hash generator with algorithm: " + algorithm;
            LOG.warn(msg, e);
            throw new SystemConfigException(e);
        }
    }

    /**
     * Get the hash encoding of a key string.
     *
     * @param key  The input string to encode.
     *
     * @return The hash encoding of the key as string. Returns null if the method fails to produce a hash.
     */
    public String hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance(algorithm);
            byte[] keyBytes = key.getBytes("UTF-8");
            md.update(keyBytes, 0, keyBytes.length);
            byte[] binaryhash = md.digest();
            return Base64.getEncoder().encodeToString(binaryhash);
        } catch (Exception e) {
            LOG.warn("Failed to get hash encoding for key: {}", key, e);
            return key;
        }
    }

    /**
     * Lookup pair key/value by hashkey but verify found key matches original key.
     *
     * @param key  The key whose associated value is to be returned
     *
     * @return the value to which the specified key is mapped, or null if this map contains no mapping for the key or
     * there is a collision
     */
    @Override
    final public T get(String key) {
        String hashkey = hash(key);
        Pair<String, T> pair = cache.get(hashkey);
        if (pair == null) {
            return null;
        } else if (Objects.equals(key, pair.getKey())) {
            return pair.getValue();
        } else {
            LOG.warn("collision {}\n{}\n{}", hashkey, key, pair.getKey());
            return null;
        }
    }

    @Override
    //(Deprecate this return type to be void)
    final public boolean set(String key, T value) {
        return cache.set(hash(key), new Pair<>(key, value));
    }

    @Override
    public void clear() {
        cache.clear();
    }

    /**
     * Key/Value pair.
     *
     * @param <K> key type
     * @param <V> value type
     */
    public static class Pair<K, V> extends AbstractMap.SimpleImmutableEntry<K, V> {
        /**
         * Constructor.
         *
         * @param key  Key of the pair
         * @param value  Value of the pair
         */
        public Pair(K key, V value) {
            super(key, value);
        }
    }
}
