// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
//
package com.yahoo.bard.webservice.logging.blocks;

import com.yahoo.bard.webservice.logging.LogInfo;


import com.fasterxml.jackson.annotation.JsonAutoDetect;

/**
 * Log that keep tracks for cache set failures.
 */
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.ANY)
public class BardCacheInfo implements LogInfo {
    String cacheOperation;
    String cacheKeyCksum;
    int cacheKeyLength;
    int cacheValLength;

    /**
     * Constructor.
     *
     * @param cacheOperation Type of activity like cache hit , miss or potential hit
     * @param cacheKeyLength The Length of the cache key
     * @param cacheKeyCksum  The cache Key MD5 checksum value
     * @param cacheValLength  The cache value length
     */
    public BardCacheInfo(String cacheOperation, int cacheKeyLength, String cacheKeyCksum, int cacheValLength) {
        this.cacheOperation = cacheOperation;
        this.cacheKeyLength = cacheKeyLength;
        this.cacheKeyCksum = cacheKeyCksum;
        this.cacheValLength = cacheValLength;
    }
}
