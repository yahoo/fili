// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
//
package com.yahoo.bard.webservice.logging.blocks;

import com.yahoo.bard.webservice.logging.LogInfo;


import com.fasterxml.jackson.annotation.JsonAutoDetect;

/**
 * Log that keep tracks for cache set failures.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class BardCacheInfo implements LogInfo {
    protected final String opType;
    protected final String cacheKeyCksum;
    protected final int cacheKeyLen;
    protected final int cacheValLen;

    /**
     * Constructor.
     *
     * @param opType Type of activity like cache hit , miss or potential hit
     * @param cacheKeyLen The Length of the cache key
     * @param cacheKeyCksum  The cache Key MD5 checksum value
     * @param cacheValLen  The cache value length
     */
    public BardCacheInfo(String opType, int cacheKeyLen, String cacheKeyCksum, int cacheValLen) {
        this.opType = opType;
        this.cacheKeyLen = cacheKeyLen;
        this.cacheKeyCksum = cacheKeyCksum;
        this.cacheValLen = cacheValLen;
    }
}
