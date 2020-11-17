// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
//
package com.yahoo.bard.webservice.logging.blocks;

import com.yahoo.bard.webservice.logging.LogInfo;


import com.fasterxml.jackson.annotation.JsonAutoDetect;

/**
 * Log that keep tracks for cache set failures.
 */
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.PUBLIC_ONLY)
public class BardCacheInfo implements LogInfo {
    String cacheKey;
    String cacheKeyCksum;
    int cacheValLength;

    /**
     * Constructor.
     *
     * @param cacheKey  The cache Key
     * @param cacheKeyCksum  The cache Key MD5 checksum
     * @param cacheValLength  The cache value length
     */
    public BardCacheInfo(String cacheKey, String cacheKeyCksum, int cacheValLength) {
        this.cacheKey = cacheKey;
        this.cacheKeyCksum = cacheKeyCksum;
        this.cacheValLength = cacheValLength;
    }
}
