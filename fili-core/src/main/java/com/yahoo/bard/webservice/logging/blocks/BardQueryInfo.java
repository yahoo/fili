// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging.blocks;

import com.yahoo.bard.webservice.logging.LogInfo;
import com.yahoo.bard.webservice.logging.RequestLog;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Main log of a request served by the TablesServlet.
 */
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.PUBLIC_ONLY)
public class BardQueryInfo implements LogInfo {
    public static final String WEIGHT_CHECK = "weightCheckQueries";
    public static final String FACT_QUERIES = "factQueryCount";
    public static final String FACT_QUERY_CACHE_HIT = "factCacheHits";

    private final String type;
    private final AtomicInteger weightCheckCount = new AtomicInteger();
    private final AtomicInteger factQueryCount = new AtomicInteger();
    private final AtomicInteger factCacheHitCount = new AtomicInteger();

    /**
     * Constructor.
     *
     * @param queryType  Type of Bard query
     */
    public BardQueryInfo(String queryType) {
        this.type = queryType;
    }

    public String getType() {
        return type;
    }

    public Map<String, AtomicInteger> getQueryCounter() {
        return Stream.of(
                new AbstractMap.SimpleImmutableEntry<>(WEIGHT_CHECK, weightCheckCount),
                new AbstractMap.SimpleImmutableEntry<>(FACT_QUERIES, factQueryCount),
                new AbstractMap.SimpleImmutableEntry<>(FACT_QUERY_CACHE_HIT, factCacheHitCount)
        ).collect(Collectors.toMap(
                AbstractMap.SimpleImmutableEntry::getKey,
                AbstractMap.SimpleImmutableEntry::getValue
        ));
    }

    /**
     * Retrieves {@link com.yahoo.bard.webservice.logging.blocks.BardQueryInfo} from
     * {@link com.yahoo.bard.webservice.logging.RequestLog}.
     *
     * @return {@link com.yahoo.bard.webservice.logging.blocks.BardQueryInfo} from
     * {@link com.yahoo.bard.webservice.logging.RequestLog}
     */
    public static BardQueryInfo getBardQueryInfo() {
        return ((BardQueryInfo) RequestLog.retrieve(BardQueryInfo.class));
    }

    /**
     * Increments the number of fact queries.
     */
    public static void incrementCountFactHits() {
        getBardQueryInfo().factQueryCount.incrementAndGet();
    }

    /**
     * Increments the number of cache-hit queries.
     */
    public static void incrementCountCacheHits() {
        getBardQueryInfo().factCacheHitCount.incrementAndGet();
    }

    /**
     * Increments the number of weight check queries.
     */
    public static void incrementCountWeightCheck() {
        getBardQueryInfo().weightCheckCount.incrementAndGet();
    }
}
