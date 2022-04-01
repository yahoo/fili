// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging.blocks;

import com.yahoo.bard.webservice.logging.LogInfo;
import com.yahoo.bard.webservice.logging.RequestLog;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
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
    public static final String FACT_PUT_ERRORS = "factCachePutErrors";
    public static final String FACT_PUT_TIMEOUTS = "factCachePutTimeouts";

    public static final String WEIGHT_CHECK_LINES_OUTPUT = "weightCheckLinesOutput";
    public static final String WEIGHT_CHECK_SKETCHES_OUTPUT = "weightCheckSketchesOutput";
    public static final String WEIGHT_CHECK_SCANNED_LINES = "weightCheckLinesScanned";
    public static final String WEIGHT_CHECK_SKETCHES_SCANNED = "weightCheckSketchesScanned";

    private final String type;
    private final AtomicLong weightCheckCount = new AtomicLong();
    private final AtomicLong factQueryCount = new AtomicLong();
    private final AtomicLong factCacheHitCount = new AtomicLong();
    private final AtomicLong factPutErrorsCount = new AtomicLong();
    private final AtomicLong factPutTimeoutsCount = new AtomicLong();
    private final Map<String, BardCacheInfo> cacheStatsMap = new TreeMap<>();

    private final AtomicLong sketchesScanned = new AtomicLong();
    private final AtomicLong linesScanned = new AtomicLong();
    private final AtomicLong sketchesOutput = new AtomicLong();
    private final AtomicLong linesOutput = new AtomicLong();

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

    public Map<String, AtomicLong> getQueryCounter() {

        Map<String, AtomicLong> counters = new TreeMap<>(Stream.of(
                new AbstractMap.SimpleImmutableEntry<>(WEIGHT_CHECK, weightCheckCount),
                new AbstractMap.SimpleImmutableEntry<>(FACT_QUERIES, factQueryCount),
                new AbstractMap.SimpleImmutableEntry<>(FACT_QUERY_CACHE_HIT, factCacheHitCount),
                new AbstractMap.SimpleImmutableEntry<>(FACT_PUT_ERRORS, factPutErrorsCount),
                new AbstractMap.SimpleImmutableEntry<>(FACT_PUT_TIMEOUTS, factPutTimeoutsCount)
        ).collect(Collectors.toMap(
                AbstractMap.SimpleImmutableEntry::getKey,
                AbstractMap.SimpleImmutableEntry::getValue
        )));
        if (weightCheckCount.get() > 0) {
            counters.put(WEIGHT_CHECK_LINES_OUTPUT, linesOutput);
            counters.put(WEIGHT_CHECK_SKETCHES_OUTPUT, sketchesOutput);
            counters.put(WEIGHT_CHECK_SCANNED_LINES, linesScanned);
            counters.put(WEIGHT_CHECK_SKETCHES_SCANNED, sketchesScanned);
        }
        return counters;
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

    /**
     * Increments the number of cache set failure count.
     */
    public static void incrementCountCacheSetFailures() {
        getBardQueryInfo().factPutErrorsCount.incrementAndGet();
    }

    /**
     * Increments the count of scanned sketches.
     *
     * @param addend  Amount of raw sketches to add.
     */
    public static void accumulateSketchesScanned(long addend) {
        getBardQueryInfo().sketchesScanned.getAndAdd(addend);
    }

    /**
     * Increments the count of scanned lines.
     *
     * @param addend  Amount of raw sketches to add.
     */
    public static void accumulateLinesScanned(long addend) {
        getBardQueryInfo().linesScanned.getAndAdd(addend);
    }
    /**
     * Increments the count of output sketches.
     *
     * @param addend  Amount of raw sketches to add.
     */
    public static void accumulateSketchesOutput(long addend) {
        getBardQueryInfo().sketchesOutput.getAndAdd(addend);
    }

    /**
     * Increments the count of output lines.
     *
     * @param addend  Amount of raw sketches to add.
     */
    public static void accumulateLinesOutput(long addend) {
        getBardQueryInfo().linesOutput.getAndAdd(addend);
    }

    /**
     * Increments the number of cache set timeout failure count.
     */
    public static void incrementCountCacheSetTimeoutFailures() {
        getBardQueryInfo().factPutTimeoutsCount.getAndIncrement()
        ;
    }

    /**
     * Adds Query key cksum to BardCacheInfo object in the cache stats map.
     * @param cksum Cksum of Cache key.
     * @param infoLog Info log object that holds other details like cache key and value length etc.
     */
    public static void addCacheInfo(String cksum, BardCacheInfo infoLog) {
        getBardQueryInfo().cacheStatsMap.put(cksum, infoLog);
    }

    /**
     * Serialize cache put stats log blocks.
     *
     * @return List of all BardCacheInfo.
     */
    public Collection<BardCacheInfo> getCacheStats() {
        return cacheStatsMap.values();
    }
}
