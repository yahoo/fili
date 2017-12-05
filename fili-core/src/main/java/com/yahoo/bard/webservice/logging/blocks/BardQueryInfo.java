// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging.blocks;

import com.yahoo.bard.webservice.logging.LogInfo;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Main log of a request served by the TablesServlet.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class BardQueryInfo implements LogInfo {
    private static final Logger LOG = LoggerFactory.getLogger(BardQueryInfo.class);
    private static final String WEIGHT_CHECK = "weight check queries count";
    private static final String FACT_QUERIES = "fact queries count";
    private static final String FACT_QUERY_CACHE_HIT  = "fact query cache hit count";

    protected final String type;
    protected final Map<String, AtomicInteger> queryCounter;

    /**
     * Constructor.
     *
     * @param queryType  Type of Bard query
     */
    public BardQueryInfo(String queryType) {
        this.type = queryType;
        this.queryCounter = Stream.of(
                new AbstractMap.SimpleImmutableEntry<>(WEIGHT_CHECK, new AtomicInteger()),
                new AbstractMap.SimpleImmutableEntry<>(FACT_QUERIES, new AtomicInteger()),
                new AbstractMap.SimpleImmutableEntry<>(FACT_QUERY_CACHE_HIT, new AtomicInteger())
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
    public void incrementCountFactHits() {
        getBardQueryInfo().incrementCountFor(BardQueryInfo.FACT_QUERIES);
    }

    /**
     * Increments the number of cache-hit queries.
     */
    public void incrementCountCacheHits() {
        getBardQueryInfo().incrementCountFor(BardQueryInfo.FACT_QUERY_CACHE_HIT);
    }

    /**
     * Increments the number of weight check queries.
     */
    public void incrementCountWeightCheck() {
        getBardQueryInfo().incrementCountFor(BardQueryInfo.WEIGHT_CHECK);
    }

    /**
     * Increments the number of a type of query, whose possible type are all specified in
     * {@link com.yahoo.bard.webservice.logging.blocks.BardQueryInfo}.
     *
     * @param queryType  The type of the query
     */
    protected void incrementCountFor(String queryType) {
        AtomicInteger count = queryCounter.get(queryType);
        if (count == null) {
            String message = ErrorMessageFormat.RESOURCE_RETRIEVAL_FAILURE.format(queryType);
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }
        count.incrementAndGet();
    }
}
