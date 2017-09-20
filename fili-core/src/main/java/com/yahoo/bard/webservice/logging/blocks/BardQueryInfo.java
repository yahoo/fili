// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging.blocks;

import com.yahoo.bard.webservice.logging.LogInfo;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;

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
    @JsonIgnore
    public static final String WEIGHT_CHECK = "weight check queries count";
    @JsonIgnore
    public static final String FACT_QUERIES = "fact queries count";
    @JsonIgnore
    public static final String FACT_QUERY_CACHE_HIT  = "fact query cache hit count";

    @JsonIgnore
    private static final Logger LOG = LoggerFactory.getLogger(BardQueryInfo.class);

    protected final String type;
    protected final Map<String, AtomicInteger> queryCounter;

    /**
     * Constructor.
     *
     * @param type  Type of Bard query
     */
    public BardQueryInfo(String type) {
        this.type = type;
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
     * Increments the number of a kind query, whose possible type are all specified in
     * {@link com.yahoo.bard.webservice.logging.blocks.BardQueryInfo}.
     *
     * @param query  The type of the query
     */
    public void incrementCountFor(String query) {
        try {
            queryCounter.get(query).incrementAndGet();
        } catch (NullPointerException exception) {
            String message = ErrorMessageFormat.RESOURCE_RETRIEVAL_FAILURE.format(query);
            LOG.error(message);
            throw new RuntimeException(message, exception);
        }
    }
}
