// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl;

import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TimeLimitingCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.util.Counter;

import java.io.IOException;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Splits a time limited lucene search for parallel execution.
 */
public class TimeLimitingCollectorManager implements
        CollectorManager<TimeLimitingCollectorManager.AccessibleTimeLimitingCollector, TopDocs> {

    final private int searchTimeoutMs;
    final private int perPage;
    final private ScoreDoc lastEntry;

    /**
     * Constructor.
     * @param lastEntry  The last entry from the previous search, the indexSearcher will begin its
     * search after this entry (if lastEntry is null, the indexSearcher will begin its search from the beginning)
     * @param perPage  The number of entries per page
     * @param searchTimeoutMs timeout in milliseconds for the lucene search
     */
    TimeLimitingCollectorManager(int searchTimeoutMs, ScoreDoc lastEntry, int perPage) {
        this.searchTimeoutMs = searchTimeoutMs;
        this.lastEntry = lastEntry;
        this.perPage = perPage;
    }

    /**
     * Works around the lack of a way to get at the underlying Collector in TimeLimitingCollector.
     */
    class AccessibleTimeLimitingCollector extends TimeLimitingCollector {
        final private TopScoreDocCollector wrappedCollector;

        /**
         * Constructor.
         * @param collector the wrapped collector
         * @param clock the clock used to implement the timeout
         * @param ticksAllowed the amount of time allowed for the search
         */
        AccessibleTimeLimitingCollector(TopScoreDocCollector collector, Counter clock, long ticksAllowed) {
            super(collector, clock, ticksAllowed);
            wrappedCollector = collector;
        }

        /**
         * This is the method TimeLimitingCollector was missing.
         * @return the wrapped collector
         */
        public TopScoreDocCollector getWrappedCollector() {
            return wrappedCollector;
        }
    }

    @Override
    public AccessibleTimeLimitingCollector newCollector() throws IOException {
        return new AccessibleTimeLimitingCollector(
                TopScoreDocCollector.create(perPage, lastEntry), Counter.newCounter(false), searchTimeoutMs
        );
    }

    @Override
    public TopDocs reduce(Collection<AccessibleTimeLimitingCollector> collectors) throws IOException {
        TopDocs[] docs = collectors.stream()
                .map(AccessibleTimeLimitingCollector::getWrappedCollector)
                .map(TopDocsCollector::topDocs)
                .collect(Collectors.toList())
                .toArray(new TopDocs[collectors.size()]);

        return TopDocs.merge(perPage, docs);
    }
}
