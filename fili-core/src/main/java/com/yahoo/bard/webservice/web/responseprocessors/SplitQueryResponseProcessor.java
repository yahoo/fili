// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors;

import com.yahoo.bard.webservice.data.cache.HashDataCache.Pair;
import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.web.DataApiRequest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This response processor receives a list of expected intervals.  As responses arrives, it stores the responses until
 * all expected intervals have arrived, at which point it passes the concatenated Json content from each of the calls
 * to its next processor.
 */
public class SplitQueryResponseProcessor implements ResponseProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(SplitQueryResponseProcessor.class);
    public static final String EXTRA_RETURN_FORMAT = "Split query received the same interval more than once: %s";
    public static final String UNEXPECTED_INTERVAL_FORMAT = "Split query received an interval it wasn't expecting: %s";

    private final ResponseProcessor next;

    private final DruidAggregationQuery<?> queryBeforeSplit;
    private final Map<Interval, AtomicInteger> expectedIntervals;
    private final List<Pair<JsonNode, LoggingContext>> completedIntervals;
    private final AtomicInteger completed;
    private final AtomicBoolean failed = new AtomicBoolean(false);
    private final RequestLog logCtx;

    /**
     * Constructor.
     *
     * @param next  The next delegate response processor
     * @param request  The request for this response
     * @param druidQuery  The unmodified druid query for this response
     * @param expectedIntervals A map of expected interval status flags
     * @param logCtx  The request log logging context
     */
    @SuppressWarnings("unchecked")
    public SplitQueryResponseProcessor(
            ResponseProcessor next,
            DataApiRequest request,
            DruidAggregationQuery<?> druidQuery,
            Map<Interval, AtomicInteger> expectedIntervals,
            RequestLog logCtx
    ) {
        this.next = next;
        this.queryBeforeSplit = druidQuery;
        this.expectedIntervals = expectedIntervals;
        this.completedIntervals = Arrays.asList(new Pair[expectedIntervals.size()]);
        this.completed = new AtomicInteger(expectedIntervals.size());
        this.logCtx = logCtx;
    }

    @Override
    public ResponseContext getResponseContext() {
        return next.getResponseContext();
    }

    @Override
    public FailureCallback getFailureCallback(final DruidAggregationQuery<?> druidQuery) {
        return new FailureCallback() {
            final FailureCallback nextFail = next.getFailureCallback(druidQuery);

            @Override
            public void invoke(Throwable error) {
                if (failed.compareAndSet(false, true)) {
                    nextFail.invoke(error);
                }
            }
        };
    }

    @Override
    public HttpErrorCallback getErrorCallback(final DruidAggregationQuery<?> druidQuery) {
        return new HttpErrorCallback() {
            final HttpErrorCallback nextError = next.getErrorCallback(druidQuery);

            @Override
            public void invoke(int statusCode, String reasonPhrase, String responseBody) {
                if (failed.compareAndSet(false, true)) {
                    nextError.invoke(statusCode, reasonPhrase, responseBody);
                }
            }
        };
    }

    @Override
    public void processResponse(JsonNode json, DruidAggregationQuery<?> druidQuery, LoggingContext metadata) {
        if (failed.get()) {
            return;
        }

        AtomicInteger sharedIndex;
        Interval interval = druidQuery.getIntervals().get(0);
        if (interval == null || (sharedIndex = expectedIntervals.get(interval)) == null) {
            fail(UNEXPECTED_INTERVAL_FORMAT, druidQuery, interval);
            return;
        }

        int index;
        if ((index = sharedIndex.getAndSet(-1)) < 0 || completedIntervals.get(index) != null) {
            fail(EXTRA_RETURN_FORMAT, druidQuery, interval);
            return;
        }

        completedIntervals.set(index, new Pair<>(json, metadata));

        if (completed.decrementAndGet() == 0) {
            Pair<JsonNode, LoggingContext> mergedResponse = mergeResponses(completedIntervals);
            RequestLog.restore(mergedResponse.getValue().getRequestLog());
            next.processResponse(mergedResponse.getKey(), queryBeforeSplit, mergedResponse.getValue());
        }
    }

    /**
     * Fail the request.
     *
     * @param format  Format string for the failure message. Expects to take an interval
     * @param druidQuery  Druid query that failed
     * @param interval  Interval of the failing druid query
     */
    private void fail(String format, DruidAggregationQuery<?> druidQuery, Interval interval) {
        String message = String.format(format, interval);
        Exception e = new IllegalStateException(message);
        LOG.error(message, e);
        getFailureCallback(druidQuery).invoke(e);
    }

    /**
     * Take a list of Jackson ArrayNodes and merge their contents, preserving order.
     *
     * @param responses  A list of pairs that encompass JSON nodes and response metadata
     *
     * @return A new pair holding the merged json and the aggregate request log context
     */
    private Pair<JsonNode, LoggingContext> mergeResponses(List<Pair<JsonNode, LoggingContext>> responses) {
        JsonNodeFactory factory = new JsonNodeFactory(true);
        ArrayNode result = factory.arrayNode();
        RequestLog.restore(logCtx);
        for (Pair<JsonNode, LoggingContext> entry : responses) {
            for (JsonNode jsonNode : entry.getKey()) {
                result.add(jsonNode);
            }
            RequestLog.accumulate(entry.getValue().getRequestLog());
        }
        RequestLog updatedCtx = RequestLog.dump();
        return new Pair<>(result, new LoggingContext(updatedCtx));
    }
}
