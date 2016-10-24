// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.EMPTY_INTERVAL_FORMAT;

import com.yahoo.bard.webservice.application.MetricRegistryFactory;
import com.yahoo.bard.webservice.druid.model.query.AllGranularity;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.util.IntervalUtils;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor;
import com.yahoo.bard.webservice.web.responseprocessors.SplitQueryResponseProcessor;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.validation.constraints.NotNull;
import javax.ws.rs.core.Response;

/**
 * Request handler breaks a query up into smaller time grain queries for parallel processing.
 * <p>
 * It creates a common response processor which serves as an accumulator to receive all replies before delegating to the
 * result set processing.
 */
public class SplitQueryRequestHandler implements DataRequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SplitQueryRequestHandler.class);
    private static final MetricRegistry REGISTRY = MetricRegistryFactory.getRegistry();
    public static final Meter SPLIT_QUERIES = REGISTRY.meter("queries.meter.split_queries.sub_queries");
    public static final Meter SPLITS = REGISTRY.meter("queries.meter.split_queries.splits");

    protected final @NotNull DataRequestHandler next;

    /**
     * Build a Split Query Request Handler.
     *
     * @param next  The next handler in the chain
     */
    public SplitQueryRequestHandler(DataRequestHandler next) {
        this.next = next;
    }

    @Override
    public boolean handleRequest(
            final RequestContext context,
            final DataApiRequest request,
            final DruidAggregationQuery<?> druidQuery,
            final ResponseProcessor response
    ) {
        Granularity granularity = druidQuery.getGranularity();
        List<Interval> queryIntervals = druidQuery.getIntervals();

        if (granularity instanceof AllGranularity) {
           // For "all" grain there is only one response bucket, so can't really split
           return next.handleRequest(context, request, druidQuery, response);
        }

        Map<Interval, AtomicInteger> expectedIntervals = Collections.unmodifiableMap(
                IntervalUtils.getSlicedIntervals(queryIntervals, granularity)
        );

        int numberOfIntervals = expectedIntervals.size();

        if (numberOfIntervals == 0) {
            String message = EMPTY_INTERVAL_FORMAT.format(request.getIntervals());
            int badRequestCode = Response.Status.BAD_REQUEST.getStatusCode();
            response.getErrorCallback(druidQuery).dispatch(badRequestCode, message, message);
            return true;
        }

        // Currently this is the only place where we fork multiple queries from a single query.
        // Here we check that this is correct and we also save the number of sub-queries.
        if (
                !context.getNumberOfIncoming().compareAndSet(1, numberOfIntervals) ||
                !context.getNumberOfOutgoing().compareAndSet(1, numberOfIntervals)
        ) {
            String msg = "Number of sub-queries not equal to \"one\" before query splitting. Possible race condition.";
            msg += "Incoming: " + context.getNumberOfIncoming().get();
            msg += ".Outgoing: " + context.getNumberOfIncoming().get();
            LOG.error(msg);
            throw new IllegalStateException(msg);
        }

        final List<DruidAggregationQuery<?>> queries = new ArrayList<>(numberOfIntervals);

        // First generate the query descriptors for each sub-query
        expectedIntervals.keySet()
                .stream()
                .forEachOrdered(interval -> queries.add(
                        druidQuery.withAllIntervals(Collections.singletonList(interval)))
                );

        // Save RequestLog up to here
        final RequestLog logCtx = RequestLog.dump();

        final SplitQueryResponseProcessor mergingResponse =
                new SplitQueryResponseProcessor(response, request, druidQuery, expectedIntervals, logCtx);

        if (numberOfIntervals > 1) {
            SPLITS.mark(1);
            SPLIT_QUERIES.mark(numberOfIntervals);
        }

        queries.stream().forEach(
                q -> {
                    RequestLog.restore(logCtx);
                    next.handleRequest(context, request, q, mergingResponse);
                }
        );

        return true;
    }
}
