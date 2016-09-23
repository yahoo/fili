// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers;

import static com.yahoo.bard.webservice.web.responseprocessors.ResponseContextKeys.VOLATILE_INTERVALS_CONTEXT_KEY;

import com.yahoo.bard.webservice.data.volatility.VolatileIntervalsService;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.responseprocessors.MappingResponseProcessor;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseContext;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor;

import java.io.Serializable;
import java.util.Map;

import javax.validation.constraints.NotNull;

/**
 * A request handler that loads the volatile intervals for a request into the response context.
 */
public class VolatileDataRequestHandler implements DataRequestHandler {

    protected final @NotNull DataRequestHandler next;
    protected final @NotNull PhysicalTableDictionary physicalTableDictionary;
    protected final @NotNull VolatileIntervalsService volatileIntervalsService;

    /**
     * Detect volatile intervals and attach metadata describing the intervals to the request.
     *
     * @param next  The next Request handler to call
     * @param physicalTableDictionary the repository of slice data
     * @param volatileIntervalsService a provider for volatile intervals (per physical table)
     */
    public VolatileDataRequestHandler(
            DataRequestHandler next,
            PhysicalTableDictionary physicalTableDictionary,
            VolatileIntervalsService volatileIntervalsService
    ) {
        this.next = next;
        this.physicalTableDictionary = physicalTableDictionary;
        this.volatileIntervalsService = volatileIntervalsService;
    }

    @Override
    public boolean handleRequest(
            RequestContext context,
            DataApiRequest request,
            DruidAggregationQuery<?> druidQuery,
            ResponseProcessor response
    ) {
        if (!(response instanceof MappingResponseProcessor)) {
            throw new IllegalStateException("Volatile data request handler requires a mapping response.");
        }
        MappingResponseProcessor mappingResponse = (MappingResponseProcessor) response;

        // Gather the volatile intervals. A volatile interval in one data source make that interval volatile overall.
        SimplifiedIntervalList volatileIntervals = druidQuery.getInnermostQuery().getDataSource().getNames().stream()
                .map(physicalTableDictionary::get)
                .map(table -> volatileIntervalsService.getVolatileIntervals(
                             druidQuery.getGranularity(),
                             druidQuery.getIntervals(),
                             table
                ))
                .flatMap(SimplifiedIntervalList::stream)
                .collect(SimplifiedIntervalList.getCollector());

        if (!volatileIntervals.isEmpty()) {
            ResponseContext responseContext = response.getResponseContext();
            responseContext.put(VOLATILE_INTERVALS_CONTEXT_KEY.getName(), volatileIntervals);
        }

        return next.handleRequest(context, request, druidQuery, mappingResponse);
    }

    /**
     * Return the volatile intervals from the context.
     *
     * @param context  The RequestContext object containing the volatile intervals if any
     *
     * @return the volatile intervals from the request or an empty list
     */
    public static SimplifiedIntervalList getVolatileIntervalsWithDefault(Map<String, Serializable> context) {
        return (SimplifiedIntervalList) context.computeIfAbsent(
                VOLATILE_INTERVALS_CONTEXT_KEY.getName(),
                (ignored) -> new SimplifiedIntervalList()
        );
    }
}
