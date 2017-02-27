// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers;

import com.yahoo.bard.webservice.data.metric.mappers.DateTimeSortMapper;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.responseprocessors.MappingResponseProcessor;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor;

import javax.validation.constraints.NotNull;

/**
 * Injects the result set mapper for dateTime based sort request.
 * The time sort mapper should be second last mappers to execute in workflow.
 */
public class DateTimeSortRequestHandler implements DataRequestHandler {
    private final  DataRequestHandler next;

    /**
     * Constructor.
     *
     * @param next  Next Handler in the chain
     */
    public DateTimeSortRequestHandler(@NotNull DataRequestHandler next) {
        this.next = next;
    }

    @Override
    public boolean handleRequest(
            RequestContext context,
            DataApiRequest request,
            DruidAggregationQuery<?> druidQuery,
            ResponseProcessor response
    ) {
        MappingResponseProcessor mappingResponse = (MappingResponseProcessor) response;
        if (request.getDateTimeSort().isPresent()) {
            mappingResponse.getMappers().add(new DateTimeSortMapper(request.getDateTimeSort().get().getDirection()));
        }
        return next.handleRequest(context, request, druidQuery, mappingResponse);
    }
}
