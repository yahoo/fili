// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers;

import com.yahoo.bard.webservice.data.metric.mappers.DateTimeSortMapper;
import com.yahoo.bard.webservice.druid.model.orderby.SortDirection;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.responseprocessors.MappingResponseProcessor;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor;

import javax.validation.constraints.NotNull;

/**
 * Injects the result set mapper for dateTime based sort if an API request asks for result in descending order.
 * The time sort mapper should be second last mappers to execute on the result set.
 */
public class DateTimeSortRequestHandler implements DataRequestHandler {
    private final @NotNull DataRequestHandler next;

    /**
     * Constructor.
     *
     * @param next  Next Handler in the chain
     */
    public DateTimeSortRequestHandler(DataRequestHandler next) {
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
        if (request.getDateTimeSort().isPresent() &&
                request.getDateTimeSort().get().getDirection().equals(SortDirection.DESC)
                ) {
            mappingResponse.getMappers().add(new DateTimeSortMapper());
        }
        return next.handleRequest(context, request, druidQuery, mappingResponse);
    }
}
