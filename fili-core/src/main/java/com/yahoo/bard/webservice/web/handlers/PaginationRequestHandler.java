// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers;

import com.yahoo.bard.webservice.data.metric.mappers.PaginationMapper;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.responseprocessors.MappingResponseProcessor;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import javax.validation.constraints.NotNull;

/**
 * Injects the result set mapper for data pagination if an API request asks for pagination.
 * The pagination mapper should be one of the last mappers to execute on the result set (in particular, it should
 * execute _after_ any mappers that delete or add rows). Therefore, the PaginationRequestHandler should be one of the
 * last handlers added to the handler chain.
 */
public class PaginationRequestHandler implements DataRequestHandler {
    private final @NotNull DataRequestHandler next;

    /**
     * Constructor.
     *
     * @param next  Next Handler in the chain
     */
    public PaginationRequestHandler(DataRequestHandler next) {
        this.next = next;
    }

    @Override
    public boolean handleRequest(
            RequestContext context,
            DataApiRequest request,
            DruidAggregationQuery<?> druidQuery,
            ResponseProcessor response
    ) {
        if (!(response instanceof MappingResponseProcessor)) {
            throw new IllegalStateException("Pagination request handler requires a mapping response.");
        }
        MappingResponseProcessor mappingResponse = (MappingResponseProcessor) response;
        if (request.getPaginationParameters().isPresent()) {
            PaginationParameters paginationParameters = request.getPaginationParameters().get();
            mappingResponse.getMappers().add(
                    new PaginationMapper(
                            paginationParameters,
                            mappingResponse,
                            request.getUriInfo().getRequestUriBuilder()
                    )
            );
        }
        return next.handleRequest(context, request, druidQuery, mappingResponse);
    }
}
