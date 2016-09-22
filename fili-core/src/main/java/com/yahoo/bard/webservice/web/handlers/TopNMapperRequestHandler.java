// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers;

import com.yahoo.bard.webservice.data.metric.mappers.TopNResultSetMapper;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.druid.model.query.TopNQuery;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.responseprocessors.MappingResponseProcessor;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor;

import javax.validation.constraints.NotNull;

/**
 * Adds the topN result set mapper to allow for top N aggregations based on general group by druid queries.
 * Its priority should be high in the chain of mappers. Currently it's fixed to be right after the partial data mapper.
 * That's why this handler needs to be injected right after PartialDataRequestHandler in the chain of request handlers.
 */
public class TopNMapperRequestHandler implements DataRequestHandler {
    private final @NotNull DataRequestHandler next;

    /**
     * Build a topN request handler.
     *
     * @param next  The next handler in the chain
     */
    public TopNMapperRequestHandler(DataRequestHandler next) {
        this.next = next;
    }

    @Override
    public boolean handleRequest(
            RequestContext context,
            DataApiRequest request,
            DruidAggregationQuery<?> druidQuery,
            ResponseProcessor response
    ) {
        if (request.getTopN().isPresent() && !(druidQuery instanceof TopNQuery)) {
            TopNResultSetMapper mapper = new TopNResultSetMapper(request.getTopN().getAsInt());
            // Add topN mapper after partial data mapper and before any other mapper
            try {
                // Index is 1 because we assume that partial data result set mapper has been injected already
                // by the PartialDataRequestHandler
                ((MappingResponseProcessor) response).getMappers().add(1, mapper);
            } catch (ClassCastException cce) {
                throw new IllegalStateException("TopN request handler requires a mapping response processor.", cce);
            }
        }
        return next.handleRequest(context, request, druidQuery, response);
    }
}
