// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.mappers;

import com.yahoo.bard.webservice.data.Result;
import com.yahoo.bard.webservice.data.ResultSet;
import com.yahoo.bard.webservice.data.ResultSetSchema;
import com.yahoo.bard.webservice.logging.blocks.BardQueryInfo;
import com.yahoo.bard.webservice.util.AllPagesPagination;
import com.yahoo.bard.webservice.util.Pagination;
import com.yahoo.bard.webservice.web.AbstractResponse;
import com.yahoo.bard.webservice.web.responseprocessors.MappingResponseProcessor;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import javax.validation.constraints.NotNull;
import javax.ws.rs.core.UriBuilder;

/**
 * Extracts the requested page of data from the Druid results. Behavior is undefined if the page requested is
 * less than 1, or the requested number of results on each page is less than 1.
 */
public class PaginationMapper extends ResultSetMapper {

    private final PaginationParameters paginationParameters;
    private final MappingResponseProcessor responseProcessor;
    private final UriBuilder uriBuilder;

    /**
     * Constructor.
     *
     * @param paginationParameters  The parameters needed for pagination
     * @param responseProcessor  The API response to which we can add the header links.
     * @param uriBuilder  The builder for creating the pagination links. (Optional)
     */
    public PaginationMapper(
            @NotNull PaginationParameters paginationParameters,
            @NotNull MappingResponseProcessor responseProcessor,
            UriBuilder uriBuilder
    ) {
        this.paginationParameters = paginationParameters;
        this.responseProcessor = responseProcessor;
        this.uriBuilder = uriBuilder;
    }

    /**
     *  Cuts the result set down to just the page requested.
     *
     * @param resultSet  The result set to be cut down.
     *
     * @return The page of results desired.
     */
    @Override
    public ResultSet map(ResultSet resultSet) {
        BardQueryInfo.accumulateDruidResponseSize(resultSet.size());
        Pagination<Result> pages = new AllPagesPagination<>(resultSet, paginationParameters);
        if (uriBuilder != null) {
            AbstractResponse.addLinks(pages, uriBuilder, responseProcessor);
        }
        //uses map for additional flexibility and robustness, even though it is currently a no-op.
        return new ResultSet(map(resultSet.getSchema()), pages.getPageOfData());
    }

    @Override
    protected Result map(Result result, ResultSetSchema schema) {
        //Not needed, because this mapper overrides map(ResultSet). So it is just a no-op.
        return result;
    }


    @Override
    protected ResultSetSchema map(ResultSetSchema schema) {
        //Because this method is not necessary, it just returns the schema unchanged.
        return schema;
    }
}
