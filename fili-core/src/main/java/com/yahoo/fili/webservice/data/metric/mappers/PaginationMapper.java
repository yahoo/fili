// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.data.metric.mappers;

import com.yahoo.fili.webservice.data.Result;
import com.yahoo.fili.webservice.data.ResultSet;
import com.yahoo.fili.webservice.data.ResultSetSchema;
import com.yahoo.fili.webservice.util.AllPagesPagination;
import com.yahoo.fili.webservice.util.Pagination;
import com.yahoo.fili.webservice.web.AbstractResponse;
import com.yahoo.fili.webservice.web.responseprocessors.MappingResponseProcessor;
import com.yahoo.fili.webservice.web.util.PaginationParameters;

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
     * @param uriBuilder  The builder for creating the pagination links.
     */
    public PaginationMapper(
            PaginationParameters paginationParameters,
            MappingResponseProcessor responseProcessor,
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
     *
     * @throws com.yahoo.fili.webservice.web.PageNotFoundException if the page requested is past the last page of data.
     */
    @Override
    public ResultSet map(ResultSet resultSet) {
        Pagination<Result> pages = new AllPagesPagination<>(resultSet, paginationParameters);
        AbstractResponse.addLinks(pages, uriBuilder, responseProcessor);
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
