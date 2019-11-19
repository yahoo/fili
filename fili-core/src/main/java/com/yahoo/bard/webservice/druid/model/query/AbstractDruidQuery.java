// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query;

import com.yahoo.bard.webservice.druid.model.QueryType;
import com.yahoo.bard.webservice.druid.model.datasource.DataSource;
import com.yahoo.bard.webservice.logging.RequestLog;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Collections;

/**
 * Base class for druid queries.
 *
 * @param <Q>  Type of AbstractDruidQuery this one extends. This allows the queries to nest their own type.
 */
public abstract class AbstractDruidQuery<Q extends AbstractDruidQuery<? super Q>> implements DruidQuery<Q> {

    protected final QueryType queryType;

    protected final DataSource dataSource;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    protected final QueryContext context;

    /**
     * Constructor.
     *
     * @param queryType  The type of this query
     * @param dataSource  The datasource
     * @param context  The context
     * @param incrementQueryId  true to fork a new context and bump up the query id, or false to create an exact copy
     * of the context.
     */
    protected AbstractDruidQuery(
            QueryType queryType,
            DataSource dataSource,
            QueryContext context,
            boolean incrementQueryId
    ) {
        this.queryType = queryType;
        this.dataSource = dataSource;
        this.context = context == null ?
                new QueryContext(Collections.<QueryContext.Param, Object>emptyMap(), null)
                        .withQueryId(RequestLog.getId()) :
                incrementQueryId ? context.fork() : context;
    }

    @Override
    public QueryType getQueryType() {
        return queryType;
    }

    @Override
    public DataSource getDataSource() {
        return dataSource;
    }

    @Override
    public QueryContext getContext() {
        return context;
    }
}
