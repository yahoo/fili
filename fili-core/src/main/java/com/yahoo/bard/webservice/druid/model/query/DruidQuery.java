// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query;

import com.yahoo.bard.webservice.druid.model.QueryType;
import com.yahoo.bard.webservice.druid.model.datasource.DataSource;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Optional;

/**
 * Common interface for Druid Query classes.
 *
 * @param <Q> class that implements DruidQuery
 */
public interface DruidQuery<Q extends DruidQuery<? super Q>> {

    /**
     * Returns the type of the query.
     *
     * @return the query type
     */
    QueryType getQueryType();

    /**
     * Returns the data source of the query.
     *
     * @return the data source
     */
    DataSource getDataSource();

    /**
     * Returns a copy of this query with the specified data source on the innermost query.
     *
     * @param dataSource  the new data source
     *
     * @return the query copy
     */
    Q withInnermostDataSource(DataSource dataSource);

    /**
     * Returns the context of the query.
     * If the context is uninitialized it returns an initialized empty context
     *
     * @return the query context
     */
    QueryContext getContext();

    /**
     * If this query is nestable, and has a nested query return it.
     *
     * @return the nested query or empty if there is no nested query
     */
    @JsonIgnore
    default Optional<? extends DruidQuery> getInnerQuery() {
        return getDataSource().getQuery();
    }

    /**
     * If this structure is part of a query stack, return the lowest element.
     *
     * @return Return the most nested inner query OR the object itself if it has no children
     */
    @JsonIgnore
    default DruidQuery<?> getInnermostQuery() {
        return getInnerQuery().isPresent() ? getInnerQuery().get().getInnermostQuery() : this;
    }

    /**
     * Returns a copy of this query with the specified data source.
     *
     * @param dataSource  the new data source
     *
     * @return the query copy
     */
    Q withDataSource(DataSource dataSource);

    /**
     * Returns a copy of this query with the specified context.
     *
     * @param context  the new context
     *
     * @return the query copy
     */
    Q withContext(QueryContext context);
}
