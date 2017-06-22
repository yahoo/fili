// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice;

import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.concurrent.Future;

/**
 * Allows queries to be executed on a sql backend from a {@link DruidAggregationQuery}.
 */
public interface SqlBackedClient {
    /**
     * Uses a {@link DruidAggregationQuery} to fetch results from a Sql client,
     * parses the results from Sql and returns an equivalent {@link JsonNode}
     * to what druid would respond with.
     *
     * @param druidQuery  The query to be executed.
     *
     * @return a json result replicating druid's responses.
     */
    Future<JsonNode> executeQuery(DruidAggregationQuery<?> druidQuery);
}
