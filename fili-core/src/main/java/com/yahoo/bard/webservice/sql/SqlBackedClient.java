// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql;

import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.SuccessCallback;
import com.yahoo.bard.webservice.druid.model.query.DruidQuery;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.concurrent.Future;

/**
 * Allows queries to be executed on a sql backend from a {@link DruidQuery}.
 */
public interface SqlBackedClient {
    /**
     * Uses a {@link DruidQuery} to fetch results from a Sql client,
     * parses the results from Sql and returns an equivalent {@link JsonNode}
     * to what druid would respond with.
     *
     * @param druidQuery  The query to be executed.
     * @param successCallback  The callback for handling a successful result.
     * @param failureCallback  The callback for handling exceptions
     *
     * @return a json result replicating druid's responses.
     */
    Future<JsonNode> executeQuery(
            DruidQuery<?> druidQuery,
            SuccessCallback successCallback,
            FailureCallback failureCallback
    );
}
