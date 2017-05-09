// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.client.impl;

import com.yahoo.bard.webservice.druid.client.DruidServiceConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.asynchttpclient.Response;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

import javax.ws.rs.core.Response.Status;

/**
 * Represents the druid web service endpoint for partial data V2.
 */
public class AsyncDruidWebServiceImplV2 extends AsyncDruidWebServiceImpl {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * IOC constructor.
     *
     * @param config  the configuration for this druid service
     * @param mapper  A shared jackson object mapper resource
     * @param headersToAppend Supplier for map of headers for Druid requests
     */
    public AsyncDruidWebServiceImplV2(
            DruidServiceConfig config,
            ObjectMapper mapper,
            Supplier<Map<String, String>> headersToAppend
    ) {
        super(config, mapper, headersToAppend);
    }

    @Override
    protected boolean hasError(Status status) {
        return status != Status.OK && status != Status.NOT_MODIFIED;
    }

    @Override
    protected JsonNode constructJsonResponse(Response response) throws IOException {
        ObjectNode objectNode = (ObjectNode) OBJECT_MAPPER.readTree(response.getResponseBody());
        objectNode.put("X-Druid-Response-Context", response.getHeader("X-Druid-Response-Context"));
        objectNode.put("status-code", response.getStatusCode());

        return objectNode;
    }
}
