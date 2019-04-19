// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.client.impl;

import static javax.ws.rs.core.Response.Status.OK;

import com.yahoo.bard.webservice.config.CacheFeatureFlag;
import com.yahoo.bard.webservice.web.responseprocessors.DruidJsonResponseContentKeys;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.asynchttpclient.Response;

import java.io.IOException;
import java.util.function.Function;

/**
 * A strategy that takes a JSON Node built from a Response, nests it and makes it a peer with the Response headers.
 */
public class HeaderNestingJsonBuilderStrategy implements Function<Response, JsonNode> {

    private final Function<Response, JsonNode> baseStrategy;

    /**
     * Constructor.
     *
     * @param baseStrategy  strategy to do initial JSON Node construction
     */
    public HeaderNestingJsonBuilderStrategy(Function<Response, JsonNode> baseStrategy) {
        this.baseStrategy = baseStrategy;
    }

    @Override
    public JsonNode apply(Response response) {
        MappingJsonFactory mappingJsonFactory = new MappingJsonFactory();
        ObjectNode objectNode = JsonNodeFactory.instance.objectNode();
        objectNode.set(DruidJsonResponseContentKeys.RESPONSE.getName(), baseStrategy.apply(response));
        try {
            try (JsonParser parser = mappingJsonFactory.createParser(
                    response.getHeader(DruidJsonResponseContentKeys.DRUID_RESPONSE_CONTEXT.getName()))) {
                objectNode.set(
                        DruidJsonResponseContentKeys.DRUID_RESPONSE_CONTEXT.getName(),
                        parser.readValueAsTree()
                );
            }
            int statusCode = response.getStatusCode();
            try (JsonParser parser = mappingJsonFactory.createParser(String.valueOf(statusCode))) {
                objectNode.set(
                        DruidJsonResponseContentKeys.STATUS_CODE.getName(),
                        parser.readValueAsTree()
                );
            }
            if (CacheFeatureFlag.ETAG.isOn() && statusCode == OK.getStatusCode()) {
                try (JsonParser parser = mappingJsonFactory.createParser(
                        response.getHeader(DruidJsonResponseContentKeys.ETAG.getName()))) {
                    objectNode.set(
                            DruidJsonResponseContentKeys.ETAG.getName(),
                                    parser.readValueAsTree()
                    );
                }
            }
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
        return objectNode;
    }
}
