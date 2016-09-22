// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data;

import com.yahoo.bard.webservice.web.ErrorMessageFormat;
import com.yahoo.bard.webservice.web.PreResponse;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseContext;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simplified version of PreResponse class for json format serialization.
 */
public class PreResponseSerializationProxy {

    private static final Logger LOG = LoggerFactory.getLogger(PreResponseSerializationProxy.class);

    public static final String RESULT_SET_KEY = "resultSet";
    public static final String RESPONSE_CONTEXT_KEY = "responseContext";

    private final ResultSetSerializationProxy resultSetSerializationProxy;
    private final String responseContext;

    /**
     * Constructor.
     *
     * @param preResponse  PreResponse object to be serialized
     * @param responseContextMapper  ObjectMapper instance with custom configuration to preserve the types
     */
    public PreResponseSerializationProxy(PreResponse preResponse, ObjectMapper responseContextMapper) {
        this.resultSetSerializationProxy = new ResultSetSerializationProxy(preResponse.getResultSet());
        this.responseContext = getSerializedResponseContext(preResponse.getResponseContext(), responseContextMapper);
    }

    @JsonProperty(RESULT_SET_KEY)
    public ResultSetSerializationProxy getResultSetSerializationProxy() {
        return resultSetSerializationProxy;
    }

    @JsonProperty(RESPONSE_CONTEXT_KEY)
    public String getResponseContext() {
        return responseContext;
    }

    /**
     * Custom serialization for ResponseContext object.
     *
     * @param responseContext  ResponseContext object to be serialized
     * @param responseContextMapper  objectMapper instance which preservers object types
     *
     * @return  Serialized responseContext
     */
    private String getSerializedResponseContext(ResponseContext responseContext, ObjectMapper responseContextMapper) {
        try {
            return responseContextMapper.writeValueAsString(responseContext);
        } catch (JsonProcessingException e) {
            String msg = ErrorMessageFormat.UNABLE_TO_SERIALIZE.format("ResponseContext");
            LOG.error(msg, e);
            throw new DeserializationException(msg, e);
        }
    }
}
