// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import javax.validation.constraints.NotNull;

/**
 * Base class for request handlers.
 */
public abstract class BaseDataRequestHandler implements DataRequestHandler {

    protected final @NotNull ObjectMapper mapper;
    protected final @NotNull ObjectWriter writer;

    /**
     * Capture the ObjectMapper and build an ObjectWriter for JSON.
     *
     * @param mapper  a shared object mapper instance
     */
    public BaseDataRequestHandler(ObjectMapper mapper) {
        this.mapper = mapper;
        this.writer = mapper.writer();
    }
}
