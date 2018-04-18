// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import javax.ws.rs.core.StreamingOutput;

/**
 * An interface for classes that transform response data to a format representation (e.g. JSON, CSV and others).
 */
@FunctionalInterface
public interface ResponseStream {
    /**
     * Gets a resource method that can be used to stream this response as an entity.
     *
     * @return The resource method
     */
    StreamingOutput getResponseStream();
}
