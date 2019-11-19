// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import java.util.LinkedHashSet;
import java.util.Map;

/**
 * Slices API Request. Such an API Request binds, validates, and models the parts of a request to the slices endpoint.
 */
public interface SlicesApiRequest extends ApiRequest {
    String REQUEST_MAPPER_NAMESPACE = "slicesApiRequestMapper";
    String EXCEPTION_HANDLER_NAMESPACE = "exceptionHandler";

    /**
     * Returns a set of all available slices.
     *
     * @return the set of all available slices
     */
    LinkedHashSet<Map<String, String>> getSlices();

    /**
     * Returns a slice object.
     *
     * @return the slice object
     */
    Map<String, Object> getSlice();
}
