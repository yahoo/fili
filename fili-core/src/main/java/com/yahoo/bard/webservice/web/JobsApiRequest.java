// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

/**
 * Jobs API Request. Such an API Request binds, validates, and models the parts of a request to the Jobs endpoint.
 */
public interface JobsApiRequest extends ApiRequest {
    String REQUEST_MAPPER_NAMESPACE = "jobsApiRequestMapper";
}
