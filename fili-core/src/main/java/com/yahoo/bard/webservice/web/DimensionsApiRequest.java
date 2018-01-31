// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

/**
 * Dimensions API Request. Such an API Request binds, validates, and models the parts of a request to the dimensions
 * endpoint.
 */
public interface DimensionsApiRequest extends ApiRequest {
    String REQUEST_MAPPER_NAMESPACE = "dimensionsApiRequestMapper";

    // CHECKSTYLE:OFF
    DimensionsApiRequest withFormat(ResponseFormatType format);

    DimensionsApiRequest withPaginationParameters(Optional<PaginationParameters> paginationParameters);

    DimensionsApiRequest withUriInfo(UriInfo uriInfo);

    DimensionsApiRequest withBuilder(Response.ResponseBuilder builder);

    DimensionsApiRequest withDimensions(LinkedHashSet<Dimension> dimensions);

    DimensionsApiRequest withFilters(Set<ApiFilter> filters);

    LinkedHashSet<Dimension> getDimensions();

    Dimension getDimension();

    Set<ApiFilter> getFilters();
    // CHECKSTYLE:ON
}
