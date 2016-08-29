// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.DIMENSIONS_UNDEFINED;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.EMPTY_DICTIONARY;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

/**
 * Dimensions API Request. Such an API Request binds, validates, and models the parts of a request to the dimensions
 * endpoint.
 */
public class DimensionsApiRequest extends ApiRequest {
    private static final Logger LOG = LoggerFactory.getLogger(DimensionsApiRequest.class);
    public static final String REQUEST_MAPPER_NAMESPACE = "dimensionApiRequestMapper";

    private final Set<Dimension> dimensions;
    private final Set<ApiFilter> filters;

    /**
     * Parses the API request URL and generates the Api Request object.
     *
     * @param dimension  single dimension URL
     * @param filters  URL filter query String in the format:
     * <pre>{@code
     * ((field name and operation):((multiple values bounded by [])or(single value))))(followed by , or end of string)
     * }</pre>
     * @param format  response data format JSON or CSV. Default is JSON.
     * @param perPage  number of rows to display per page of results. If present in the original request,
     * must be a positive integer. If not present, must be the empty string.
     * @param page  desired page of results. If present in the original request, must be a positive
     * integer. If not present, must be the empty string.
     * @param dimensionDictionary  cache containing all the valid dimension objects.
     * @param uriInfo  The URI of the request object.
     *
     * @throws BadApiRequestException is thrown in the following scenarios:
     * <ol>
     *     <li>Invalid dimension in the API request.</li>
     *     <li>Invalid filter syntax in the API request.</li>
     *     <li>Invalid filter dimensions in the API request.</li>
     *     <li>Pagination parameters in the API request that are not positive integers.</li>
     * </ol>
     */
    public DimensionsApiRequest(
            String dimension,
            String filters,
            String format,
            @NotNull String perPage,
            @NotNull String page,
            DimensionDictionary dimensionDictionary,
            UriInfo uriInfo
    ) throws BadApiRequestException {
        super(format, perPage, page, uriInfo);

        // Zero or more grouping dimensions may be specified
        this.dimensions = generateDimensions(dimension, dimensionDictionary);

        // Zero or more filtering dimensions may be referenced
        this.filters = generateFilters(filters, dimensionDictionary);

        LOG.debug(
                "Api request: \nDimensions: {},\nFilters: {},\nFormat: {}\nPagination: {}",
                this.dimensions,
                this.filters,
                this.format,
                this.paginationParameters
        );
    }

    /**
     * No argument constructor, meant to be used only for testing.
     */
    @ForTesting
    protected DimensionsApiRequest() {
        super();
        this.dimensions = null;
        this.filters = null;
    }

    /**
     * All argument constructor, meant to be used for rewriting apiRequest.
     *
     * @param format  Format of the request
     * @param paginationParameters  Pagination info for the request
     * @param uriInfo  URI info
     * @param builder  A response builder for the request
     * @param dimensions  Desired dimensions of the request
     * @param filters  Filters applied to the request
     */
    private DimensionsApiRequest(
            ResponseFormatType format,
            Optional<PaginationParameters> paginationParameters,
            UriInfo uriInfo,
            Response.ResponseBuilder builder,
            Set<Dimension> dimensions,
            Set<ApiFilter> filters
    ) {
        super(format, SYNCHRONOUS_ASYNC_AFTER_VALUE, paginationParameters, uriInfo, builder);
        this.dimensions = dimensions;
        this.filters = filters;
    }

    /**
     * Returns a set of dimension names that contains either the requested dimension or all the available ones.
     *
     * @param apiDimension  Dimension string from the URL.
     * @param dimensionDictionary  Dimension dictionary contains the map of valid dimension names and dimension objects.
     *
     * @return Set of dimension objects.
     * @throws BadApiRequestException if an invalid dimension is requested or the dimension dictionary is empty.
     */
    protected Set<Dimension> generateDimensions(
            String apiDimension,
            DimensionDictionary dimensionDictionary
    ) throws BadApiRequestException {
        // Dimension is optional hence check if dimension is requested.
        Set<Dimension> generated = dimensionDictionary.findAll().stream()
                .filter(dimension -> apiDimension == null || apiDimension.equals(dimension.getApiName()))
                .collect(Collectors.toCollection(LinkedHashSet::new));

        if (generated.isEmpty()) {
            String msg;
            if (dimensionDictionary.findAll().isEmpty()) {
                msg = EMPTY_DICTIONARY.logFormat("Dimension");
            } else {
                msg = DIMENSIONS_UNDEFINED.logFormat(apiDimension);
            }
            LOG.error(msg);
            throw new BadApiRequestException(msg);
        }

        LOG.trace("Generated set of dimensions: {}", generated);
        return generated;
    }

    /**
     * Generates filter objects based on the filter query in the api request.
     *
     * @param filterQuery  Expects a URL filter query String in the format:
     * (dimension name).(fieldname)-(operation):[?(value or comma separated values)]?
     * @param dimensionDictionary  DimensionDictionary
     *
     * @return Set of filter objects.
     * @throws BadApiRequestException if the filter query string does not match required syntax.
     */
    protected Set<ApiFilter> generateFilters(
            String filterQuery,
            DimensionDictionary dimensionDictionary
    ) throws BadApiRequestException {
        LOG.trace("Dimension Dictionary: {}", dimensionDictionary);
        // Set of filter objects
        Set<ApiFilter> generated = new LinkedHashSet<>();

        // Filters are optional hence check if filters are requested.
        if (filterQuery == null || "".equals(filterQuery)) {
            return generated;
        }

        // split on '],' to get list of filters
        List<String> apiFilters = Arrays.asList(filterQuery.split(COMMA_AFTER_BRACKET_PATTERN));

        for (String apiFilter : apiFilters) {
            ApiFilter newFilter;
            try {
                newFilter = new ApiFilter(apiFilter, dimensionDictionary);
            } catch (BadFilterException filterException) {
                // bad response if filter dimensions do not match requested dimension
                throw new BadApiRequestException(filterException.getMessage(), filterException);
            }

            generated.add(newFilter);
        }

        LOG.trace("Generated set of filters: {}", generated);
        return generated;
    }

    // CHECKSTYLE:OFF
    public DimensionsApiRequest withFormat(ResponseFormatType format) {
        return new DimensionsApiRequest(format, paginationParameters, uriInfo, builder, dimensions, filters);
    }

    public DimensionsApiRequest withPaginationParameters(Optional<PaginationParameters> paginationParameters) {
        return new DimensionsApiRequest(format, paginationParameters, uriInfo, builder, dimensions, filters);
    }

    public DimensionsApiRequest withUriInfo(UriInfo uriInfo) {
        return new DimensionsApiRequest(format, paginationParameters, uriInfo, builder, dimensions, filters);
    }

    public DimensionsApiRequest withBuilder(Response.ResponseBuilder builder) {
        return new DimensionsApiRequest(format, paginationParameters, uriInfo, builder, dimensions, filters);
    }

    public DimensionsApiRequest withDimensions(Set<Dimension> dimensions) {
        return new DimensionsApiRequest(format, paginationParameters, uriInfo, builder, dimensions, filters);
    }

    public DimensionsApiRequest withFilters(Set<ApiFilter> filters) {
        return new DimensionsApiRequest(format, paginationParameters, uriInfo, builder, dimensions, filters);
    }
    // CHECKSTYLE:ON

    public Set<Dimension> getDimensions() {
        return this.dimensions;
    }

    public Dimension getDimension() {
        return this.dimensions.iterator().next();
    }

    public Set<ApiFilter> getFilters() {
        return this.filters;
    }
}
