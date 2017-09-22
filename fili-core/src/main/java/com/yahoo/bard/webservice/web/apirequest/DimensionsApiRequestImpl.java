// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.DIMENSIONS_UNDEFINED;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.EMPTY_DICTIONARY;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.BadFilterException;
import com.yahoo.bard.webservice.web.DimensionsApiRequest;
import com.yahoo.bard.webservice.web.ForTesting;
import com.yahoo.bard.webservice.web.ResponseFormatType;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import com.google.common.collect.Sets;

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
 * Dimension API Request Implementation binds, validates, and models the parts of a request to the dimension endpoint.
 */
public class DimensionsApiRequestImpl extends ApiRequestImpl implements DimensionsApiRequest {
    private static final Logger LOG = LoggerFactory.getLogger(DimensionsApiRequestImpl.class);

    private final LinkedHashSet<Dimension> dimensions;
    private final LinkedHashSet<ApiFilter> filters;

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
    public DimensionsApiRequestImpl(
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
     *
     * @deprecated it's not a good practice to have testing code here. This constructor will be removed entirely.
     */
    @Deprecated
    @ForTesting
    protected DimensionsApiRequestImpl() {
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
    private DimensionsApiRequestImpl(
            ResponseFormatType format,
            Optional<PaginationParameters> paginationParameters,
            UriInfo uriInfo,
            Response.ResponseBuilder builder,
            Iterable<Dimension> dimensions,
            Iterable<ApiFilter> filters
    ) {
        super(format, SYNCHRONOUS_ASYNC_AFTER_VALUE, paginationParameters, uriInfo, builder);
        this.dimensions = Sets.newLinkedHashSet(dimensions);
        this.filters = Sets.newLinkedHashSet(filters);
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
    protected LinkedHashSet<Dimension> generateDimensions(
            String apiDimension,
            DimensionDictionary dimensionDictionary
    ) throws BadApiRequestException {
        // Dimension is optional hence check if dimension is requested.
        LinkedHashSet<Dimension> generated = dimensionDictionary.findAll().stream()
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
    protected LinkedHashSet<ApiFilter> generateFilters(
            String filterQuery,
            DimensionDictionary dimensionDictionary
    ) throws BadApiRequestException {
        LOG.trace("Dimension Dictionary: {}", dimensionDictionary);
        // Set of filter objects
        LinkedHashSet<ApiFilter> generated = new LinkedHashSet<>();

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

    @Override
    public DimensionsApiRequest withFormat(ResponseFormatType format) {
        return new DimensionsApiRequestImpl(format, paginationParameters, uriInfo, builder, dimensions, filters);
    }

    @Override
    public DimensionsApiRequest withPaginationParameters(Optional<PaginationParameters> paginationParameters) {
        return new DimensionsApiRequestImpl(format, paginationParameters, uriInfo, builder, dimensions, filters);
    }

    @Override
    public DimensionsApiRequest withUriInfo(UriInfo uriInfo) {
        return new DimensionsApiRequestImpl(format, paginationParameters, uriInfo, builder, dimensions, filters);
    }

    @Override
    public DimensionsApiRequest withBuilder(Response.ResponseBuilder builder) {
        return new DimensionsApiRequestImpl(format, paginationParameters, uriInfo, builder, dimensions, filters);
    }

    @Override
    public DimensionsApiRequest withDimensions(LinkedHashSet<Dimension> dimensions) {
        return new DimensionsApiRequestImpl(format, paginationParameters, uriInfo, builder, dimensions, filters);
    }

    @Override
    public DimensionsApiRequest withFilters(Set<ApiFilter> filters) {
        return new DimensionsApiRequestImpl(format, paginationParameters, uriInfo, builder, dimensions, filters);
    }

    @Override
    public LinkedHashSet<Dimension> getDimensions() {
        return this.dimensions;
    }

    @Override
    public Dimension getDimension() {
        return this.dimensions.iterator().next();
    }

    @Override
    public Set<ApiFilter> getFilters() {
        return this.filters;
    }
}
