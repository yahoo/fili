// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.DIMENSIONS_UNDEFINED;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.EMPTY_DICTIONARY;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadFilterException;
import com.yahoo.bard.webservice.web.ResponseFormatType;
import com.yahoo.bard.webservice.web.apirequest.generator.filter.FilterBinders;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;
import javax.ws.rs.core.Response;

/**
 * Dimension API Request Implementation binds, validates, and models the parts of a request to the dimension endpoint.
 */
public class DimensionsApiRequestImpl extends ApiRequestImpl implements DimensionsApiRequest {
    private static final Logger LOG = LoggerFactory.getLogger(DimensionsApiRequestImpl.class);

    private final LinkedHashSet<Dimension> dimensions;
    private final LinkedHashSet<ApiFilter> filters;

    protected FilterBinders filterBinders = FilterBinders.getInstance();

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
     *
     * @throws BadApiRequestException is thrown in the following scenarios:
     * <ol>
     *     <li>Invalid dimension in the API request.</li>
     *     <li>Invalid filter syntax in the API request.</li>
     *     <li>Invalid filter dimensions in the API request.</li>
     *     <li>Pagination parameters in the API request that are not positive integers.</li>
     * </ol>
     * @deprecated prefer constructor with download filename
     */
    @Deprecated
    public DimensionsApiRequestImpl(
            String dimension,
            String filters,
            String format,
            @NotNull String perPage,
            @NotNull String page,
            DimensionDictionary dimensionDictionary
    ) throws BadApiRequestException {
        this(dimension, filters, format, null, perPage, page, dimensionDictionary);
    }

    /**
     * Parses the API request URL and generates the Api Request object.
     *
     * @param dimension  single dimension URL
     * @param filters  URL filter query String in the format:
     * <pre>{@code
     * ((field name and operation):((multiple values bounded by [])or(single value))))(followed by , or end of string)
     * }</pre>
     * @param format  response data format JSON or CSV. Default is JSON.
     * @param downloadFilename If not null and not empty, indicates the response should be downloaded by the client with
     * the provided filename. Otherwise indicates the response should be rendered in the browser.
     * @param perPage  number of rows to display per page of results. If present in the original request,
     * must be a positive integer. If not present, must be the empty string.
     * @param page  desired page of results. If present in the original request, must be a positive
     * integer. If not present, must be the empty string.
     * @param dimensionDictionary  cache containing all the valid dimension objects.
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
            String downloadFilename,
            @NotNull String perPage,
            @NotNull String page,
            DimensionDictionary dimensionDictionary
    ) throws BadApiRequestException {
        super(format, downloadFilename, ApiRequest.SYNCHRONOUS_REQUEST_FLAG, perPage, page);

        // Zero or more grouping dimensions may be specified
        this.dimensions = generateDimensions(dimension, dimensionDictionary);

        // Zero or more filtering dimensions may be referenced
        this.filters = generateFilters(filters, dimensionDictionary);

        LOG.debug(
                "Api request: \nDimensions: {},\nFilters: {},\nFormat: {},\nFilename: {},\nPagination: {}",
                this.dimensions,
                this.filters,
                this.format,
                this.downloadFilename,
                this.paginationParameters
        );
    }

    /**
     * All argument constructor, meant to be used for rewriting apiRequest.
     *
     * @param format  Format of the request
     * @param paginationParameters  Pagination info for the request
     * @param dimensions  Desired dimensions of the request
     * @param filters  Filters applied to the request
     * @deprecated prefer constructor with downloadFilename
     */
    @Deprecated
    private DimensionsApiRequestImpl(
            ResponseFormatType format,
            Optional<PaginationParameters> paginationParameters,
            Iterable<Dimension> dimensions,
            Iterable<ApiFilter> filters
    ) {
        super(format, SYNCHRONOUS_ASYNC_AFTER_VALUE, paginationParameters);
        this.dimensions = Sets.newLinkedHashSet(dimensions);
        this.filters = Sets.newLinkedHashSet(filters);
    }
    /**
     * All argument constructor, meant to be used for rewriting apiRequest.
     *
     * @param format  Format of the request
     * @param downloadFilename If not null and not empty, indicates the response should be downloaded by the client with
     * the provided filename. Otherwise indicates the response should be rendered in the browser.
     * @param paginationParameters  Pagination info for the request
     * @param dimensions  Desired dimensions of the request
     * @param filters  Filters applied to the request
     */
    private DimensionsApiRequestImpl(
            ResponseFormatType format,
            String downloadFilename,
            PaginationParameters paginationParameters,
            Iterable<Dimension> dimensions,
            Iterable<ApiFilter> filters
    ) {
        super(format, downloadFilename, SYNCHRONOUS_ASYNC_AFTER_VALUE, paginationParameters);
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
        String[] apiFilters = filterQuery.split(COMMA_AFTER_BRACKET_PATTERN);

        for (String apiFilter : apiFilters) {
            ApiFilter newFilter;
            try {
                newFilter = generateFilter(apiFilter, dimensionDictionary);
            } catch (BadFilterException filterException) {
                // bad response if filter dimensions do not match requested dimension
                throw new BadApiRequestException(filterException.getMessage(), filterException);
            }

            generated.add(newFilter);
        }

        LOG.trace("Generated set of filters: {}", generated);
        return generated;
    }

    /**
     * Parses the URL filter Query and generates the ApiFilter object.
     *
     * @param apiFilter  Expects a URL filter query String in the format:
     * <code>(dimension name)|(field name)-(operation)[?(value or comma separated values)]?</code>
     * @param dimensionDictionary  cache containing all the valid dimension objects.
     *
     * @return the ApiFilter
     * @throws BadFilterException Exception when filter pattern is not matched or when any of its properties are not
     * valid.
     */
    protected ApiFilter generateFilter(String apiFilter, DimensionDictionary dimensionDictionary)
            throws BadFilterException {
        return FilterBinders.getInstance().generateApiFilter(apiFilter, dimensionDictionary);
    }

    @Override
    public DimensionsApiRequest withFormat(ResponseFormatType format) {
        return new DimensionsApiRequestImpl(format, downloadFilename, paginationParameters, dimensions, filters);
    }

    @Override
    public DimensionsApiRequest withPaginationParameters(PaginationParameters paginationParameters) {
        return new DimensionsApiRequestImpl(format, downloadFilename, paginationParameters, dimensions, filters);
    }

    @Override
    public DimensionsApiRequest withBuilder(Response.ResponseBuilder builder) {
        return new DimensionsApiRequestImpl(format, downloadFilename, paginationParameters, dimensions, filters);
    }

    @Override
    public DimensionsApiRequest withDimensions(LinkedHashSet<Dimension> dimensions) {
        return new DimensionsApiRequestImpl(format, downloadFilename, paginationParameters, dimensions, filters);
    }

    @Override
    public DimensionsApiRequest withFilters(Set<ApiFilter> filters) {
        return new DimensionsApiRequestImpl(format, downloadFilename, paginationParameters, dimensions, filters);
    }

    @Override
    public DimensionsApiRequest withDownloadFilename(String downloadFilename) {
        return new DimensionsApiRequestImpl(format, downloadFilename, paginationParameters, dimensions, filters);
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
