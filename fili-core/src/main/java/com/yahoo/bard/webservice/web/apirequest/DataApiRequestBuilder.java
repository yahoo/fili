// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import com.yahoo.bard.webservice.config.BardFeatureFlag;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.util.EnumUtils;
import com.yahoo.bard.webservice.web.ApiHaving;
import com.yahoo.bard.webservice.web.ResponseFormatType;
import com.yahoo.bard.webservice.web.apirequest.generator.Generator;
import com.yahoo.bard.webservice.web.filters.ApiFilters;
import com.yahoo.bard.webservice.web.util.BardConfigResources;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Builder for {@link DataApiRequestValueObject} objects.
 *
 * Construction and validation of some resources may depend on some other request resources. For example, some
 * {@link Dimension}s may only be accessible on specific {@link LogicalTable}s. In this case, the dimension generator
 * needs to ensure that all grouping dimensions are against a logical table that supports them, and thus it has a
 * dependency on the LogicalTable resource. The LogicalTable resource must be generated before the dimension generator
 * runs.
 *
 * To support this, this builder uses {@code isResourceInitialized()} and {@code getResourceIfInitialized()} semantics.
 * {@code isResourceInitialized()} should ALWAYS be called before attempting to access a resource using
 * {@code getResourceIfInitialized()} to ensure that the generator for that resource has been called.
 *
 * An {@link UninitializedRequestResourceException} is thrown if {@code getResourceIfInitialized()} is called before the
 * resource has been set using the appropriate setter.
 *
 * Resources that are not grouped into collections are returned as {@link Optional}. This is because the resource may
 * not have been specified in the query. Specific generator implementations may throw an error if a resource is empty,
 * but this is an implementation detail and the {@link DataApiRequest} construction API does not enforce this.
 *
 * Whether or not the resource has been initialized it completely separate from the Optional contract on the get
 * methods. {@code isResourceInitialized} and the {@code Optional} return value are separate contracts.
 */
public class DataApiRequestBuilder {

    /**
     * Enum representing the phases of building a data api request. A setter for each of these phases MUST be called
     * at least once before the build method is called.
     */
    public enum RequestResource {
        LOGICAL_TABLE,
        GRANULARITY,
        DIMENSIONS,
        DIMENSION_FIELDS,
        LOGICAL_METRICS,
        INTERVALS,
        API_FILTERS,
        HAVINGS,
        SORTS,
        COUNT,
        TOP_N,
        FORMAT,
        DOWNLOAD_FILENAME,
        TIMEZONE,
        ASYNC_AFTER,
        PAGINATION_PARAMETERS;

        private String resourceName;

        /**
         * Constructor.
         */
        RequestResource() {
            this.resourceName = EnumUtils.camelCase(this.name());
        }

        /**
         * Gets the name of the resource this enum represents.
         *
         * @return the resource name
         */
        public String getResourceName() {
            return resourceName;
        }
    }

    private static final Map<RequestResource, Boolean> INITIALIZED_BUILT_MAPPING;

    static {
        EnumMap<RequestResource, Boolean> phaseMap = new EnumMap<>(RequestResource.class);
        for (RequestResource phase : RequestResource.values()) {
            phaseMap.put(phase, Boolean.FALSE);
        }
        INITIALIZED_BUILT_MAPPING = Collections.unmodifiableMap(phaseMap);
    }

    private LogicalTable logicalTable;
    private Granularity granularity;
    private LinkedHashSet<Dimension> dimensions;
    private LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> perDimensionFields;
    private LinkedHashSet<LogicalMetric> metrics;
    private List<Interval> intervals;
    private ApiFilters apiFilters;
    private LinkedHashMap<LogicalMetric, Set<ApiHaving>> havings;
    private LinkedHashSet<OrderByColumn> sorts;
    private Integer count;
    private Integer topN;
    private ResponseFormatType format;
    private String downloadFilename;
    private DateTimeZone timeZone;
    private Long asyncAfter;
    private PaginationParameters paginationParameters;

    private final BardConfigResources resources;
    private final Map<RequestResource, Boolean> built;

    /**
     * Constructor.
     *
     * @param resources set of config resources generators can use while building pieces of the
     * {@link DataApiRequestValueObject}
     */
    public DataApiRequestBuilder(BardConfigResources resources) {
        this.resources = resources;
        this.built = new EnumMap<>(INITIALIZED_BUILT_MAPPING);
    }

    /**
     * Utility method that performs binding and validating for generic generator.
     *
     * @param params  Raw parameters from data request
     * @param generator  Generator object that can convert a raw request parameter into the relevant model object
     * @param <T>  Type of model object to generate
     * @return the generated model object
     */
    protected <T> T bindAndValidate(RequestParameters params, Generator<T> generator) {
        T bound = generator.bind(this, params, resources);
        generator.validate(bound, this, params, resources);
        return bound;
    }

    // SETTERS

    /**
     * Generates and sets the requested {@link LogicalTable}.
     *
     * @param params  Raw request params
     * @param generator  Generator for LogicalTable
     * @return the builder
     */
    public DataApiRequestBuilder setLogicalTable(RequestParameters params, Generator<LogicalTable> generator) {
        built.put(RequestResource.LOGICAL_TABLE, Boolean.TRUE);
        this.logicalTable = bindAndValidate(params, generator);
        return this;
    }

    /**
     * Generates and sets the requested {@link Granularity}.
     *
     * @param params  Raw request params
     * @param generator  Generator for Granularity
     * @return the builder
     */
    public DataApiRequestBuilder setGranularity(RequestParameters params, Generator<Granularity> generator) {
        built.put(RequestResource.GRANULARITY, Boolean.TRUE);
        this.granularity = bindAndValidate(params, generator);
        return this;
    }

    /**
     * Generates and sets the requested {@link Dimension}s.
     *
     * @param params  Raw request params
     * @param generator  Generator for Dimensions
     * @return the builder
     */
    public DataApiRequestBuilder setDimensions(
            RequestParameters params,
            Generator<LinkedHashSet<Dimension>> generator
    ) {
        built.put(RequestResource.DIMENSIONS, Boolean.TRUE);
        this.dimensions = bindAndValidate(params, generator);
        return this;
    }


    /**
     * Generates and sets the mappings from the requested {@link Dimension}s to their requested {@link DimensionField}.
     *
     * @param params  Raw request params
     * @param generator  Generator for mapping of dimension to request dimension fields
     * @return the builder
     */
    public DataApiRequestBuilder setDimensionFields(
            RequestParameters params,
            Generator<LinkedHashMap<Dimension, LinkedHashSet<DimensionField>>> generator
    ) {
        built.put(RequestResource.DIMENSION_FIELDS, Boolean.TRUE);
        this.perDimensionFields = bindAndValidate(params, generator);
        return this;
    }

    /**
     * Generates and sets the requested {@link LogicalMetric}.
     *
     * @param params  Raw request params
     * @param generator  Generator for logical metrics
     * @return the builder
     */
    public DataApiRequestBuilder setMetrics(
            RequestParameters params,
            Generator<LinkedHashSet<LogicalMetric>> generator
    ) {
        built.put(RequestResource.LOGICAL_METRICS, Boolean.TRUE);
        this.metrics = bindAndValidate(params, generator);
        return this;
    }

    /**
     * Generates and sets the requested {@link Interval}s.
     *
     * @param params  Raw request params
     * @param generator  Generator for list of Intervals
     * @return the builder
     */
    public DataApiRequestBuilder setIntervals(RequestParameters params, Generator<List<Interval>> generator) {
        built.put(RequestResource.INTERVALS, Boolean.TRUE);
        this.intervals = bindAndValidate(params, generator);
        return this;
    }

    /**
     * Generates and sets the requested {@link ApiFilters}.
     *
     * @param params  Raw request params
     * @param generator  Generator for ApiFilters
     * @return the builder
     */
    public DataApiRequestBuilder setApiFilters(RequestParameters params, Generator<ApiFilters> generator) {
        built.put(RequestResource.API_FILTERS, Boolean.TRUE);
        this.apiFilters = bindAndValidate(params, generator);
        return this;
    }

    /**
     * Generates and sets the requested mapping of {@link LogicalMetric} to a set of {@link ApiHaving}s filtering that
     * metric.
     *
     * @param params  Raw request params
     * @param generator  Generator for mapping og logical metric to api havings
     * @return the builder
     */
    public DataApiRequestBuilder setHavings(
            RequestParameters params,
            Generator<LinkedHashMap<LogicalMetric, Set<ApiHaving>>> generator
    ) {
        built.put(RequestResource.HAVINGS, Boolean.TRUE);
        this.havings = bindAndValidate(params, generator);
        return this;
    }

    /**
     * Generates and sets the requested {@link OrderByColumn}s.
     *
     * @param params  Raw request params
     * @param generator  Generator for set of sorts
     * @return the builder
     */
    public DataApiRequestBuilder setSorts(
            RequestParameters params,
            Generator<LinkedHashSet<OrderByColumn>> generator
    ) {
        built.put(RequestResource.SORTS, Boolean.TRUE);
        this.sorts = bindAndValidate(params, generator);
        return this;
    }

    /**
     * Generates and sets the requested count.
     *
     * @param params  Raw request params
     * @param generator  Generator for count
     * @return the builder
     */
    public DataApiRequestBuilder setCount(RequestParameters params, Generator<Integer> generator) {
        built.put(RequestResource.COUNT, Boolean.TRUE);
        this.count = bindAndValidate(params, generator);
        return this;
    }

    /**
     * Generates and sets the requested topN.
     *
     * @param params  Raw request params
     * @param generator  Generator for topN
     * @return the builder
     */
    public DataApiRequestBuilder setTopN(RequestParameters params, Generator<Integer> generator) {
        built.put(RequestResource.TOP_N, Boolean.TRUE);
        this.topN = bindAndValidate(params, generator);
        return this;
    }

    /**
     * Generates and sets the requested {@link ResponseFormatType}.
     *
     * @param params  Raw request params
     * @param generator  Generator for ResponseFormatType
     * @return the builder
     */
    public DataApiRequestBuilder setFormat(RequestParameters params, Generator<ResponseFormatType> generator) {
        built.put(RequestResource.FORMAT, Boolean.TRUE);
        this.format = bindAndValidate(params, generator);
        return this;
    }

    /**
     * Generates and sets the requested download filename.
     *
     * @param params  Raw request params
     * @param generator  Generator for the download filename
     * @return the builder
     */
    public DataApiRequestBuilder setDownloadFilename(RequestParameters params, Generator<String> generator) {
        built.put(RequestResource.DOWNLOAD_FILENAME, Boolean.TRUE);
        this.downloadFilename = bindAndValidate(params, generator);
        return this;
    }

    /**
     * Generates and sets the requested {@link DateTimeZone}.
     *
     * @param params  Raw request params
     * @param generator  Generator for DateTimeZone
     * @return the builder
     */
    public DataApiRequestBuilder setTimeZone(RequestParameters params, Generator<DateTimeZone> generator) {
        built.put(RequestResource.TIMEZONE, Boolean.TRUE);
        this.timeZone = bindAndValidate(params, generator);
        return this;
    }

    /**
     * Generates and sets the requested async after timeout.
     *
     * @param params  Raw request params
     * @param generator  Generator for async after timeout
     * @return the builder
     */
    public DataApiRequestBuilder setAsyncAfter(RequestParameters params, Generator<Long> generator) {
        built.put(RequestResource.ASYNC_AFTER, Boolean.TRUE);
        this.asyncAfter = bindAndValidate(params, generator);
        return this;
    }

    /**
     * Generates and sets the requested {@link PaginationParameters}.
     *
     * @param params  Raw request params
     * @param generator  Generator for PaginationParameters
     * @return the builder
     */
    public DataApiRequestBuilder setPaginationParameters(
            RequestParameters params,
            Generator<PaginationParameters> generator
    ) {
        built.put(RequestResource.PAGINATION_PARAMETERS, Boolean.TRUE);
        this.paginationParameters = bindAndValidate(params, generator);
        return this;
    }

    // GETTERS

    /**
     * Returns if the logical table has been initialized or not.
     *
     * @return if the logical table has been initialized or not
     */
    public boolean isLogicalTableInitialized() {
        return built.get(RequestResource.LOGICAL_TABLE);
    }

    /**
     * Getter for logical table.
     *
     * @return the logical table
     */
    public Optional<LogicalTable> getLogicalTableIfInitialized() {
        if (!isLogicalTableInitialized()) {
            throw new UninitializedRequestResourceException(RequestResource.LOGICAL_TABLE);
        }
        return Optional.ofNullable(logicalTable);
    }

    /**
     * Returns if the granularity has been initialized or not.
     *
     * @return if the granularity has been initialized or not
     */
    public boolean isGranularityInitialized() {
        return built.get(RequestResource.GRANULARITY);
    }

    /**
     * Getter for granularity.
     *
     * @return the granularity
     */
    public Optional<Granularity> getGranularityIfInitialized() {
        if (!isGranularityInitialized()) {
            throw new UninitializedRequestResourceException(RequestResource.GRANULARITY);
        }
        return Optional.ofNullable(granularity);
    }

    /**
     * Returns if the dimensions have been initialized.
     *
     * @return if the dimensions have been initialized
     */
    public boolean isDimensionsInitialized() {
        return built.get(RequestResource.DIMENSIONS);
    }

    /**
     * Getter for dimension.
     *
     * @return the dimensions
     */
    public LinkedHashSet<Dimension> getDimensionsIfInitialized() {
        if (!isDimensionsInitialized()) {
            throw new UninitializedRequestResourceException(RequestResource.DIMENSIONS);
        }
        return dimensions;
    }

    /**
     * Returns if the dimension fields have been initialized.
     *
     * @return if the dimension fields have been initialized
     */
    public boolean isPerDimensionFieldsInitialized() {
        return built.get(RequestResource.DIMENSION_FIELDS);
    }

    /**
     * Getter for mapping between grouping dimensions and their requested fields.
     *
     * @return the mapping between grouping and their requested fields
     */
    public LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> getPerDimensionFieldsIfInitialized() {
        if (!isPerDimensionFieldsInitialized()) {
            throw new UninitializedRequestResourceException(RequestResource.DIMENSION_FIELDS);
        }
        return perDimensionFields;
    }

    /**
     * Returns if the logical metrics have been initialized.
     *
     * @return if the logical metrics have been initialized
     */
    public boolean isLogicalMetricsInitialized() {
        return built.get(RequestResource.LOGICAL_METRICS);
    }

    /**
     * Getter for logical metrics.
     *
     * @return the logical metrics
     */
    public LinkedHashSet<LogicalMetric> getLogicalMetricsIfInitialized() {
        if (!isLogicalMetricsInitialized()) {
            throw new UninitializedRequestResourceException(RequestResource.LOGICAL_METRICS);
        }
        return metrics;
    }

    /**
     * Returns if the intervals have been initialized.
     *
     * @return if the intervals have been initialized
     */
    public boolean isIntervalsInitialized() {
        return built.get(RequestResource.INTERVALS);
    }

    /**
     * Getter for the request intervals.
     *
     * @return the request intervals
     */
    public List<Interval> getIntervalsIfInitialized() {
        if (!isIntervalsInitialized()) {
            throw new UninitializedRequestResourceException(RequestResource.INTERVALS);
        }
        return intervals;
    }

    /**
     * Returns if the ApiFilters are initialized.
     *
     * @return if the ApiFilters are initialized
     */
    public boolean isApiFiltersInitialized() {
        return built.get(RequestResource.API_FILTERS);
    }

    /**
     * Getter for the ApiFilters.
     *
     * @return the ApiFilters
     */
    public ApiFilters getApiFiltersIfInitialized() {
        if (!isApiFiltersInitialized()) {
            throw new UninitializedRequestResourceException(RequestResource.API_FILTERS);
        }
        return apiFilters;
    }

    /**
     * Returns if the havings are initialized.
     *
     * @return if the havings are initialized
     */
    public boolean isHavingsInitialized() {
        return built.get(RequestResource.HAVINGS);
    }

    /**
     * Getter for the havings.
     *
     * @return the havings
     */
    public LinkedHashMap<LogicalMetric, Set<ApiHaving>> getHavingsIfInitialized() {
        if (!isHavingsInitialized()) {
            throw new UninitializedRequestResourceException(RequestResource.HAVINGS);
        }
        return havings;
    }

    /**
     * Returns if the sorts are initialized.
     *
     * @return if the sorts are initialized
     */
    public boolean isSortsInitialized() {
        return built.get(RequestResource.SORTS);
    }

    /**
     * Getter for the sorts.
     *
     * @return the sorts
     */
    public LinkedHashSet<OrderByColumn> getSortsIfInitialized() {
        if (!isSortsInitialized()) {
            throw new UninitializedRequestResourceException(RequestResource.SORTS);
        }
        return sorts;
    }

    /**
     * Returns if the count is initialized.
     *
     * @return if the count is initialized
     */
    public boolean isCountInitialized() {
        return built.get(RequestResource.COUNT);
    }

    /**
     * Getter for the count.
     *
     * @return the count
     */
    public Optional<Integer> getCountIfInitialized() {
        if (!isCountInitialized()) {
            throw new UninitializedRequestResourceException(RequestResource.COUNT);
        }
        return Optional.ofNullable(count);
    }

    /**
     * Returns if top n is initialized.
     *
     * @return if top n is initialized
     */
    public boolean isTopNInitialized() {
        return built.get(RequestResource.TOP_N);
    }

    /**
     * Getter for the topN.
     *
     * @return the topN
     */
    public Optional<Integer> getTopNIfInitialized() {
        if (!isTopNInitialized()) {
            throw new UninitializedRequestResourceException(RequestResource.TOP_N);
        }
        return Optional.ofNullable(topN);
    }

    /**
     * Returns if format was initialized.
     *
     * @return if format was initialized
     */
    public boolean isFormatInitialized() {
        return built.get(RequestResource.FORMAT);
    }

    /**
     * Getter for the response format.
     *
     * @return the response format
     */
    public Optional<ResponseFormatType> getFormatIfInitialized() {
        if (!isFormatInitialized()) {
            throw new UninitializedRequestResourceException(RequestResource.FORMAT);
        }
        return Optional.ofNullable(format);
    }

    /**
     * Returns if the download filename is initialized.
     *
     * @return if the download filename has been initialized
     */
    public boolean isDownloadFilenameInitialized() {
        return built.get(RequestResource.DOWNLOAD_FILENAME);
    }

    /**
     * Getter for the download filename.
     *
     * @return the download filename
     */
    public Optional<String> getDownloadFilenameIfInitialized() {
        if (!isDownloadFilenameInitialized()) {
            throw new UninitializedRequestResourceException(RequestResource.DOWNLOAD_FILENAME);
        }
        return Optional.ofNullable(downloadFilename);
    }

    /**
     * Returns if timezone is initialized.
     *
     * @return if timezone is initialized
     */
    public boolean isTimeZoneInitialized() {
        return built.get(RequestResource.TIMEZONE);
    }

    /**
     * Getter for the timezone.
     *
     * @return the timezone
     */
    public Optional<DateTimeZone> getTimeZoneIfInitialized() {
        if (!isTimeZoneInitialized()) {
            throw new UninitializedRequestResourceException(RequestResource.TIMEZONE);
        }
        return Optional.ofNullable(timeZone);
    }

    /**
     * Returns if async after is initialized.
     *
     * @return if async after is initialized
     */
    public boolean isAsyncAfterInitialized() {
        return built.get(RequestResource.ASYNC_AFTER);
    }

    /**
     * Getter for the async after.
     *
     * @return the async after
     */
    public Optional<Long> getAsyncAfterIfInitialized() {
        if (!isAsyncAfterInitialized()) {
            throw new UninitializedRequestResourceException(RequestResource.ASYNC_AFTER);
        }
        return Optional.ofNullable(asyncAfter);
    }

    /**
     * Returns if pagination parameters are initialized.
     *
     * @return if pagination parameters are initialized
     */
    public boolean isPaginationParametersInitialized() {
        return built.get(RequestResource.PAGINATION_PARAMETERS);
    }

    /**
     * Getter for the pagination parameters.
     *
     * @return the pagination parameters
     */
    public Optional<PaginationParameters> getPaginationParametersIfInitialized() {
        if (!isPaginationParametersInitialized()) {
            throw new UninitializedRequestResourceException(RequestResource.PAGINATION_PARAMETERS);
        }
        return Optional.ofNullable(paginationParameters);
    }

    /**
     * Build method. Generates a {@link DataApiRequest} using the values set on all of the setters. if
     * {@link BardFeatureFlag#POJO_DARI_REQUIRE_ALL_STAGES_CALLED} is on, ALL SETTERS MUST BE CALLED BEFORE THE
     * BUILD METHOD CAN BE CALLED. The generators may produce null, but the set method must still be called. This
     * ensures that there is no piece of the query that should be getting built that is not because the data request
     * model is being built inaccurately.
     *
     * @return the constructed DataApiRequest object
     */
    public DataApiRequest build() {

        // validate that ALL build phases have been called
        if (BardFeatureFlag.POJO_DARI_REQUIRE_ALL_STAGES_CALLED.isOn()) {
            Set<RequestResource> uninitializedLifecycles = built.entrySet().stream()
                    .filter(entry -> !entry.getValue())
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());
            if (!uninitializedLifecycles.isEmpty()) {
                String msg = String.format(
                        "Attempted to build DataApiRequest without attempting to build %s",
                        uninitializedLifecycles.stream()
                                .map(phase -> phase.name().toLowerCase(Locale.ENGLISH))
                                .collect(Collectors.joining(", "))
                );
                throw new IllegalStateException(msg);
            }
        }

        return new DataApiRequestValueObject(
                logicalTable,
                granularity,
                dimensions,
                perDimensionFields,
                metrics,
                intervals,
                apiFilters,
                havings,
                sorts,
                count,
                topN,
                format,
                downloadFilename,
                timeZone,
                asyncAfter,
                paginationParameters
        );
    }

    /**
     * Exception indicating that the requested resource has not been initialized. Resources may be initialized to an
     * empty optional or an empty collection, but the request parameters for a resource MUST be parsed and an initial
     * value set in the builder before they can be accessed. Accessing a resource whose request parameters have not yet
     * been parsed is always an error case.
     */
    public static class UninitializedRequestResourceException extends RuntimeException {

        private static final String UNINITIALIZED_REQUEST_RESOURCE_MESSAGE = "Resource %s was requested but has not " +
                "been initialized. Ensure the generator for resource %s has been used to generate the resource AND " +
                "the resource has been added to the builder.";

        private final RequestResource resource;

        /**
         * Constructor.
         *
         * @param resource  The name of the resource that was requested.
         */
        public UninitializedRequestResourceException(RequestResource resource) {
            super(generateExceptionMessage(resource));
            this.resource = resource;
        }

        /**
         * Constructor.
         *
         * @param resource  The name of the resource that was requested.
         * @param throwable  An underlying exception that caused this exception to be thrown.
         */
        public UninitializedRequestResourceException(RequestResource resource, Throwable throwable) {
            super(generateExceptionMessage(resource), throwable);
            this.resource = resource;
        }

        /**
         * Generates the exception message using the resource name.
         *
         * @param resource  The name of the resource that was requested.
         * @return the formatted error message.
         */
        private static String generateExceptionMessage(RequestResource resource) {
            return String.format(
                    UNINITIALIZED_REQUEST_RESOURCE_MESSAGE,
                    resource.getResourceName(),
                    resource.getResourceName()
            );
        }

        /**
         * Getter.
         *
         * @return the name of the resource that was requested.
         */
        public RequestResource getResource() {
            return resource;
        }
    }
}
