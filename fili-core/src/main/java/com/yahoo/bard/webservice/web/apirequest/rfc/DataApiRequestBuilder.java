// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.rfc;

import com.yahoo.bard.webservice.config.BardFeatureFlag;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.web.ApiHaving;
import com.yahoo.bard.webservice.web.ResponseFormatType;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest;
import com.yahoo.bard.webservice.web.filters.ApiFilters;
import com.yahoo.bard.webservice.web.util.BardConfigResources;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Builder for {@link DataApiRequestImpl} objects.
 */
// TODO this class needs getters on all fields
// TODO look into making the type signatures extendable by subclasses
public class DataApiRequestBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(DataApiRequestBuilder.class);

    /**
     * Enum representing the phases of building a data api request. A a setter for each of these phases MUST be called
     * at least once before the build method is called.
     */
    private enum BuildPhase {
        LOGICAL_TABLE,
        GRANULARITY,
        DIMENSIONS,
        DIMENSION_FIELDS,
        METRICS,
        INTERVALS,
        API_FILTERS,
        HAVINGS,
        SORTS,
        COUNT,
        TOP_N,
        FORMAT,
        FILENAME,
        TIMEZONE,
        ASYNC_AFTER,
        PAGINATION
    }

    private static final Map<BuildPhase, Boolean> INITIALIZED_BUILT_MAPPING;


    static {
        EnumMap<BuildPhase, Boolean> phaseMap = new EnumMap<>(BuildPhase.class);
        for (BuildPhase phase : BuildPhase.values()) {
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
    private int count;
    private int topN;
    private ResponseFormatType format;
    private String downloadFilename;
    private DateTimeZone timeZone;
    private long asyncAfter;
    private PaginationParameters paginationParameters;

    private final BardConfigResources resources;
    private final Map<BuildPhase, Boolean> built;

    /**
     * Constructor.
     *
     * @param resources set of resources generators can use while building pieces of the {@link DataApiRequestImpl}
     */
    public DataApiRequestBuilder(BardConfigResources resources) {
        this.resources = resources;
        built = new EnumMap<>(INITIALIZED_BUILT_MAPPING);
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

    /**
     * Generates and sets the requested {@link LogicalTable}.
     *
     * @param params  Raw request params
     * @param generator  Generator for LogicalTable
     * @return the builder
     */
    public DataApiRequestBuilder logicalTable(RequestParameters params, Generator<LogicalTable> generator) {
        built.put(BuildPhase.LOGICAL_TABLE, Boolean.TRUE);
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
    public DataApiRequestBuilder granularity(RequestParameters params, Generator<Granularity> generator) {
        built.put(BuildPhase.GRANULARITY, Boolean.TRUE);
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
    public DataApiRequestBuilder dimensions(RequestParameters params, Generator<LinkedHashSet<Dimension>> generator) {
        built.put(BuildPhase.DIMENSIONS, Boolean.TRUE);
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
    public DataApiRequestBuilder dimensionFields(
            RequestParameters params,
            Generator<LinkedHashMap<Dimension, LinkedHashSet<DimensionField>>> generator
    ) {
        built.put(BuildPhase.DIMENSION_FIELDS, Boolean.TRUE);
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
    public DataApiRequestBuilder metrics(RequestParameters params, Generator<LinkedHashSet<LogicalMetric>> generator) {
        built.put(BuildPhase.METRICS, Boolean.TRUE);
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
    public DataApiRequestBuilder intervals(RequestParameters params, Generator<List<Interval>> generator) {
        built.put(BuildPhase.INTERVALS, Boolean.TRUE);
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
    public DataApiRequestBuilder apiFilters(RequestParameters params, Generator<ApiFilters> generator) {
        built.put(BuildPhase.API_FILTERS, Boolean.TRUE);
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
    public DataApiRequestBuilder havings(
            RequestParameters params,
            Generator<LinkedHashMap<LogicalMetric, Set<ApiHaving>>> generator
    ) {
        built.put(BuildPhase.HAVINGS, Boolean.TRUE);
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
    public DataApiRequestBuilder sorts(
            RequestParameters params,
            Generator<LinkedHashSet<OrderByColumn>> generator
    ) {
        built.put(BuildPhase.SORTS, Boolean.TRUE);
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
    public DataApiRequestBuilder count(RequestParameters params, Generator<Integer> generator) {
        built.put(BuildPhase.COUNT, Boolean.TRUE);
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
    public DataApiRequestBuilder topN(RequestParameters params, Generator<Integer> generator) {
        built.put(BuildPhase.TOP_N, Boolean.TRUE);
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
    public DataApiRequestBuilder format(RequestParameters params, Generator<ResponseFormatType> generator) {
        built.put(BuildPhase.FORMAT, Boolean.TRUE);
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
    public DataApiRequestBuilder downloadFilename(RequestParameters params, Generator<String> generator) {
        built.put(BuildPhase.FILENAME, Boolean.TRUE);
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
    public DataApiRequestBuilder timeZone(RequestParameters params, Generator<DateTimeZone> generator) {
        built.put(BuildPhase.TIMEZONE, Boolean.TRUE);
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
    public DataApiRequestBuilder asyncAfter(RequestParameters params, Generator<Long> generator) {
        built.put(BuildPhase.ASYNC_AFTER, Boolean.TRUE);
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
    public DataApiRequestBuilder paginationParameters(
            RequestParameters params,
            Generator<PaginationParameters> generator
    ) {
        built.put(BuildPhase.PAGINATION, Boolean.TRUE);
        this.paginationParameters = bindAndValidate(params, generator);
        return this;
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
            Set<BuildPhase> uninitializedLifecycles = built.entrySet().stream()
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

        return new com.yahoo.bard.webservice.web.apirequest.rfc.DataApiRequestImpl(
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
}
