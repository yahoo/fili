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

// TODO look into making the type signatures extendable by subclasses
public class DataApiRequestBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(DataApiRequestBuilder.class);

    // TODO wrap this in a feature flag.
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

    public DataApiRequestBuilder(BardConfigResources resources) {
        this.resources = resources;
        built = new EnumMap<>(INITIALIZED_BUILT_MAPPING);
    }

    <T> T bindAndValidate(RequestParameters params, Generator<T> generator) {
        T bound = generator.bind(this, params, resources);
        generator.validate(bound, this, params, resources);
        return bound;
    }

    public DataApiRequestBuilder logicalTable(RequestParameters params, Generator<LogicalTable> generator) {
        built.put(BuildPhase.LOGICAL_TABLE, Boolean.TRUE);
        this.logicalTable = bindAndValidate(params, generator);
        return this;
    }

    public DataApiRequestBuilder granularity(RequestParameters params, Generator<Granularity> generator) {
        built.put(BuildPhase.GRANULARITY, Boolean.TRUE);
        this.granularity = bindAndValidate(params, generator);
        return this;
    }

    public DataApiRequestBuilder dimensions(RequestParameters params, Generator<LinkedHashSet<Dimension>> generator) {
        built.put(BuildPhase.DIMENSIONS, Boolean.TRUE);
        this.dimensions = bindAndValidate(params, generator);
        return this;
    }

    public DataApiRequestBuilder dimensionFields(
            RequestParameters params,
            Generator<LinkedHashMap<Dimension, LinkedHashSet<DimensionField>>> generator
    ) {
        built.put(BuildPhase.DIMENSION_FIELDS, Boolean.TRUE);
        this.perDimensionFields = bindAndValidate(params, generator);
        return this;
    }

    public DataApiRequestBuilder metrics(RequestParameters params, Generator<LinkedHashSet<LogicalMetric>> generator) {
        built.put(BuildPhase.METRICS, Boolean.TRUE);
        this.metrics = bindAndValidate(params, generator);
        return this;
    }

    public DataApiRequestBuilder intervals(RequestParameters params, Generator<List<Interval>> generator) {
        built.put(BuildPhase.INTERVALS, Boolean.TRUE);
        this.intervals = bindAndValidate(params, generator);
        return this;
    }

    public DataApiRequestBuilder apiFilters(RequestParameters params, Generator<ApiFilters> generator) {
        built.put(BuildPhase.API_FILTERS, Boolean.TRUE);
        this.apiFilters = bindAndValidate(params, generator);
        return this;
    }

    public DataApiRequestBuilder havings(
            RequestParameters params,
            Generator<LinkedHashMap<LogicalMetric, Set<ApiHaving>>> generator
    ) {
        built.put(BuildPhase.HAVINGS, Boolean.TRUE);
        this.havings = bindAndValidate(params, generator);
        return this;
    }

    public DataApiRequestBuilder sorts(
            RequestParameters params,
            Generator<LinkedHashSet<OrderByColumn>> generator
    ) {
        built.put(BuildPhase.SORTS, Boolean.TRUE);
        this.sorts = bindAndValidate(params, generator);
        return this;
    }

    public DataApiRequestBuilder count(RequestParameters params, Generator<Integer> generator) {
        built.put(BuildPhase.COUNT, Boolean.TRUE);
        this.count = bindAndValidate(params, generator);
        return this;
    }

    public DataApiRequestBuilder topN(RequestParameters params, Generator<Integer> generator) {
        built.put(BuildPhase.TOP_N, Boolean.TRUE);
        this.topN = bindAndValidate(params, generator);
        return this;
    }

    public DataApiRequestBuilder format(RequestParameters params, Generator<ResponseFormatType> generator) {
        built.put(BuildPhase.FORMAT, Boolean.TRUE);
        this.format = bindAndValidate(params, generator);
        return this;
    }

    public DataApiRequestBuilder downloadFilename(RequestParameters params, Generator<String> generator) {
        built.put(BuildPhase.FILENAME, Boolean.TRUE);
        this.downloadFilename = bindAndValidate(params, generator);
        return this;
    }

    public DataApiRequestBuilder timeZone(RequestParameters params, Generator<DateTimeZone> generator) {
        built.put(BuildPhase.TIMEZONE, Boolean.TRUE);
        this.timeZone = bindAndValidate(params, generator);
        return this;
    }

    public DataApiRequestBuilder asyncAfter(RequestParameters params, Generator<Long> generator) {
        built.put(BuildPhase.ASYNC_AFTER, Boolean.TRUE);
        this.asyncAfter = bindAndValidate(params, generator);
        return this;
    }

    public DataApiRequestBuilder paginationParameters(RequestParameters params, Generator<PaginationParameters> generator) {
        built.put(BuildPhase.PAGINATION, Boolean.TRUE);
        this.paginationParameters = bindAndValidate(params, generator);
        return this;
    }

    public DataApiRequest build() {

        // validate that ALL build phases have been called
        if (BardFeatureFlag.POJO_DARI_REQUIRE_ALL_STAGES_CALLED.isOn()) {
            Set<BuildPhase> uninitializedLifecycles = built.entrySet().stream()
                    .filter(Map.Entry::getValue)
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
