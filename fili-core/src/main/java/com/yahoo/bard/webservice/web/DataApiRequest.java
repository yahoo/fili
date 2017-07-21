// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.DATE_TIME_SORT_VALUE_INVALID;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.DIMENSION_FIELDS_UNDEFINED;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.HAVING_METRICS_NOT_IN_QUERY_FORMAT;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INCORRECT_METRIC_FILTER_FORMAT;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INTEGER_INVALID;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INVALID_METRIC_FILTER_CONDITION;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.METRICS_MISSING;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.METRICS_UNDEFINED;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.NON_AGGREGATABLE_INVALID;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.SORT_DIRECTION_INVALID;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.SORT_METRICS_NOT_IN_QUERY_FORMAT;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.SORT_METRICS_NOT_SORTABLE_FORMAT;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.SORT_METRICS_UNDEFINED;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TABLE_UNDEFINED;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TOP_N_UNSORTED;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.UNSUPPORTED_FILTERED_METRIC_CATEGORY;

import com.yahoo.bard.webservice.config.BardFeatureFlag;
import com.yahoo.bard.webservice.data.DruidHavingBuilder;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.DimensionRowNotFoundException;
import com.yahoo.bard.webservice.data.filterbuilders.DefaultDruidFilterBuilder;
import com.yahoo.bard.webservice.data.filterbuilders.DruidFilterBuilder;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.time.GranularityParser;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.druid.model.having.Having;
import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn;
import com.yahoo.bard.webservice.druid.model.orderby.SortDirection;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.druid.util.FieldConverterSupplier;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.TimedPhase;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.table.TableIdentifier;
import com.yahoo.bard.webservice.util.StreamUtils;
import com.yahoo.bard.webservice.web.util.BardConfigResources;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

/**
 * Data API Request. Such an API Request binds, validates, and models the parts of a request to the data endpoint.
 */
public class DataApiRequest extends ApiRequest {
    private static final Logger LOG = LoggerFactory.getLogger(DataApiRequest.class);
    private static final String RATIO_METRIC_CATEGORY = "Ratios";
    public static final String REQUEST_MAPPER_NAMESPACE = "dataApiRequestMapper";
    private static final String DATE_TIME_STRING = "dateTime";

    private final LogicalTable table;

    private final Granularity granularity;

    private final Set<Dimension> dimensions;
    private final LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> perDimensionFields;
    private final Set<LogicalMetric> logicalMetrics;
    private final Set<Interval> intervals;
    private final Map<Dimension, Set<ApiFilter>> filters;
    private final Map<LogicalMetric, Set<ApiHaving>> havings;
    private final Having having;
    private final LinkedHashSet<OrderByColumn> sorts;
    private final int count;
    private final int topN;

    private final DateTimeZone timeZone;

    private final DruidFilterBuilder filterBuilder;

    private final Optional<OrderByColumn> dateTimeSort;

    /**
     * Parses the API request URL and generates the Api Request object.
     *
     * @param tableName  logical table corresponding to the table name specified in the URL
     * @param granularity  string time granularity in URL
     * @param dimensions  single dimension or multiple dimensions separated by '/' in URL
     * @param logicalMetrics  URL logical metric query string in the format:
     * <pre>{@code single metric or multiple logical metrics separated by ',' }</pre>
     * @param intervals  URL intervals query string in the format:
     * <pre>{@code single interval in ISO 8601 format, multiple values separated by ',' }</pre>
     * @param filters  URL filter query String in the format:
     * <pre>{@code
     * ((field name and operation):((multiple values bounded by [])or(single value))))(followed by , or end of string)
     * }</pre>
     * @param havings  URL having query String in the format:<pre>
     * {@code
     * ((metric name)-(operation)((values bounded by [])))(followed by , or end of string)
     * }</pre>
     * @param sorts  string of sort columns along with sort direction in the format:<pre>
     * {@code (metricName or dimensionName)|(sortDirection) eg: pageViews|asc }</pre>
     * @param count  count of number of records to be returned in the response
     * @param topN  number of first records per time bucket to be returned in the response
     * @param format  response data format JSON or CSV. Default is JSON.
     * @param timeZoneId  a joda time zone id
     * @param asyncAfter  How long the user is willing to wait for a synchronous request in milliseconds
     * @param perPage  number of rows to display per page of results. If present in the original request,
     * must be a positive integer. If not present, must be the empty string.
     * @param page  desired page of results. If present in the original request, must be a positive
     * integer. If not present, must be the empty string.
     * @param uriInfo  The URI of the request object.
     * @param bardConfigResources  The configuration resources used to build this api request
     *
     * @throws BadApiRequestException in the following scenarios:
     * <ol>
     *     <li>Null or empty table name in the API request.</li>
     *     <li>Invalid time grain in the API request.</li>
     *     <li>Invalid dimension in the API request.</li>
     *     <li>Invalid logical metric in the API request.</li>
     *     <li>Invalid interval in the API request.</li>
     *     <li>Invalid filter syntax in the API request.</li>
     *     <li>Invalid filter dimensions in the API request.</li>
     *     <li>Invalid having syntax in the API request.</li>
     *     <li>Invalid having metrics in the API request.</li>
     *     <li>Pagination parameters in the API request that are not positive integers.</li>
     * </ol>
     */
    public DataApiRequest(
            String tableName,
            String granularity,
            List<PathSegment> dimensions,
            String logicalMetrics,
            String intervals,
            String filters,
            String havings,
            String sorts,
            String count,
            String topN,
            String format,
            String timeZoneId,
            String asyncAfter,
            @NotNull String perPage,
            @NotNull String page,
            UriInfo uriInfo,
            BardConfigResources bardConfigResources
    ) throws BadApiRequestException {
        super(format, asyncAfter, perPage, page, uriInfo);

        GranularityParser granularityParser = bardConfigResources.getGranularityParser();
        DimensionDictionary dimensionDictionary = bardConfigResources.getDimensionDictionary();

        timeZone = generateTimeZone(timeZoneId, bardConfigResources.getSystemTimeZone());

        // Time grain must be from allowed interval keywords
        this.granularity = generateGranularity(granularity, timeZone, granularityParser);

        TableIdentifier tableId = new TableIdentifier(tableName, this.granularity);

        // Logical table must be in the logical table dictionary
        this.table = bardConfigResources.getLogicalTableDictionary().get(tableId);
        if (this.table == null) {
            LOG.debug(TABLE_UNDEFINED.logFormat(tableName));
            throw new BadApiRequestException(TABLE_UNDEFINED.format(tableName));
        }

        DateTimeFormatter dateTimeFormatter = generateDateTimeFormatter(timeZone);

        this.intervals = generateIntervals(intervals, this.granularity, dateTimeFormatter);

        this.filterBuilder = bardConfigResources.getFilterBuilder();

        MetricDictionary metricDictionary = bardConfigResources
                .getMetricDictionary()
                .getScope(Collections.singletonList(tableName));

        // At least one logical metric is required
        this.logicalMetrics = generateLogicalMetrics(logicalMetrics, metricDictionary, dimensionDictionary, table);
        validateMetrics(this.logicalMetrics, this.table);

        // Zero or more grouping dimensions may be specified
        this.dimensions = generateDimensions(dimensions, dimensionDictionary);
        validateRequestDimensions(this.dimensions, this.table);

        // Map of dimension to its fields specified using show clause (matrix params)
        this.perDimensionFields = generateDimensionFields(dimensions, dimensionDictionary);

        // Zero or more filtering dimensions may be referenced
        this.filters = generateFilters(filters, table, dimensionDictionary);
        validateRequestDimensions(this.filters.keySet(), this.table);

        // Zero or more having queries may be referenced
        this.havings = generateHavings(havings, this.logicalMetrics,  metricDictionary);

        this.having = DruidHavingBuilder.buildHavings(this.havings);

        //Using the LinkedHashMap to preserve the sort order
        LinkedHashMap<String, SortDirection> sortColumnDirection = generateSortColumns(sorts);

        //Requested sort on dateTime column
        this.dateTimeSort = generateDateTimeSortColumn(sortColumnDirection);

        // Requested sort on metrics - optional, can be empty Set
        this.sorts = generateSortColumns(
                removeDateTimeSortColumn(sortColumnDirection),
                this.logicalMetrics, metricDictionary
        );

        // Overall requested number of rows in the response. Ignores grouping in time buckets.
        this.count = generateInteger(count, "count");

        // This is the validation part for count that is inlined here because currently it is very brief.
        if (this.count < 0) {
            LOG.debug(INTEGER_INVALID.logFormat(count, "count"));
            throw new BadApiRequestException(INTEGER_INVALID.logFormat(count, "count"));
        }

        // Requested number of rows per time bucket in the response
        this.topN = generateInteger(topN, "topN");

        // This is the validation part for topN that is inlined here because currently it is very brief.
        if (this.topN < 0) {
            LOG.debug(INTEGER_INVALID.logFormat(topN, "topN"));
            throw new BadApiRequestException(INTEGER_INVALID.logFormat(topN, "topN"));
        } else if (this.topN > 0 && this.sorts.isEmpty()) {
            LOG.debug(TOP_N_UNSORTED.logFormat(topN));
            throw new BadApiRequestException(TOP_N_UNSORTED.format(topN));
        }

        LOG.debug(
                "Api request: TimeGrain: {}," +
                        " Table: {}," +
                        " Dimensions: {}," +
                        " Dimension Fields: {}," +
                        " Filters: {},\n" +
                        " Havings: {},\n" +
                        " Logical metrics: {},\n\n" +
                        " Sorts: {}," +
                        " Count: {}," +
                        " TopN: {}," +
                        " AsyncAfter: {}" +
                        " Format: {}" +
                        " Pagination: {}",
                this.granularity,
                this.table.getName(),
                this.dimensions,
                this.perDimensionFields,
                this.filters,
                this.havings,
                this.logicalMetrics,
                this.sorts,
                this.count,
                this.topN,
                this.asyncAfter,
                this.format,
                this.paginationParameters
        );

        validateAggregatability(this.dimensions, this.filters);
        validateTimeAlignment(this.granularity, this.intervals);
    }

    /**
     * To check whether dateTime column request is first one in the sort list or not.
     *
     * @param sortColumns  LinkedHashMap of columns and its direction. Using LinkedHashMap to preserve the order
     *
     * @return True if dateTime column is first one in the sort list. False otherwise
     */
    protected Boolean isDateTimeFirstSortField(LinkedHashMap<String, SortDirection> sortColumns) {
        if (sortColumns != null) {
            List<String> columns = new ArrayList<>(sortColumns.keySet());
            return columns.get(0).equals(DATE_TIME_STRING);
        } else {
            return false;
        }
    }

    /**
     * Method to remove the dateTime column from map of columns and its direction.
     *
     * @param sortColumns  map of columns and its direction
     *
     * @return  Map of columns and its direction without dateTime sort column
     */
    protected Map<String, SortDirection> removeDateTimeSortColumn(Map<String, SortDirection> sortColumns) {
        if (sortColumns != null && sortColumns.containsKey(DATE_TIME_STRING)) {
            sortColumns.remove(DATE_TIME_STRING);
            return sortColumns;
        } else {
            return sortColumns;
        }
    }

    /**
     * Method to generate DateTime sort column from the map of columns and its direction.
     *
     * @param sortColumns  LinkedHashMap of columns and its direction. Using LinkedHashMap to preserve the order
     *
     * @return Instance of OrderByColumn for dateTime
     */
    protected Optional<OrderByColumn> generateDateTimeSortColumn(LinkedHashMap<String, SortDirection> sortColumns) {

        if (sortColumns != null && sortColumns.containsKey(DATE_TIME_STRING)) {
            if (!isDateTimeFirstSortField(sortColumns)) {
                LOG.debug(DATE_TIME_SORT_VALUE_INVALID.logFormat());
                throw new BadApiRequestException(DATE_TIME_SORT_VALUE_INVALID.format());
            } else {
                return Optional.of(new OrderByColumn(DATE_TIME_STRING, sortColumns.get(DATE_TIME_STRING)));
            }
        } else {
            return Optional.empty();
        }
    }

    /**
     * Method to convert sort list to column and direction map.
     *
     * @param sorts  String of sort columns
     *
     * @return LinkedHashMap of columns and their direction. Using LinkedHashMap to preserve the order
     */
    protected LinkedHashMap<String, SortDirection> generateSortColumns(String sorts) {
        LinkedHashMap<String, SortDirection> sortDirectionMap = new LinkedHashMap();

        if (sorts != null && !sorts.isEmpty()) {
            Arrays.stream(sorts.split(","))
                    .map(e -> Arrays.asList(e.split("\\|")))
                    .forEach(e -> sortDirectionMap.put(e.get(0), getSortDirection(e)));
            return sortDirectionMap;
        }
        return null;
    }

    /**
     * No argument constructor, meant to be used only for testing.
     */
    @ForTesting
    protected DataApiRequest() {
        super();
        this.table = null;
        this.granularity = null;
        this.dimensions = null;
        this.perDimensionFields = null;
        this.logicalMetrics = null;
        this.intervals = null;
        this.filterBuilder = new DefaultDruidFilterBuilder();
        this.filters = null;
        this.havings = null;
        this.having = null;
        this.sorts = null;
        this.dateTimeSort = null;
        this.count = 0;
        this.topN = 0;
        this.timeZone = null;
    }

    /**
     * All argument constructor, meant to be used for rewriting apiRequest.
     *  @param format  Format for the response
     * @param paginationParameters  Pagination info
     * @param uriInfo  The URI info
     * @param builder  A response builder
     * @param table  Logical table requested
     * @param granularity  Granularity of the request
     * @param dimensions  Grouping dimensions of the request
     * @param perDimensionFields  Fields for each of the grouped dimensions
     * @param logicalMetrics  Metrics requested
     * @param intervals  Intervals requested
     * @param filters  Global filters
     * @param havings  Top-level Having caluses for the request
     * @param having  Single global Druid Having
     * @param sorts  Sorting info for the request
     * @param count  Global limit for the request
     * @param topN  Count of per-bucket limit (TopN) for the request
     * @param asyncAfter  How long in milliseconds the user is willing to wait for a synchronous response
     * @param timeZone  TimeZone for the request
     * @param filterBuilder  A builder to use when building filters for the request
     * @param dateTimeSort A dateTime sort column with its direction
     */
    private DataApiRequest(
            ResponseFormatType format,
            Optional<PaginationParameters> paginationParameters,
            UriInfo uriInfo,
            Response.ResponseBuilder builder,
            LogicalTable table,
            Granularity granularity,
            Set<Dimension> dimensions,
            LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> perDimensionFields,
            Set<LogicalMetric> logicalMetrics,
            Set<Interval> intervals,
            Map<Dimension, Set<ApiFilter>> filters,
            Map<LogicalMetric, Set<ApiHaving>> havings,
            Having having,
            LinkedHashSet<OrderByColumn> sorts,
            int count,
            int topN,
            long asyncAfter,
            DateTimeZone timeZone,
            DruidFilterBuilder filterBuilder,
            Optional<OrderByColumn> dateTimeSort
    ) {
        super(format, asyncAfter, paginationParameters, uriInfo, builder);
        this.table = table;
        this.granularity = granularity;
        this.dimensions = dimensions;
        this.perDimensionFields = perDimensionFields;
        this.logicalMetrics = logicalMetrics;
        this.intervals = intervals;
        this.filters = filters;
        this.havings = havings;
        this.having = having;
        this.sorts = sorts;
        this.count = count;
        this.topN = topN;
        this.timeZone = timeZone;
        this.filterBuilder = filterBuilder;
        this.dateTimeSort = dateTimeSort;
    }

    /**
     * Extracts the list of dimensions from the url dimension path segments and "show" matrix params and generates a map
     * of dimension to dimension fields which needs to be annotated on the response.
     * <p>
     * If no "show" matrix param has been set, it returns the default dimension fields configured for the dimension.
     *
     * @param apiDimensionPathSegments  Path segments for the dimensions
     * @param dimensionDictionary  Dimension dictionary to look the dimensions up in
     *
     * @return A map of dimension to requested dimension fields
     */
    protected LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> generateDimensionFields(
            @NotNull List<PathSegment> apiDimensionPathSegments,
            @NotNull DimensionDictionary dimensionDictionary
    ) {
        try (TimedPhase timer = RequestLog.startTiming("GeneratingDimensionFields")) {
            return apiDimensionPathSegments.stream()
                    .filter(pathSegment -> !pathSegment.getPath().isEmpty())
                    .collect(Collectors.toMap(
                            pathSegment -> dimensionDictionary.findByApiName(pathSegment.getPath()),
                            pathSegment -> bindShowClause(pathSegment, dimensionDictionary),
                            (LinkedHashSet<DimensionField> e, LinkedHashSet<DimensionField> i) -> {
                                e.addAll(i);
                                return e;
                            },
                            LinkedHashMap::new
                    ));
        }
    }

    /**
     * Given a path segment, bind the fields specified in it's "show" matrix parameter for the dimension specified in
     * the path segment's path.
     *
     * @param pathSegment  Path segment to bind from
     * @param dimensionDictionary  Dimension dictionary to look the dimension up in
     *
     * @return the set of bound DimensionFields specified in the show clause
     * @throws BadApiRequestException if any of the specified fields are not valid for the dimension
     */
    private LinkedHashSet<DimensionField> bindShowClause(
            PathSegment pathSegment,
            DimensionDictionary dimensionDictionary
    )
            throws BadApiRequestException {
        Dimension dimension = dimensionDictionary.findByApiName(pathSegment.getPath());
        List<String> showFields = pathSegment.getMatrixParameters().entrySet().stream()
                .filter(entry -> entry.getKey().equals("show"))
                .flatMap(entry -> entry.getValue().stream())
                .flatMap(s -> Arrays.stream(s.split(",")))
                .collect(Collectors.toList());

        if (showFields.size() == 1 && showFields.contains(DimensionFieldSpecifierKeywords.ALL.toString())) {
            // Show all fields
            return dimension.getDimensionFields();
        } else if (showFields.size() == 1 && showFields.contains(DimensionFieldSpecifierKeywords.NONE.toString())) {
            // Show no fields
            return new LinkedHashSet<>();
        } else if (!showFields.isEmpty()) {
            // Show the requested fields
            return bindDimensionFields(dimension, showFields);
        } else {
            // Show the default fields
            return dimension.getDefaultDimensionFields();
        }
    }

    /**
     * Given a Dimension and a set of DimensionField names, bind the names to the available dimension fields of the
     * dimension.
     *
     * @param dimension  Dimension to bind the fields for
     * @param showFields  Names of the fields to bind
     *
     * @return the set of DimensionFields for the names
     * @throws BadApiRequestException if any of the names are not dimension fields on the dimension
     */
    private LinkedHashSet<DimensionField> bindDimensionFields(Dimension dimension, List<String> showFields)
            throws BadApiRequestException {
        Map<String, DimensionField> dimensionNameToFieldMap = dimension.getDimensionFields().stream()
                .collect(StreamUtils.toLinkedDictionary(DimensionField::getName));

        LinkedHashSet<DimensionField> dimensionFields = new LinkedHashSet<>();
        Set<String> invalidDimensionFields = new LinkedHashSet<>();
        for (String field : showFields) {
            if (dimensionNameToFieldMap.containsKey(field)) {
                dimensionFields.add(dimensionNameToFieldMap.get(field));
            } else {
                invalidDimensionFields.add(field);
            }
        }

        if (!invalidDimensionFields.isEmpty()) {
            LOG.debug(DIMENSION_FIELDS_UNDEFINED.logFormat(invalidDimensionFields, dimension.getApiName()));
            throw new BadApiRequestException(DIMENSION_FIELDS_UNDEFINED.format(
                    invalidDimensionFields,
                    dimension.getApiName()
            ));
        }
        return dimensionFields;
    }

    /**
     * Validate that the request references any non-aggregatable dimensions in a valid way.
     *
     * @param apiDimensions  the set of group by dimensions.
     * @param apiFilters  the set of api filters.
     *
     * @throws BadApiRequestException if a the request violates aggregatability constraints of dimensions.
     */
    protected void validateAggregatability(Set<Dimension> apiDimensions, Map<Dimension, Set<ApiFilter>> apiFilters)
            throws BadApiRequestException {
        // The set of non-aggregatable dimensions requested as group by dimensions
        Set<Dimension> nonAggGroupByDimensions = apiDimensions.stream()
                .filter(StreamUtils.not(Dimension::isAggregatable))
                .collect(Collectors.toSet());

        // Check that out of the non-aggregatable dimensions that are not referenced in the group by set already,
        // none of them is mentioned in a filter with more or less than one value
        boolean isValid = apiFilters.entrySet().stream()
                .filter(entry -> !entry.getKey().isAggregatable())
                .filter(entry -> !nonAggGroupByDimensions.contains(entry.getKey()))
                .map(Map.Entry::getValue)
                .noneMatch(valueSet -> valueSet.stream().anyMatch(isNonAggregatableInFilter()));

        if (!isValid) {
            List<String> invalidDimensionsInFilters = apiFilters.entrySet().stream()
                    .filter(entry -> !entry.getKey().isAggregatable())
                    .filter(entry -> !nonAggGroupByDimensions.contains(entry.getKey()))
                    .filter(entry -> entry.getValue().stream().anyMatch(isNonAggregatableInFilter()))
                    .map(Map.Entry::getKey)
                    .map(Dimension::getApiName)
                    .collect(Collectors.toList());

            LOG.debug(NON_AGGREGATABLE_INVALID.logFormat(invalidDimensionsInFilters));
            throw new BadApiRequestException(NON_AGGREGATABLE_INVALID.format(invalidDimensionsInFilters));
        }
    }

    /**
     * Validity rules for non-aggregatable dimensions that are only referenced in filters.
     * A query that references a non-aggregatable dimension in a filter without grouping by this dimension, is valid
     * only if the requested dimension field is a key for this dimension and only a single value is requested
     * with an inclusive operator ('in' or 'eq').
     *
     * @return A predicate that determines a given dimension is non aggregatable and also not constrained to one row
     * per result
     */
    public static Predicate<ApiFilter> isNonAggregatableInFilter() {
        return apiFilter ->
                !apiFilter.getDimensionField().equals(apiFilter.getDimension().getKey()) ||
                        apiFilter.getValues().size() != 1 ||
                        !(
                                apiFilter.getOperation().equals(FilterOperation.in) ||
                                        apiFilter.getOperation().equals(FilterOperation.eq)
                        );
    }

    /**
     * Extracts the list of metrics from the url metric query string and generates a set of LogicalMetrics.
     *
     * @param apiMetricQuery  URL query string containing the metrics separated by ','.
     * @param metricDictionary  Metric dictionary contains the map of valid metric names and logical metric objects.
     * @param dimensionDictionary  Dimension dictionary to look the dimension up in
     * @param table  The logical table for the data request
     *
     * @return Set of metric objects.
     * @throws BadApiRequestException if the metric dictionary returns a null or if the apiMetricQuery is invalid.
     */
    protected LinkedHashSet<LogicalMetric> generateLogicalMetrics(
            String apiMetricQuery,
            MetricDictionary metricDictionary,
            DimensionDictionary dimensionDictionary,
            LogicalTable table
    ) throws BadApiRequestException {
        try (TimedPhase timer = RequestLog.startTiming("GeneratingLogicalMetrics")) {
            LOG.trace("Metric dictionary: {}", metricDictionary);

            if (apiMetricQuery == null || "".equals(apiMetricQuery)) {
                LOG.debug(METRICS_MISSING.logFormat());
                throw new BadApiRequestException(METRICS_MISSING.format());
            }
            // set of logical metric objects
            LinkedHashSet<LogicalMetric> generated = new LinkedHashSet<>();
            List<String> invalidMetricNames = new ArrayList<>();

            //If INTERSECTION_REPORTING_ENABLED flag is true, convert the aggregators into FilteredAggregators and
            //replace old PostAggs with  new postAggs in order to generate a new Filtered Logical Metric
            if (BardFeatureFlag.INTERSECTION_REPORTING.isOn()) {
                ArrayNode metricsJsonArray;
                try {
                    //For a given metricString, returns an array of json objects contains metric name and associated
                    // filters

                    metricsJsonArray = MetricParser.generateMetricFilterJsonArray(apiMetricQuery);
                } catch (IllegalArgumentException e) {
                    LOG.debug(INCORRECT_METRIC_FILTER_FORMAT.logFormat(e.getMessage()));
                    throw new BadApiRequestException(INCORRECT_METRIC_FILTER_FORMAT.format(apiMetricQuery));
                }
                //check for the duplicate occurrence of metrics in an API
                FieldConverterSupplier.metricsFilterSetBuilder.validateDuplicateMetrics(metricsJsonArray);
                for (int i = 0; i < metricsJsonArray.size(); i++) {
                    JsonNode jsonObject;
                    try {
                        jsonObject = metricsJsonArray.get(i);
                    } catch (IndexOutOfBoundsException e) {
                        LOG.debug(INCORRECT_METRIC_FILTER_FORMAT.logFormat(e.getMessage()));
                        throw new BadApiRequestException(INCORRECT_METRIC_FILTER_FORMAT.format(apiMetricQuery));
                    }
                    String metricName = jsonObject.get("name").asText();
                    LogicalMetric logicalMetric = metricDictionary.get(metricName);

                    // If metric dictionary returns a null, it means the requested metric is not found.
                    if (logicalMetric == null) {
                        invalidMetricNames.add(metricName);
                    } else {
                        //metricFilterObject contains all the filters for a given metric
                        JsonNode metricFilterObject = jsonObject.get("filter");

                        //Currently supporting AND operation for metric filters.
                        if (metricFilterObject.has("AND") && !metricFilterObject.get("AND").isNull()) {

                            //We currently do not support ratio metrics
                            if (logicalMetric.getCategory().equals(RATIO_METRIC_CATEGORY)) {
                                LOG.debug(
                                        UNSUPPORTED_FILTERED_METRIC_CATEGORY.logFormat(
                                                logicalMetric.getName(),
                                                logicalMetric.getCategory()
                                        )
                                );
                                throw new BadApiRequestException(
                                        UNSUPPORTED_FILTERED_METRIC_CATEGORY.format(
                                                logicalMetric.getName(),
                                                logicalMetric.getCategory()
                                        )
                                );
                            }
                            try {
                                logicalMetric = FieldConverterSupplier.metricsFilterSetBuilder.getFilteredLogicalMetric(
                                        logicalMetric,
                                        metricFilterObject,
                                        dimensionDictionary,
                                        table,
                                        this
                                );
                            } catch (DimensionRowNotFoundException dimRowException) {
                                LOG.debug(dimRowException.getMessage());
                                throw new BadApiRequestException(dimRowException.getMessage(), dimRowException);
                            }

                            //If metric filter isn't empty or it has anything other then 'AND' then throw an exception
                        } else if (!metricFilterObject.asText().isEmpty()) {
                            LOG.debug(INVALID_METRIC_FILTER_CONDITION.logFormat(metricFilterObject.asText()));
                            throw new BadApiRequestException(
                                    INVALID_METRIC_FILTER_CONDITION.format(metricFilterObject.asText())
                            );
                        }
                        generated.add(logicalMetric);
                    }
                }
            } else {
                //Feature flag for intersection reporting is disabled
                // list of metrics extracted from the query string
                List<String> metricApiQuery = Arrays.asList(apiMetricQuery.split(","));
                for (String metricName : metricApiQuery) {
                    LogicalMetric logicalMetric = metricDictionary.get(metricName);

                    // If metric dictionary returns a null, it means the requested metric is not found.
                    if (logicalMetric == null) {
                        invalidMetricNames.add(metricName);
                    } else {
                        generated.add(logicalMetric);
                    }
                }
            }

            if (!invalidMetricNames.isEmpty()) {
                LOG.debug(METRICS_UNDEFINED.logFormat(invalidMetricNames.toString()));
                throw new BadApiRequestException(METRICS_UNDEFINED.format(invalidMetricNames.toString()));
            }
            LOG.trace("Generated set of logical metric: {}", generated);
            return generated;
        }
    }

    /**
     * Generate current date based on granularity.
     *
     * @param dateTime  The current moment as a DateTime
     * @param timeGrain  The time grain used to round the date time
     *
     * @return truncated current date based on granularity
     */
    protected DateTime getCurrentDate(DateTime dateTime, TimeGrain timeGrain) {
        return timeGrain.roundFloor(dateTime);
    }

    /**
     * Generates having objects based on the having query in the api request.
     *
     * @param havingQuery  Expects a URL having query String in the format:
     * (dimension name)-(operation)[(value or comma separated values)]?
     * @param logicalMetrics  The logical metrics used in this query
     * @param metricDictionary  The metric dictionary to bind parsed metrics from the query
     *
     * @return Set of having objects.
     *
     * @throws BadApiRequestException if the having query string does not match required syntax.
     */
    protected Map<LogicalMetric, Set<ApiHaving>> generateHavings(
            String havingQuery,
            Set<LogicalMetric> logicalMetrics,
            MetricDictionary metricDictionary
    ) throws BadApiRequestException {
        try (TimedPhase phase = RequestLog.startTiming("GeneratingHavings")) {
            LOG.trace("Metric Dictionary: {}", metricDictionary);
            // Havings are optional hence check if havings are requested.
            if (havingQuery == null || "".equals(havingQuery)) {
                return Collections.emptyMap();
            }

            List<String> unmatchedMetrics = new ArrayList<>();

            // split on '],' to get list of havings
            List<String> apiHavings = Arrays.asList(havingQuery.split(COMMA_AFTER_BRACKET_PATTERN));
            Map<LogicalMetric, Set<ApiHaving>> generated = new LinkedHashMap<>();
            for (String apiHaving : apiHavings) {
                try {
                    ApiHaving newHaving = new ApiHaving(apiHaving, metricDictionary);
                    LogicalMetric metric = newHaving.getMetric();
                    if (!logicalMetrics.contains(metric)) {
                        unmatchedMetrics.add(metric.getName());
                    } else {
                        generated.putIfAbsent(metric, new LinkedHashSet<>());
                        generated.get(metric).add(newHaving);
                    }
                } catch (BadHavingException havingException) {
                    throw new BadApiRequestException(havingException.getMessage(), havingException);
                }
            }

            if (!unmatchedMetrics.isEmpty()) {
                LOG.debug(HAVING_METRICS_NOT_IN_QUERY_FORMAT.logFormat(unmatchedMetrics.toString()));
                throw new BadApiRequestException(
                        HAVING_METRICS_NOT_IN_QUERY_FORMAT.format(unmatchedMetrics.toString())
                );

            }

            LOG.trace("Generated map of havings: {}", generated);

            return generated;
        }
    }

    /**
     * Extract valid sort direction.
     *
     * @param columnWithDirection  Column and its sorting direction
     *
     * @return Sorting direction. If no direction provided then the default one will be DESC
     */
    protected SortDirection getSortDirection(List<String> columnWithDirection) {
        try {
            return columnWithDirection.size() == 2 ?
                    SortDirection.valueOf(columnWithDirection.get(1).toUpperCase(Locale.ENGLISH)) :
                    SortDirection.DESC;
        } catch (IllegalArgumentException ignored) {
            String sortDirectionName = columnWithDirection.get(1);
            LOG.debug(SORT_DIRECTION_INVALID.logFormat(sortDirectionName));
            throw new BadApiRequestException(SORT_DIRECTION_INVALID.format(sortDirectionName));
        }
    }

    /**
     * Generates a Set of OrderByColumn.
     *
     * @param sortDirectionMap  Map of columns and their direction
     * @param logicalMetrics  Set of LogicalMetrics in the query
     * @param metricDictionary  Metric dictionary contains the map of valid metric names and logical metric objects.
     *
     * @return a Set of OrderByColumn
     * @throws BadApiRequestException if the sort clause is invalid.
     */
    protected LinkedHashSet<OrderByColumn> generateSortColumns(
             Map<String, SortDirection> sortDirectionMap,
            Set<LogicalMetric> logicalMetrics,
            MetricDictionary metricDictionary
    ) throws BadApiRequestException {
        try (TimedPhase timer = RequestLog.startTiming("GeneratingSortColumns")) {
            String sortMetricName;
            LinkedHashSet<OrderByColumn> metricSortColumns = new LinkedHashSet<>();

            if (sortDirectionMap == null) {
                return metricSortColumns;
            }
            List<String> unknownMetrics = new ArrayList<>();
            List<String> unmatchedMetrics = new ArrayList<>();
            List<String> unsortableMetrics = new ArrayList<>();

            for (Map.Entry<String, SortDirection> entry : sortDirectionMap.entrySet())  {
                sortMetricName = entry.getKey();

                LogicalMetric logicalMetric = metricDictionary.get(sortMetricName);

                // If metric dictionary returns a null, it means the requested sort metric is not found.
                if (logicalMetric == null) {
                    unknownMetrics.add(sortMetricName);
                    continue;
                }
                if (!logicalMetrics.contains(logicalMetric)) {
                    unmatchedMetrics.add(sortMetricName);
                    continue;
                }
                if (logicalMetric.getTemplateDruidQuery() == null) {
                    unsortableMetrics.add(sortMetricName);
                    continue;
                }
                metricSortColumns.add(new OrderByColumn(logicalMetric, entry.getValue()));
            }
            if (!unknownMetrics.isEmpty()) {
                LOG.debug(SORT_METRICS_UNDEFINED.logFormat(unknownMetrics.toString()));
                throw new BadApiRequestException(SORT_METRICS_UNDEFINED.format(unknownMetrics.toString()));
            }
            if (!unmatchedMetrics.isEmpty()) {
                LOG.debug(SORT_METRICS_NOT_IN_QUERY_FORMAT.logFormat(unmatchedMetrics.toString()));
                throw new BadApiRequestException(SORT_METRICS_NOT_IN_QUERY_FORMAT.format(unmatchedMetrics.toString()));
            }
            if (!unsortableMetrics.isEmpty()) {
                LOG.debug(SORT_METRICS_NOT_SORTABLE_FORMAT.logFormat(unsortableMetrics.toString()));
                throw new BadApiRequestException(SORT_METRICS_NOT_SORTABLE_FORMAT.format(unsortableMetrics.toString()));
            }

            return metricSortColumns;
        }
    }

    /**
     * Parses the requested input String by converting it to an integer, while treating null as zero.
     *
     * @param value  The requested integer value as String.
     * @param parameterName  The parameter name that corresponds to the requested integer value.
     *
     * @return The integer corresponding to {@code value} or zero if {@code value} is null.
     * @throws BadApiRequestException if the input String can not be parsed as an integer.
     */
    protected int generateInteger(String value, String parameterName) throws BadApiRequestException {
        try {
            return value == null ? 0 : Integer.parseInt(value);
        } catch (NumberFormatException nfe) {
            LOG.debug(INTEGER_INVALID.logFormat(value, parameterName), nfe);
            throw new BadApiRequestException(INTEGER_INVALID.logFormat(value, parameterName), nfe);
        }
    }

    // CHECKSTYLE:OFF
    public DataApiRequest withFormat(ResponseFormatType format) {
        return new DataApiRequest(format, paginationParameters, uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, filters, havings, having, sorts, count, topN, asyncAfter, timeZone, filterBuilder, dateTimeSort);
    }

    public DataApiRequest withPaginationParameters(Optional<PaginationParameters> paginationParameters) {
        return new DataApiRequest(format, paginationParameters, uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, filters, havings, having, sorts, count, topN, asyncAfter, timeZone, filterBuilder, dateTimeSort);
    }

    public DataApiRequest withUriInfo(UriInfo uriInfo) {
        return new DataApiRequest(format, paginationParameters, uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, filters, havings, having, sorts, count, topN, asyncAfter, timeZone, filterBuilder, dateTimeSort);
    }

    public DataApiRequest withBuilder(Response.ResponseBuilder builder) {
        return new DataApiRequest(format, paginationParameters, uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, filters, havings, having, sorts, count, topN, asyncAfter, timeZone, filterBuilder, dateTimeSort);
    }

    public DataApiRequest withTable(LogicalTable table) {
        return new DataApiRequest(format, paginationParameters, uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, filters, havings, having, sorts, count, topN, asyncAfter, timeZone, filterBuilder, dateTimeSort);
    }

    public DataApiRequest withGranularity(Granularity granularity) {
        return new DataApiRequest(format, paginationParameters, uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, filters, havings, having, sorts, count, topN, asyncAfter, timeZone, filterBuilder, dateTimeSort);
    }

    public DataApiRequest withDimensions(Set<Dimension> dimensions) {
        return new DataApiRequest(format, paginationParameters, uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, filters, havings, having, sorts, count, topN, asyncAfter, timeZone, filterBuilder, dateTimeSort);
    }

    public DataApiRequest withPerDimensionFields(LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> perDimensionFields) {
        return new DataApiRequest(format, paginationParameters, uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, filters, havings, having, sorts, count, topN, asyncAfter, timeZone, filterBuilder, dateTimeSort);
    }

    public DataApiRequest withLogicalMetrics(Set<LogicalMetric> logicalMetrics) {
        return new DataApiRequest(format, paginationParameters, uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, filters, havings, having, sorts, count, topN, asyncAfter, timeZone, filterBuilder, dateTimeSort);
    }

    public DataApiRequest withIntervals(Set<Interval> intervals) {
        return new DataApiRequest(format, paginationParameters, uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, filters, havings, having, sorts, count, topN, asyncAfter, timeZone, filterBuilder, dateTimeSort);
    }

    public DataApiRequest withFilters(Map<Dimension, Set<ApiFilter>> filters) {
        return new DataApiRequest(format, paginationParameters, uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, filters, havings, having, sorts, count, topN, asyncAfter, timeZone, filterBuilder, dateTimeSort);
    }

    public DataApiRequest withHavings(Map<LogicalMetric, Set<ApiHaving>> havings) {
        return new DataApiRequest(format, paginationParameters, uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, filters, havings, having, sorts, count, topN, asyncAfter, timeZone, filterBuilder, dateTimeSort);
    }

    public DataApiRequest withHaving(Having having) {
        return new DataApiRequest(format, paginationParameters, uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, filters, havings, having, sorts, count, topN, asyncAfter, timeZone, filterBuilder, dateTimeSort);
    }

    public DataApiRequest withSorts(LinkedHashSet<OrderByColumn> sorts) {
        return new DataApiRequest(format, paginationParameters, uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, filters, havings, having, sorts, count, topN, asyncAfter, timeZone, filterBuilder, dateTimeSort);
    }

    public DataApiRequest withCount(int count) {
        return new DataApiRequest(format, paginationParameters, uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, filters, havings, having, sorts, count, topN, asyncAfter, timeZone, filterBuilder, dateTimeSort);
    }

    public DataApiRequest withTopN(int topN) {
        return new DataApiRequest(format, paginationParameters, uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, filters, havings, having, sorts, count, topN, asyncAfter, timeZone, filterBuilder, dateTimeSort);
    }

    public DataApiRequest withAsyncAfter(long asyncAfter) {
        return new DataApiRequest(format, paginationParameters, uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, filters, havings, having, sorts, count, topN, asyncAfter, timeZone, filterBuilder, dateTimeSort);
    }

    public DataApiRequest withTimeZone(DateTimeZone timeZone) {
        return new DataApiRequest(format, paginationParameters, uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, filters, havings, having, sorts, count, topN, asyncAfter, timeZone, filterBuilder, dateTimeSort);
    }

    public DataApiRequest withFilterBuilder(DruidFilterBuilder filterBuilder) {
        return new DataApiRequest(format, paginationParameters, uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, filters, havings, having, sorts, count, topN, asyncAfter, timeZone, filterBuilder, dateTimeSort);
    }

    // CHECKSTYLE:ON

    /**
     * Gets the filter dimensions form the given set of filter objects.
     *
     * @return Set of filter dimensions.
     */
    public Set<Dimension> getFilterDimensions() {
        return filters.keySet();
    }

    public LogicalTable getTable() {
        return this.table;
    }

    public Granularity getGranularity() {
        return this.granularity;
    }

    public Set<Dimension> getDimensions() {
        return this.dimensions;
    }

    public LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> getDimensionFields() {
        return this.perDimensionFields;
    }

    public Set<LogicalMetric> getLogicalMetrics() {
        return this.logicalMetrics;
    }

    public Set<Interval> getIntervals() {
        return this.intervals;
    }

    public Map<Dimension, Set<ApiFilter>> getFilters() {
        return this.filters;
    }

    /**
    * Builds and returns the Druid filters from this request's {@link ApiFilter}s.
    * <p>
    * The Druid filters are built (an expensive operation) every time this method is called. Use it judiciously.
    *
    * @return the Druid filter
    */
    public Filter getFilter() {
        try (TimedPhase timer = RequestLog.startTiming("BuildingDruidFilter")) {
            return filterBuilder.buildFilters(this.filters);
        } catch (DimensionRowNotFoundException e) {
            LOG.debug(e.getMessage());
            throw new BadApiRequestException(e);
        }
    }

    public Map<LogicalMetric, Set<ApiHaving>> getHavings() {
        return this.havings;
    }

    public Having getHaving() {
        return this.having;
    }

    public LinkedHashSet<OrderByColumn> getSorts() {
        return this.sorts;
    }

    public OptionalInt getCount() {
        return count == 0 ? OptionalInt.empty() : OptionalInt.of(count);
    }

    public OptionalInt getTopN() {
        return topN == 0 ? OptionalInt.empty() : OptionalInt.of(topN);
    }

    public DateTimeZone getTimeZone() {
        return timeZone;
    }

    public DruidFilterBuilder getFilterBuilder() {
        return filterBuilder;
    }

    public Optional<OrderByColumn> getDateTimeSort() {
        return dateTimeSort;
    }
}
