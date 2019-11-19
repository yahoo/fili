// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.data.Result;
import com.yahoo.bard.webservice.data.ResultSet;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricColumn;
import com.yahoo.bard.webservice.util.DateTimeFormatterFactory;
import com.yahoo.bard.webservice.util.DateTimeUtils;
import com.yahoo.bard.webservice.util.Pagination;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;
import com.yahoo.bard.webservice.util.StreamUtils;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest;

import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * ResponseData class. A bag of metadata that may be needed by the `ResponseWriter` to serialize Fili's response,
 * including the `ResultSet` itself, along with a collection of methods to help `ResponseWriters` perform
 * serialization.
 */
public class ResponseData {

    protected static final Logger LOG = LoggerFactory.getLogger(ResponseData.class);
    protected static final Map<Dimension, Map<DimensionField, String>> DIMENSION_FIELD_COLUMN_NAMES = new HashMap<>();

    protected final ResultSet resultSet;
    protected final LinkedHashSet<MetricColumn> apiMetricColumns;
    protected final LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> requestedApiDimensionFields;
    protected final SimplifiedIntervalList missingIntervals;
    protected final SimplifiedIntervalList volatileIntervals;
    protected final Pagination pagination;
    protected final Map<String, URI> paginationLinks;

    /**
     * Constructor.
     *
     * @param resultSet  ResultSet to turn into response
     * @param apiMetricColumnNames  The names of the logical metrics requested
     * @param requestedApiDimensionFields  The fields for each dimension that should be shown in the response
     * @param missingIntervals  intervals over which partial data exists
     * @param volatileIntervals  intervals over which data is understood as 'best-to-date'
     * @param pagination  The object containing the pagination information. Null if we are not paginating.
     * @param paginationLinks  A mapping from link names to links to be added to the end of the JSON response.
     */
    public ResponseData(
            ResultSet resultSet,
            LinkedHashSet<String> apiMetricColumnNames,
            LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> requestedApiDimensionFields,
            SimplifiedIntervalList missingIntervals,
            SimplifiedIntervalList volatileIntervals,
            Pagination pagination,
            Map<String, URI> paginationLinks
    ) {
        this.resultSet = resultSet;
        this.apiMetricColumns = generateApiMetricColumns(apiMetricColumnNames);
        this.requestedApiDimensionFields = requestedApiDimensionFields;
        this.missingIntervals = missingIntervals;
        this.volatileIntervals = volatileIntervals;
        this.pagination = pagination;
        this.paginationLinks = paginationLinks;

        LOG.trace("Initialized with ResultSet: {}", this.resultSet);
    }

    /**
     * Constructor.
     *
     * @param resultSet  ResultSet to turn into response
     * @param apiRequest  API Request to get the metric columns from
     * @param missingIntervals  intervals over which partial data exists
     * @param volatileIntervals  intervals over which data is understood as 'best-to-date'
     * @param pagination  The object containing the pagination information. Null if we are not paginating.
     * @param paginationLinks  A mapping from link names to links to be added to the end of the JSON response.
     *
     * @deprecated  All the values needed to build a Response should be passed explicitly instead of relying on the
     * DataApiRequest
     */
    @Deprecated
    public ResponseData(
            ResultSet resultSet,
            DataApiRequest apiRequest,
            SimplifiedIntervalList missingIntervals,
            SimplifiedIntervalList volatileIntervals,
            Pagination pagination,
            Map<String, URI> paginationLinks
    ) {
        this(
                resultSet,
                apiRequest.getLogicalMetrics().stream()
                        .map(LogicalMetric::getName)
                        .collect(Collectors.toCollection(LinkedHashSet<String>::new)),
                apiRequest.getDimensionFields(),
                missingIntervals,
                volatileIntervals,
                pagination,
                paginationLinks
        );
    }

    public ResultSet getResultSet() {
        return resultSet;
    }

    public SimplifiedIntervalList getMissingIntervals() {
        return missingIntervals;
    }

    public SimplifiedIntervalList getVolatileIntervals() {
        return volatileIntervals;
    }

    public Pagination getPagination() {
        return pagination;
    }

    public Map<String, URI> getPaginationLinks() {
        return paginationLinks;
    }

    public LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> getRequestedApiDimensionFields() {
        return requestedApiDimensionFields;
    }

    public LinkedHashSet<MetricColumn> getApiMetricColumns() {
        return apiMetricColumns;
    }

    /**
     * Builds a set of only those metric columns which correspond to the metrics requested in the API.
     *
     * @param apiMetricColumnNames  Set of Metric names extracted from the requested api metrics
     *
     * @return set of metric columns
     */
    protected LinkedHashSet<MetricColumn> generateApiMetricColumns(Set<String> apiMetricColumnNames) {
        // Get the metric columns from the schema
        Map<String, MetricColumn> metricColumnMap = resultSet.getSchema().getColumns(MetricColumn.class).stream()
                .collect(StreamUtils.toLinkedDictionary(MetricColumn::getName));

        // Select only api metrics from resultSet
        return apiMetricColumnNames.stream()
                .map(metricColumnMap::get)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Build the headers for the dimension columns.
     *
     * @param entry  Entry to base the columns on.
     *
     * @return the headers as a Stream
     */
    public Stream<String> generateDimensionColumnHeaders(Map.Entry<Dimension, LinkedHashSet<DimensionField>> entry) {
        if (entry.getValue().isEmpty()) {
            return Stream.empty();
        }
        return entry.getValue().stream().map(dimField -> getDimensionColumnName(entry.getKey(), dimField));
    }

    /**
     * Builds map of result row from a result.
     *
     * @param result  The result to process
     *
     * @return map of result row
     */
    public Map<String, Object> buildResultRow(Result result) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("dateTime", result.getTimeStamp().toString(DateTimeFormatterFactory.getOutputFormatter()));

        // Loop through the Map<DimensionColumn, DimensionRow> and format it to dimensionColumnName : dimensionRowDesc
        Map<DimensionColumn, DimensionRow> dr = result.getDimensionRows();
        for (Entry<DimensionColumn, DimensionRow> dce : dr.entrySet()) {
            DimensionRow drow = dce.getValue();
            Dimension dimension = dce.getKey().getDimension();

            Set<DimensionField> requestedDimensionFields;
            if (requestedApiDimensionFields.get(dimension) != null) {
                requestedDimensionFields = requestedApiDimensionFields.get(dimension);

                if (!requestedDimensionFields.isEmpty()) {
                    // Otherwise, show the fields requested, with the pipe-separated name
                    for (DimensionField dimensionField : requestedDimensionFields) {
                        row.put(getDimensionColumnName(dimension, dimensionField), drow.get(dimensionField));
                    }
                }
            }
        }

        // Loop through the Map<MetricColumn, Object> and format it to a metricColumnName: metricValue map
        for (MetricColumn apiMetricColumn : apiMetricColumns) {
            row.put(apiMetricColumn.getName(), result.getMetricValue(apiMetricColumn));
        }
        return row;
    }

    /**
     * Builds map of result row from a result and loads the dimension rows into the sidecar map.
     *
     * @param result  The result to process
     * @param sidecars  Map of sidecar data (dimension rows in the result)
     *
     * @return map of result row
     */
    public Map<String, Object> buildResultRowWithSidecars(
            Result result,
            Map<Dimension, Set<Map<DimensionField, String>>> sidecars
    ) {

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("dateTime", result.getTimeStamp().toString(DateTimeFormatterFactory.getOutputFormatter()));

        // Loop through the Map<DimensionColumn, DimensionRow> and format it to dimensionColumnName : dimensionRowKey
        Map<DimensionColumn, DimensionRow> dr = result.getDimensionRows();
        for (Entry<DimensionColumn, DimensionRow> dimensionColumnEntry : dr.entrySet()) {
            // Get the pieces we need out of the map entry
            Dimension dimension = dimensionColumnEntry.getKey().getDimension();
            DimensionRow dimensionRow = dimensionColumnEntry.getValue();
            Set<DimensionField> requestedDimensionFields = requestedApiDimensionFields.get(dimension);

            if (requestedDimensionFields == null || requestedDimensionFields.isEmpty())
            {
                // add sidecar only if at-least one field needs to be shown
                continue;
            }

            // The key field is required
            requestedDimensionFields.add(dimension.getKey());

            Map<DimensionField, String> dimensionFieldToValueMap = requestedDimensionFields.stream()
                    .collect(StreamUtils.toLinkedMap(Function.identity(), dimensionRow::get));

            // Add the dimension row's requested fields to the sidecar map
            sidecars.get(dimension).add(dimensionFieldToValueMap);

            // Put the dimension name and dimension row's key value into the row map
            row.put(dimension.getApiName(), dimensionRow.get(dimension.getKey()));
        }

        // Loop through the Map<MetricColumn, Object> and format it to a metricColumnName: metricValue map
        for (MetricColumn apiMetricColumn : apiMetricColumns) {
            row.put(apiMetricColumn.getName(), result.getMetricValue(apiMetricColumn));
        }
        return row;
    }

    /**
     * Build a list of interval strings. Format of interval string: yyyy-MM-dd' 'HH:mm:ss/yyyy-MM-dd' 'HH:mm:ss
     *
     * @param intervals  list of intervals to be converted into string
     *
     * @return list of interval strings
     */
    public List<String> buildIntervalStringList(Collection<Interval> intervals) {
        return intervals.stream()
                .map(it -> DateTimeUtils.intervalToString(it, DateTimeFormatterFactory.getOutputFormatter(), "/"))
                .collect(Collectors.toList());
    }

    /**
     * Retrieve dimension column name from cache, or build it and cache it.
     *
     * @param dimension  The dimension for the column name
     * @param dimensionField  The dimensionField for the column name
     *
     * @return The name for the dimension and column as it will appear in the response document
     */
    public static String getDimensionColumnName(Dimension dimension, DimensionField dimensionField) {
        Map<DimensionField, String> columnNamesForDimensionFields;
        columnNamesForDimensionFields = DIMENSION_FIELD_COLUMN_NAMES.computeIfAbsent(
                dimension,
                (key) -> new ConcurrentHashMap()
        );
        return columnNamesForDimensionFields.computeIfAbsent(
                dimensionField, (field) -> dimension.getApiName() + "|" + field.getName()
        );
    }
}
