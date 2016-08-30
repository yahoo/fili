// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.config.BardFeatureFlag;
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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
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

import javax.ws.rs.core.StreamingOutput;

/**
 * Response class.
 */
public class Response {

    private static final Logger LOG = LoggerFactory.getLogger(Response.class);
    private static final Map<Dimension, Map<DimensionField, String>> DIMENSION_FIELD_COLUMN_NAMES = new HashMap<>();

    private final ResultSet resultSet;
    private final LinkedHashSet<MetricColumn> apiMetricColumns;
    private final LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> requestedApiDimensionFields;
    private final ResponseFormatType responseFormatType;
    private final SimplifiedIntervalList missingIntervals;
    private final SimplifiedIntervalList volatileIntervals;
    private final Map<String, URI> paginationLinks;
    private final JsonFactory jsonFactory;
    private final Pagination pagination;
    private final CsvMapper csvMapper;

    /**
     * Constructor.
     *
     * @param resultSet  ResultSet to turn into response
     * @param apiMetricColumnNames  The names of the logical metrics requested
     * @param requestedApiDimensionFields  The fields for each dimension that should be shown in the response
     * @param responseFormatType  The format in which the response should be returned to the user
     * @param missingIntervals  intervals over which partial data exists
     * @param volatileIntervals  intervals over which data is understood as 'best-to-date'
     * @param paginationLinks  A mapping from link names to links to be added to the end of the JSON response.
     * @param pagination  The object containing the pagination information. Null if we are not paginating.
     * @param objectMappers  Suite of Object Mappers to use when serializing
     */
    public Response(
            ResultSet resultSet,
            LinkedHashSet<String> apiMetricColumnNames,
            LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> requestedApiDimensionFields,
            ResponseFormatType responseFormatType,
            SimplifiedIntervalList missingIntervals,
            SimplifiedIntervalList volatileIntervals,
            Map<String, URI> paginationLinks,
            Pagination pagination,
            ObjectMappersSuite objectMappers
    ) {
        this.resultSet = resultSet;
        this.apiMetricColumns = generateApiMetricColumns(apiMetricColumnNames);
        this.requestedApiDimensionFields = requestedApiDimensionFields;
        this.responseFormatType = responseFormatType;
        this.missingIntervals = missingIntervals;
        this.volatileIntervals = volatileIntervals;
        this.paginationLinks = paginationLinks;
        this.pagination = pagination;
        this.jsonFactory = new JsonFactory(objectMappers.getMapper());
        this.csvMapper = objectMappers.getCsvMapper();

        LOG.trace("Initialized with ResultSet: {}", this.resultSet);
    }

    /**
     * Constructor.
     *
     * @param resultSet  ResultSet to turn into response
     * @param apiRequest  API Request to get the metric columns from
     * @param missingIntervals  intervals over which partial data exists
     * @param volatileIntervals  intervals over which data is understood as 'best-to-date'
     * @param paginationLinks  A mapping from link names to links to be added to the end of the JSON response.
     * @param pagination  The object containing the pagination information. Null if we are not paginating.
     * @param objectMappers  Suite of Object Mappers to use when serializing
     *
     * @deprecated  All the values needed to build a Response should be passed explicitly instead of relying on the
     * DataApiRequest
     */
    @Deprecated
    public Response(
            ResultSet resultSet,
            DataApiRequest apiRequest,
            SimplifiedIntervalList missingIntervals,
            SimplifiedIntervalList volatileIntervals,
            Map<String, URI> paginationLinks,
            Pagination pagination,
            ObjectMappersSuite objectMappers
    ) {
        this(
                resultSet,
                apiRequest.getLogicalMetrics().stream()
                        .map(LogicalMetric::getName)
                        .collect(Collectors.toCollection(LinkedHashSet<String>::new)),
                apiRequest.getDimensionFields(),
                apiRequest.getFormat(),
                missingIntervals,
                volatileIntervals,
                paginationLinks,
                pagination,
                objectMappers
        );
    }

    /**
     * Writes the response string in the proper format.
     *
     * @param os  The output stream to write the response bytes to
     *
     * @throws IOException if writing to output stream fails
     */
    public void write(OutputStream os) throws IOException {
        if (responseFormatType == ResponseFormatType.CSV) {
            writeCsvResponse(os);
        } else if (responseFormatType == ResponseFormatType.JSONAPI) {
            writeJsonApiResponse(os, missingIntervals, volatileIntervals, pagination);
        } else {
            writeJsonResponse(os, missingIntervals, volatileIntervals, pagination);
        }
    }

    /**
     * Writes JSON-API response.
     * <p/>
     * The response when serialized is in the following format:
     * <pre>
     * {@code{
     *   "rows" : [
     *     {
     *       "dateTime" : "YYYY-MM-dd HH:mm:ss.SSS",
     *       "logicalMetric1Name" : logicalMetric1Value,
     *       "logicalMetric2Name" : logicalMetric2Value,
     *       ...
     *       "dimension1Name" : "dimension1ValueKeyValue1",
     *       "dimension2Name" : "dimension2ValueKeyValue1",
     *       ...
     *     }, {
     *      ...
     *     }
     *   ],
     *   "dimension1Name" : [
     *     {
     *       "dimension1KeyFieldName" : "dimension1KeyFieldValue1",
     *       "dimension1OtherFieldName" : "dimension1OtherFieldValue1"
     *     }, {
     *       "dimension1KeyFieldName" : "dimension1KeyFieldValue2",
     *       "dimension1OtherFieldName" : "dimension1OtherFieldValue2"
     *     },
     *     ...
     *   ],
     *   "dimension2Name" : [
     *     {
     *       "dimension2KeyFieldName" : "dimension2KeyFieldValue1",
     *       "dimension2OtherFieldName" : "dimension2OtherFieldValue1"
     *     }, {
     *       "dimension2KeyFieldName" : "dimension2KeyFieldValue2",
     *       "dimension2OtherFieldName" : "dimension2OtherFieldValue2"
     *     },
     *     ...
     *   ]
     *   "linkName1" : "http://uri1",
     *   "linkName2": "http://uri2",
     *   ...
     *   "linkNameN": "http://uriN"
     * }
     * }
     * </pre>
     *
     * Where "linkName1" ... "linkNameN" are the N keys in paginationLinks, and "http://uri1" ... "http://uriN" are the
     * associated URI's.
     *
     * @param os  OutputStream
     * @param missingIntervals  intervals over which partial data exists
     * @param volatileIntervals  Intervals over which the data is volatile
     * @param pagination  Contains the pagination metadata (i.e. the number of rows per page, and the page requested).
     *
     * @throws IOException if a problem is encountered writing to the OutputStream
     */
    private void writeJsonApiResponse(
            OutputStream os,
            SimplifiedIntervalList missingIntervals,
            SimplifiedIntervalList volatileIntervals,
            Pagination pagination
    ) throws IOException {

        try (JsonGenerator generator = jsonFactory.createGenerator(os)) {
            // Holder for the dimension rows in the result set
            Map<Dimension, Set<Map<DimensionField, String>>> sidecars = new HashMap<>();
            for (DimensionColumn dimensionColumn : resultSet.getSchema().getColumns(DimensionColumn.class)) {
                sidecars.put(dimensionColumn.getDimension(), new LinkedHashSet<>());
            }

            // Start the top-level JSON object
            generator.writeStartObject();

            // Write the data rows and extract the dimension rows for the sidecars
            generator.writeArrayFieldStart("rows");
            for (Result result : resultSet) {
                generator.writeObject(buildResultRowWithSidecars(result, sidecars));
            }
            generator.writeEndArray();

            // Write the sidecar for each dimension
            for (Entry<Dimension, Set<Map<DimensionField, String>>> sidecar : sidecars.entrySet()) {
                generator.writeArrayFieldStart(sidecar.getKey().getApiName());
                for (Map<DimensionField, String> dimensionRow : sidecar.getValue()) {

                    // Write each of the sidecar rows
                    generator.writeStartObject();
                    for (DimensionField dimensionField : dimensionRow.keySet()) {
                        generator.writeObjectField(dimensionField.getName(), dimensionRow.get(dimensionField));
                    }
                    generator.writeEndObject();
                }
                generator.writeEndArray();
            }

            writeMetaObject(generator, missingIntervals, volatileIntervals, pagination);

            // End the top-level JSON object
            generator.writeEndObject();
        } catch (IOException e) {
            LOG.error("Unable to write JSON: {}", e.toString());
            throw e;
        }
    }


    /**
     * Writes JSON response.
     * <p/>
     * The response when serialized is in the following format
     * <pre>
     * {@code
     * {
     *     "metricColumn1Name" : metricValue1,
     *     "metricColumn2Name" : metricValue2,
     *     .
     *     .
     *     .
     *     "dimensionColumnName" : "dimensionRowDesc",
     *     "dateTime" : "formattedDateTimeString"
     * },
     *   "linkName1" : "http://uri1",
     *   "linkName2": "http://uri2",
     *   ...
     *   "linkNameN": "http://uriN"
     * }
     * </pre>
     *
     * Where "linkName1" ... "linkNameN" are the N keys in paginationLinks, and "http://uri1" ... "http://uriN" are the
     * associated URI's.
     *
     * @param os  OutputStream
     * @param missingIntervals  intervals over which partial data exists
     * @param volatileIntervals  Intervals over which the data is volatile
     * @param pagination  Contains the pagination metadata (i.e. the number of rows per page, and the page requested).
     *
     * @throws IOException if a problem is encountered writing to the OutputStream
     */
    private void writeJsonResponse(
            OutputStream os,
            SimplifiedIntervalList missingIntervals,
            SimplifiedIntervalList volatileIntervals,
            Pagination pagination
    ) throws IOException {

        try (JsonGenerator g = jsonFactory.createGenerator(os)) {
            g.writeStartObject();

            g.writeArrayFieldStart("rows");
            for (Result result : resultSet) {
                g.writeObject(buildResultRow(result));
            }
            g.writeEndArray();

            writeMetaObject(g, missingIntervals, volatileIntervals, pagination);

            g.writeEndObject();
        } catch (IOException e) {
            LOG.error("Unable to write JSON: {}", e.toString());
            throw e;
        }
    }

    /**
     * Writes CSV response.
     *
     * @param os  OutputStream
     *
     * @throws IOException if a problem is encountered writing to the OutputStreamC
     */
    private void writeCsvResponse(OutputStream os) throws IOException {
        CsvSchema schema = buildCsvHeaders();

        // Just write the header first
        csvMapper.writer().with(schema.withSkipFirstDataRow(true)).writeValue(os, Collections.emptyMap());

        ObjectWriter writer = csvMapper.writer().with(schema.withoutHeader());

        try {
            resultSet.stream()
                    .map(this::buildResultRow)
                    .forEachOrdered(
                            row -> {
                                try {
                                    writer.writeValue(os, row);
                                } catch (IOException ioe) {
                                    String msg = String.format("Unable to write CSV data row: %s", row);
                                    LOG.error(msg, ioe);
                                    throw new RuntimeException(msg, ioe);
                                }
                            }
                    );
        } catch (RuntimeException re) {
            throw new IOException(re);
        }
    }

    /**
     * Builds a set of only those metric columns which correspond to the metrics requested in the API.
     *
     * @param apiMetricColumnNames  Set of Metric names extracted from the requested api metrics
     *
     * @return set of metric columns
     */
    private LinkedHashSet<MetricColumn> generateApiMetricColumns(Set<String> apiMetricColumnNames) {
        // Get the metric columns from the schema
        Map<String, MetricColumn> metricColumnMap = resultSet.getSchema().getColumns(MetricColumn.class).stream()
                .collect(StreamUtils.toLinkedDictionary(MetricColumn::getName));

        // Select only api metrics from resultSet
        return apiMetricColumnNames.stream()
                .map(metricColumnMap::get)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Builds the CSV header.
     *
     * @return The CSV schema with the header
     */
    protected CsvSchema buildCsvHeaders() {
        CsvSchema.Builder builder = CsvSchema.builder();
        Stream.concat(
                Stream.of("dateTime"),
                Stream.concat(
                        requestedApiDimensionFields.entrySet().stream().flatMap(this::generateDimensionColumnHeaders),
                        apiMetricColumns.stream().map(MetricColumn::getName)
                )
        ).forEachOrdered(builder::addColumn);
        return builder.setUseHeader(true).build();
    }

    /**
     * Build the headers for the dimension columns.
     *
     * @param entry  Entry to base the columns on.
     *
     * @return the headers as a Stream
     */
    private Stream<String> generateDimensionColumnHeaders(Map.Entry<Dimension, LinkedHashSet<DimensionField>> entry) {
        if (entry.getValue().isEmpty()) {
            return Stream.of(entry.getKey().getApiName());
        } else {
            return entry.getValue().stream().map(dimField -> getDimensionColumnName(entry.getKey(), dimField));
        }
    }

    /**
     * Builds map of result row from a result.
     *
     * @param result  the result to process
     *
     * @return map of result row
     */
    private Map<String, Object> buildResultRow(Result result) {
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

                // When no fields are requested, show the key field
                if (requestedDimensionFields.isEmpty()) {
                    // When no fields are requested, show the key field
                    row.put(dimension.getApiName(), drow.get(dimension.getKey()));
                } else {
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
     * @param result  the result to process
     * @param sidecars  Map of sidecar data (dimension rows in the result)
     *
     * @return map of result row
     */
    private Map<String, Object> buildResultRowWithSidecars(
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

            if (requestedApiDimensionFields.get(dimension) != null) {
                // add sidecar only if at-least one field needs to be shown
                Set<DimensionField> requestedDimensionFields = requestedApiDimensionFields.get(dimension);
                if (requestedDimensionFields.size() > 0) {
                    // The key field is required
                    requestedDimensionFields.add(dimension.getKey());

                    //
                    Map<DimensionField, String> dimensionFieldToValueMap = requestedDimensionFields.stream()
                            .collect(StreamUtils.toLinkedMap(Function.identity(), dimensionRow::get));

                    // Add the dimension row's requested fields to the sidecar map
                    sidecars.get(dimension).add(dimensionFieldToValueMap);
                }
            }

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
    private List<String> buildIntervalStringList(Collection<Interval> intervals) {
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
    private static String getDimensionColumnName(Dimension dimension, DimensionField dimensionField) {
        Map<DimensionField, String> columnNamesForDimensionFields;
        columnNamesForDimensionFields = DIMENSION_FIELD_COLUMN_NAMES.computeIfAbsent(
                dimension,
                (key) -> new ConcurrentHashMap()
        );
        return columnNamesForDimensionFields.computeIfAbsent(
                dimensionField, (field) -> dimension.getApiName() + "|" + field.getName()
        );
    }

    /**
     * Builds the meta object for the JSON response. The meta object is only built if there were missing intervals, or
     * the results are being paginated.
     *
     * @param generator  The JsonGenerator used to build the JSON response.
     * @param missingIntervals  The set of intervals that do not contain data.
     * @param volatileIntervals  The set of intervals that have volatile data.
     * @param pagination  Object containing the pagination metadata (i.e. the number of rows per page, and the requested
     * page)
     *
     * @throws IOException if the generator throws an IOException.
     */
    private void writeMetaObject(
            JsonGenerator generator,
            Collection<Interval> missingIntervals,
            SimplifiedIntervalList volatileIntervals,
            Pagination pagination
    ) throws IOException {
        boolean paginating = pagination != null;
        boolean haveMissingIntervals = BardFeatureFlag.PARTIAL_DATA.isOn() && !missingIntervals.isEmpty();
        boolean haveVolatileIntervals = volatileIntervals != null && ! volatileIntervals.isEmpty();

        if (!paginating && !haveMissingIntervals && !haveVolatileIntervals) {
            return;
        }

        generator.writeObjectFieldStart("meta");

        // Add partial data info into the metadata block if needed.
        if (haveMissingIntervals) {
            generator.writeObjectField("missingIntervals", buildIntervalStringList(missingIntervals));
        }

        // Add volatile intervals
        if (haveVolatileIntervals) {
            generator.writeObjectField("volatileIntervals", buildIntervalStringList(volatileIntervals));
        }

        // Add pagination information if paginating.
        if (paginating) {
            generator.writeObjectFieldStart("pagination");

            for (Entry<String, URI> entry : paginationLinks.entrySet()) {
                generator.writeObjectField(entry.getKey(), entry.getValue());
            }

            generator.writeNumberField("currentPage", pagination.getPage());
            generator.writeNumberField("rowsPerPage", pagination.getPerPage());
            generator.writeNumberField("numberOfResults", pagination.getNumResults());

            generator.writeEndObject();
        }

        generator.writeEndObject();
    }

    /**
     * Get a resource method that can be used to stream this response as an entity.
     *
     * @return The resource method
     */
    public StreamingOutput getResponseStream() {
        return this::write;
    }
}
