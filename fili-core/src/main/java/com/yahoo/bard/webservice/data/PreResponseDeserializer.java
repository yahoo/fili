// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data;

import static com.yahoo.bard.webservice.data.PreResponseSerializationProxy.RESPONSE_CONTEXT_KEY;
import static com.yahoo.bard.webservice.data.PreResponseSerializationProxy.RESULT_SET_KEY;
import static com.yahoo.bard.webservice.data.ResultSerializationProxy.DIMENSION_VALUES_KEY;
import static com.yahoo.bard.webservice.data.ResultSerializationProxy.METRIC_VALUES_KEY;
import static com.yahoo.bard.webservice.data.ResultSerializationProxy.TIMESTAMP_KEY;
import static com.yahoo.bard.webservice.data.ResultSetSerializationProxy.RESULTS_KEY;
import static com.yahoo.bard.webservice.data.ResultSetSerializationProxy.SCHEMA_DIM_COLUMNS;
import static com.yahoo.bard.webservice.data.ResultSetSerializationProxy.SCHEMA_GRANULARITY;
import static com.yahoo.bard.webservice.data.ResultSetSerializationProxy.SCHEMA_KEY;
import static com.yahoo.bard.webservice.data.ResultSetSerializationProxy.SCHEMA_METRIC_COLUMNS_TYPE;
import static com.yahoo.bard.webservice.data.ResultSetSerializationProxy.SCHEMA_TIMEZONE;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.metric.MetricColumn;
import com.yahoo.bard.webservice.data.metric.MetricColumnWithValueType;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.data.time.GranularityParser;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.util.GranularityParseException;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;
import com.yahoo.bard.webservice.web.PreResponse;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseContext;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Streams;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Class to de-serialize and prepare the PreResponse object from JSON. The advantages of custom deserialization are:
 * a. We can reconstruct the dimension rows for each result with the help of dimension dictionary.
 * b. Schema object de-serialization is more efficient then the default one.
 * c. Provides the flexibility of optimizing serialization
 * <p>
 * Sample serialized PreResponse string
 * <pre>
 * <code>
 *  {
         "responseContext": "[
                 "com.yahoo.bard.webservice.web.responseprocessors.ResponseContext",
                 {
                     "randomHeader": "someHeader",
                     "missingIntervals": ["java.util.ArrayList",
                         ["a","b", "c",
                             ["java.util.ArrayList",
                                 [
                                    [
                                     "org.joda.time.Interval",
                                     "2011-02-02T07:00:00.000Z/2011-02-03T10:15:00.000Z"
                                    ]
                                 ]
                             ],
                             [
                                 "java.math.BigDecimal",100
                             ]
                         ]
                     ]
                 }
         ]",
         "resultSet": {
             "results": [
                 {
                 "dimensionRows": {
                     "ageBracket": "1",
                     "country": "US",
                     "gender": "m"
                 },
                     "metricsRows": {
                     "lookbackPageViews": 112,
                     "retentionPageViews": 113,
                     "simplePageViews": 111
                 },
                 "timeStamp": "2016-01-12T00:00:00.000Z"
                 },
                 {
                     "dimensionRows": {
                     "ageBracket": "4",
                     "country": "IN",
                     "gender": "f"
                 },
                     "metricsRows": {
                     "lookbackPageViews": 212,
                     "retentionPageViews": 213,
                     "simplePageViews": 211
                 },
                 "timeStamp": "2016-01-12T00:00:00.000Z"
                 }
             ],
             "schema": {
                 "dimensionColumns": [
                     "ageBracket",
                     "country",
                     "gender"
                 ],
                     "granularity": "day",
                     "metricColumns": [
                     "lookbackPageViews",
                     "simplePageViews",
                     "retentionPageViews"
                 ],
                     "metricColumnsType": {
                     "lookbackPageViews": "java.math.BigDecimal",
                     "retentionPageViews": "java.math.BigDecimal",
                     "simplePageViews": "java.math.BigDecimal"
                 },
                 "timeZone": "UTC"
             }
         }
   }
 * </code>
 * </pre>
 */
public class PreResponseDeserializer {

    public static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    private static final Logger LOG = LoggerFactory.getLogger(PreResponseDeserializer.class);

    private final DimensionDictionary dimensionDictionary;
    private final GranularityParser granularityParser;
    private final ObjectMapper nonResponseContextMapper;
    private final ObjectMapper responseContextMapper;

    /**
     * Class constructor.
     *
     * @param dimensionDictionary  DimensionDictionary which contains all the details about dimensions
     * @param nonResponseContextMapper  Handles the deserialization of everything except the response context
     * @param responseContextMapper  Handles the deserialization of the response context of the PreResponse
     * @param granularityParser  Time grain provider
     */
    public PreResponseDeserializer(
            DimensionDictionary dimensionDictionary,
            ObjectMapper nonResponseContextMapper,
            ObjectMapper responseContextMapper,
            GranularityParser granularityParser
    ) {
        this.dimensionDictionary = dimensionDictionary;
        this.nonResponseContextMapper = nonResponseContextMapper;
        this.responseContextMapper = responseContextMapper;
        this.granularityParser = granularityParser;
    }

    /**
     * Deserialize the custom serialized PreResponse.
     *
     * @param preResponse  Custom serialized PreResponse
     *
     * @return De-serialized PreResponse object
     *
     * @throws IOException in case of deserialization of ResponseContext fails
     */
    public PreResponse deserialize(String preResponse) throws IOException {
         JsonNode serializedPreResponse = nonResponseContextMapper.readTree(preResponse);
         return new PreResponse(
                getResultSet(serializedPreResponse.get(RESULT_SET_KEY)),
                getResponseContext(serializedPreResponse.get(RESPONSE_CONTEXT_KEY))
        );
    }

    /**
     * Deserialize the serialized ResponseContext. Method throws an IOException when mapper fails to read the
     * serialized ResponseContext.
     *
     * @param serializedResponseContext  serialized ResponseContext as jsonNode
     *
     * @return Deserialized responseContext
     *
     * @throws IOException when there's a problem reading the response context from the JsonNode
     */
    private ResponseContext getResponseContext(JsonNode serializedResponseContext) throws IOException {
        return responseContextMapper.readValue(serializedResponseContext.asText(), ResponseContext.class);
    }

    /**
     * Generates ResultSet object from the JsonNode which contains the serialized ResultSet.
     *
     * @param serializedResultSet  JsonNode which contains the serialized ResultSet
     *
     * @return ResultSet object generated from JsonNode
     */
    private ResultSet getResultSet(JsonNode serializedResultSet) {
        ResultSetSchema resultSetSchema = getResultSetSchema(serializedResultSet.get(SCHEMA_KEY));
        List<Result> results = Streams.stream(serializedResultSet.get(RESULTS_KEY))
                .map(serializedResult -> getResult(serializedResult, resultSetSchema))
                .collect(Collectors.toList());

        return new ResultSet(resultSetSchema, results);
    }

    /**
     * Generates ZonedSchema object from given JsonNode.
     *
     * @param schemaNode  JsonNode which contains all the columns, timezone and granularity
     *
     * @return ResultSetSchema object generated from the JsonNode
     */
    private ResultSetSchema getResultSetSchema(JsonNode schemaNode) {
        DateTimeZone timezone = generateTimezone(
                schemaNode.get(SCHEMA_TIMEZONE).asText(),
                DateTimeZone.forID(
                        SYSTEM_CONFIG.getStringProperty(SYSTEM_CONFIG.getPackageVariableName("timezone"), "UTC")
                )
        );

        //Recreate ResultSetSchema
        LinkedHashSet<Column> columns = Stream.concat(
                Streams.stream(schemaNode.get(SCHEMA_DIM_COLUMNS))
                        .map(JsonNode::asText)
                        .map(this::resolveDimensionName)
                        .map(DimensionColumn::new),
                Streams.stream(() -> schemaNode.get(SCHEMA_METRIC_COLUMNS_TYPE).fields())
                        .map(entry -> new MetricColumnWithValueType(entry.getKey(), entry.getValue().asText()))
        ).collect(Collectors.toCollection(LinkedHashSet::new));

        return new ResultSetSchema(generateGranularity(schemaNode.get(SCHEMA_GRANULARITY).asText(), timezone), columns);
    }

    /**
     * Method to get dimension from DimensionDictionary for a given name.
     *
     * @param dimensionName  To find Dimension from dimension dictionary
     *
     * @return The dimension with the given name
     */
    private Dimension resolveDimensionName(String dimensionName) {
        Dimension dimension = dimensionDictionary.findByApiName(dimensionName);
        if (dimension == null) {
            String msg = ErrorMessageFormat.UNABLE_TO_FIND_DIMENSION_FROM_DICTIONARY.format(dimensionName);
            LOG.error(msg);
            throw new DeserializationException(msg);
        }
        return dimension;
    }


    /**
     * Creates new Result object from JsonNode.
     *
     * @param serializedResult  JsonNode which contains all the serialized details to generate Result object
     * @param resultSetSchema  Schema of the result to generate the Result object
     *
     * @return Result object generated from given JsonNode
     */
    private Result getResult(JsonNode serializedResult, ResultSetSchema resultSetSchema) {
        return new Result(
                extractDimensionValues(
                        serializedResult.get(DIMENSION_VALUES_KEY),
                        resultSetSchema.getColumns(DimensionColumn.class)
                ),
                extractMetricValues(
                        serializedResult.get(METRIC_VALUES_KEY),
                        resultSetSchema.getColumns(MetricColumnWithValueType.class)
                ),
                DateTime.parse(serializedResult.get(TIMESTAMP_KEY).asText())
        );
    }

    /**
     * Extracts dimension rows for the given dimension columns.
     *
     * @param dimensionRowsNode  JsonNode which contains all the dimension rows which contains dimension names
     * and its unique id as its value
     * @param dimensionColumns  DimensionColumns which needs to have dimension rows
     *
     * @return Map of all the dimensionRows associated with dimensionColumns
     */
    private Map<DimensionColumn, DimensionRow> extractDimensionValues(
            JsonNode dimensionRowsNode,
            Set<DimensionColumn> dimensionColumns
    ) {
        return dimensionColumns.stream().collect(Collectors.toMap(
                Function.identity(),
                dimensionColumn -> dimensionColumn.getDimension().findDimensionRowByKeyValue(
                        dimensionRowsNode.get(dimensionColumn.getDimension().getApiName()).asText()
                )
        ));
    }

    /**
     * Extracts the metric values for all the metric columns from JsonNode which contains all the metrics names and
     * respective values.
     *
     * @param metricsRows  JsonNode which contains all the metric names and respective values as key-value pair
     * @param metricColumns  MetricColumns with value types
     *
     * @return metric columns with their values
     */
    private Map<MetricColumn, Object> extractMetricValues(
            JsonNode metricsRows,
            Set<MetricColumnWithValueType> metricColumns
    ) {

        Map<MetricColumn, Object> metricColumnObjectMap = new HashMap<>();
        for (MetricColumnWithValueType metricColumn : metricColumns) {
            try {
                metricColumnObjectMap.put(
                        metricColumn,
                        //deserialize the metric value based on its class type
                        nonResponseContextMapper.readValue(
                                metricsRows.get(metricColumn.getName()).toString(), metricColumn.getClassType()
                        )
                );
            } catch (JsonParseException | NullPointerException e) {
                String msg = ErrorMessageFormat.METRIC_VALUE_PARSING_ERROR.format("parse");
                LOG.error(msg, e);
                throw new DeserializationException(msg, e);
            } catch (JsonMappingException e) {
                String msg = ErrorMessageFormat.METRIC_VALUE_PARSING_ERROR.format("map");
                LOG.error(msg, e);
                throw new DeserializationException(msg, e);
            } catch (IOException e) {
                String msg = ErrorMessageFormat.METRIC_VALUE_PARSING_ERROR.format("identify");
                LOG.error(msg, e);
                throw new DeserializationException(msg, e);
            }
        }
        return metricColumnObjectMap;
    }

    /**
     * Generate a Granularity instance based on given query granularity.
     *
     * @param granularity  A string representation of the granularity
     * @param dateTimeZone  The time zone to use for this granularity
     *
     * @return A granularity instance with time zone information
     *
     * @throws DeserializationException if the string matches no meaningful granularity
     */
    private Granularity generateGranularity(String granularity, DateTimeZone dateTimeZone)
            throws DeserializationException {
        try {
            return granularityParser.parseGranularity(granularity, dateTimeZone);
        } catch (GranularityParseException e) {
            String msg = ErrorMessageFormat.GRANULARITY_PARSING_ERROR.format(granularity);
            LOG.error(msg, e);
            throw new DeserializationException(msg, e);
        }
    }

    /**
     * Get a time zone instance for the specified time zone id.
     *
     * @param timeZoneId   the ID of the datetime zone, null means default
     * @param systemTimeZone  timeZone of the system
     *
     * @return the DateTimeZone object for the ID
     *
     * @throws DeserializationException When timeZoneId is unable to recognize
     */
    private DateTimeZone generateTimezone(String timeZoneId, DateTimeZone systemTimeZone)
            throws DeserializationException {
        if (timeZoneId == null) {
            return systemTimeZone;
        }
        try {
            return DateTimeZone.forID(timeZoneId);
        } catch (IllegalArgumentException e) {
            String msg = ErrorMessageFormat.UNKNOWN_TIMEZONE_ID.format(timeZoneId);
            LOG.error(msg, e);
            throw new DeserializationException(msg , e);
        }
    }

    @JsonIgnore
    public DimensionDictionary getDimensionDictionary() {
        return dimensionDictionary;
    }

    @JsonIgnore
    public ObjectMapper getNonResponseContextMapper() {
        return nonResponseContextMapper;
    }

    @JsonIgnore
    public ObjectMapper getResponseContextMapper() {
        return responseContextMapper;
    }

    @JsonIgnore
    public GranularityParser getGranularityParser() {
        return granularityParser;
    }
}
