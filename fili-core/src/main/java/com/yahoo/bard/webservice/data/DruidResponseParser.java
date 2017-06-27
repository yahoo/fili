// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.RESULT_SET_ERROR;

import com.yahoo.bard.webservice.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.metric.MetricColumn;
import com.yahoo.bard.webservice.druid.model.DefaultQueryType;
import com.yahoo.bard.webservice.druid.model.QueryType;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.table.Column;

import com.fasterxml.jackson.databind.JsonNode;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import javax.inject.Singleton;

/**
 * A class for building result sets from Druid Responses.
 */
//TODO:This class needs refactoring due to code duplication. The use of dependency injection also needs to be considered
@Singleton
public class DruidResponseParser {

    private static final Logger LOG = LoggerFactory.getLogger(DruidResponseParser.class);

    /**
     * Parse Druid GroupBy result into ResultSet.
     *
     * @param jsonResult  Druid results in json
     * @param schema  Schema for results
     * @param queryType  the type of query, note that this implementation only supports instances of
     * {@link DefaultQueryType}
     * @param dateTimeZone the time zone used for format the results
     *
     * @return the set of results
     */
    public ResultSet parse(
            JsonNode jsonResult,
            ResultSetSchema schema,
            QueryType queryType,
            DateTimeZone dateTimeZone
    ) {

        LOG.trace("Parsing druid query {} by json result: {} using schema: {}", queryType, jsonResult, schema);

        if (!(queryType instanceof DefaultQueryType)) {
            // Throw an exception for unsupported query types
            unsupportedQueryType(queryType);
        }
        DefaultQueryType defaultQueryType = (DefaultQueryType) queryType;

        /* Get dimension and metric columns */
        Set<DimensionColumn> dimensionColumns = schema.getColumns(DimensionColumn.class);
        Set<MetricColumn> metricColumns = schema.getColumns(MetricColumn.class);

        List<Result> results = null;
        switch (defaultQueryType) {
            case GROUP_BY:
                results = makeGroupByResults(jsonResult, dimensionColumns, metricColumns, dateTimeZone);
                break;
            case TOP_N:
                results = makeTopNResults(jsonResult, dimensionColumns, metricColumns, dateTimeZone);
                break;
            case TIMESERIES:
                results = makeTimeSeriesResults(jsonResult, metricColumns, dateTimeZone);
                break;
            case LOOKBACK:
                results = makeLookbackResults(jsonResult, dimensionColumns, metricColumns, dateTimeZone);
                break;
            default:
                // Throw an exception for unsupported query types
                unsupportedQueryType(queryType);
        }

        LOG.trace("Parsed druid query {} results: {}", queryType, results);
        return new ResultSet(schema, results);
    }

    /**
     * Log an error message and throw an exception for an unsupported query type.
     *
     * @param queryType  The query type that is not supported
     */
    private void unsupportedQueryType(QueryType queryType) {
        String msg = RESULT_SET_ERROR.logFormat(queryType);
        LOG.error(msg);
        throw new UnsupportedOperationException(msg);
    }

    /**
     * Create a list of results from a JsonNode of a groupBy response.
     *
     * @param jsonResult  current results to parse in json
     * @param dimensionColumns  set of dimension columns
     * @param metricColumns  set of metric columns
     * @param dateTimeZone  The date time zone to apply to timestamps
     *
     * @return list of results
     */
    private List<Result> makeGroupByResults(
            JsonNode jsonResult,
            Set<DimensionColumn> dimensionColumns,
            Set<MetricColumn> metricColumns,
            DateTimeZone dateTimeZone
    ) {
        List<Result> results = new ArrayList<>();

        for (JsonNode record : jsonResult) {
            DateTime timeStamp = new DateTime(record.get("timestamp").asText(), dateTimeZone);

            JsonNode event = record.get("event");
            LinkedHashMap<DimensionColumn, DimensionRow> dimensionRows = extractDimensionRows(dimensionColumns, event);
            LinkedHashMap<MetricColumn, Object> metricValues = extractMetricValues(metricColumns, event);

            results.add(new Result(dimensionRows, metricValues, timeStamp));
        }

        return results;
    }

    /**
     * Create a list of results from a JsonNode of a topN response.
     *
     * @param jsonResult  current record to parse
     * @param dimensionColumns  set of dimension columns
     * @param metricColumns  set of metric columns
     * @param dateTimeZone  The date time zone to apply to timestamps
     *
     * @return list of results
     */
    private List<Result> makeTopNResults(
            JsonNode jsonResult,
            Set<DimensionColumn> dimensionColumns,
            Set<MetricColumn> metricColumns,
            DateTimeZone dateTimeZone
    ) {
        List<Result> results = new ArrayList<>();

        /* loop over all records */
        for (JsonNode record : jsonResult) {
            DateTime timeStamp = new DateTime(record.get("timestamp").asText(), dateTimeZone);
            JsonNode result = record.get("result");

            /* loop over records per timebucket */
            for (final JsonNode entry : result) {
                LinkedHashMap<DimensionColumn, DimensionRow> dimensionRows = extractDimensionRows(
                        dimensionColumns,
                        entry
                );
                LinkedHashMap<MetricColumn, Object> metricValues = extractMetricValues(metricColumns, entry);

                results.add(new Result(dimensionRows, metricValues, timeStamp));
            }
        }

        return results;
    }

    /**
     * Create a list of results from a JsonNode of a timeseries response.
     *
     * @param jsonResult  current record to parse
     * @param metricColumns  set of metric columns
     * @param dateTimeZone  The date time zone to apply to timestamps
     *
     * @return list of results
     */
    private List<Result> makeTimeSeriesResults(
            JsonNode jsonResult,
            Set<MetricColumn> metricColumns,
            DateTimeZone dateTimeZone
    ) {
        List<Result> results = new ArrayList<>();

        /* loop over all records */
        for (JsonNode record : jsonResult) {
            DateTime timeStamp = new DateTime(record.get("timestamp").asText(), dateTimeZone);

            JsonNode result = record.get("result");
            LinkedHashMap<MetricColumn, Object> metricValues = extractMetricValues(metricColumns, result);

            results.add(new Result(new LinkedHashMap<>(), metricValues, timeStamp));
        }

        return results;
    }

    /**
     * Create a list of results from a JsonNode of a lookback response.
     *
     * @param jsonResult  current results to parse in json
     * @param dimensionColumns  set of dimension columns
     * @param metricColumns  set of metric columns
     * @param dateTimeZone  The date time zone to apply to timestamps
     *
     * @return list of results
     */
    private List<Result> makeLookbackResults(
            JsonNode jsonResult,
            Set<DimensionColumn> dimensionColumns,
            Set<MetricColumn> metricColumns,
            DateTimeZone dateTimeZone
    ) {
        List<Result> results = new ArrayList<>();

        for (JsonNode record : jsonResult) {
            DateTime timeStamp = new DateTime(record.get("timestamp").asText(), dateTimeZone);

            JsonNode result = record.get("result");
            LinkedHashMap<MetricColumn, Object> metricValues = extractMetricValues(metricColumns, result);


            LinkedHashMap<DimensionColumn, DimensionRow> dimensionRows;

            dimensionRows = dimensionColumns == null ?
                    new LinkedHashMap<>() :
                    extractDimensionRows(dimensionColumns, result);


            results.add(new Result(dimensionRows, metricValues, timeStamp));
        }
        return results;
    }

    /**
     * Extract the dimension rows for a json object given the set of all available dimension columns and the json
     * object.
     *
     * @param dimensionColumns  the set of dimension columns
     * @param entry  the json object
     *
     * @return map of dimension columns to dimension rows
     */
    private LinkedHashMap<DimensionColumn, DimensionRow> extractDimensionRows(
            Set<DimensionColumn> dimensionColumns,
            JsonNode entry
    ) {
        LinkedHashMap<DimensionColumn, DimensionRow> dimensionRows = new LinkedHashMap<>();

        for (DimensionColumn dc : dimensionColumns) {
            JsonNode fieldNode = entry.get(dc.getName());
            String fieldValue = "";
            if (fieldNode != null) {
                fieldValue = fieldNode.asText("");
            }

            DimensionRow drow = dc.getDimension().findDimensionRowByKeyValue(fieldValue);
            if (drow == null) {
                drow = dc.getDimension().createEmptyDimensionRow(fieldValue);
            }
            dimensionRows.put(dc, drow);
        }

        return dimensionRows;
    }

    /**
     * Extract the metric values for a json object given the set of all available metric columns and the json object.
     *
     * @param metricColumns  the set of metric columns
     * @param entry  the json object
     *
     * @return map of metric columns to metric values
     */
    private LinkedHashMap<MetricColumn, Object> extractMetricValues(
            Set<MetricColumn> metricColumns,
            JsonNode entry
    ) {
        LinkedHashMap<MetricColumn, Object> metricValues = new LinkedHashMap<>();

        for (MetricColumn mc : metricColumns) {
            JsonNode fieldNode = entry.get(mc.getName());
            if (fieldNode == null) {
                LOG.warn("Found null node for metric column {}", mc.getName());
            } else {
                metricValues.put(mc, getNodeValue(fieldNode));
            }
        }

        return metricValues;
    }

    /**
     * Extracts the value from a JsonNode.
     *
     * @param node  The node whose value is to be extracted
     *
     * @return the value as a BigDecimal if the node is a number, the value as a String if the node is textual,
     * the value as a boolean if the node is a boolean, null if the node is null, and node otherwise.
     */
    private Object getNodeValue(JsonNode node) {
        return node.isNumber() ? node.decimalValue() :
                node.isTextual() ? node.textValue() :
                node.isBoolean() ? node.booleanValue() :
                node.isNull() ? null :
                node;
    }

    /**
     * Produce the schema-defining columns for a given druid query.
     *
     * @param druidQuery  The query being modelled.
     *
     * @return A stream of columns based on the signature of the Druid Query.
     */
    public Stream<Column> buildSchemaColumns(DruidAggregationQuery<?> druidQuery) {
        // Pass through to druid query to allow for possible behavior customization on injected DruidResponseParsers.
        return druidQuery.buildSchemaColumns();
    }
}
