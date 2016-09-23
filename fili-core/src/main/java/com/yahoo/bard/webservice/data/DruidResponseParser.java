// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.RESULT_SET_ERROR;

import com.yahoo.bard.webservice.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.metric.MetricColumn;
import com.yahoo.bard.webservice.druid.model.QueryType;
import com.yahoo.bard.webservice.table.ZonedSchema;

import com.fasterxml.jackson.databind.JsonNode;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

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
     * @param queryType  the type of query
     *
     * @return the set of results
     */
    public ResultSet parse(JsonNode jsonResult, ZonedSchema schema, QueryType queryType) {

        LOG.trace("Parsing druid query {} by json result: {} using schema: {}", queryType, jsonResult, schema);

        /* Get dimension and metric columns */
        Set<DimensionColumn> dimensionColumns = schema.getColumns(DimensionColumn.class);
        Set<MetricColumn> metricColumns = schema.getColumns(MetricColumn.class);

        List<Result> results;
        switch (queryType) {
            case GROUP_BY:
                results = makeGroupByResults(jsonResult, dimensionColumns, metricColumns, schema.getDateTimeZone());
                break;
            case TOP_N:
                results = makeTopNResults(jsonResult, dimensionColumns, metricColumns, schema.getDateTimeZone());
                break;
            case TIMESERIES:
                results = makeTimeSeriesResults(jsonResult, metricColumns, schema.getDateTimeZone());
                break;
            case LOOKBACK:
                results = makeLookbackResults(jsonResult, dimensionColumns, metricColumns, schema.getDateTimeZone());
                break;
            default:
                String msg = RESULT_SET_ERROR.logFormat(queryType);
                LOG.error(msg);
                throw new UnsupportedOperationException(msg);
        }

        LOG.trace("Parsed druid query {} results: {}", queryType, results);
        return new ResultSet(results, schema);
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
}
