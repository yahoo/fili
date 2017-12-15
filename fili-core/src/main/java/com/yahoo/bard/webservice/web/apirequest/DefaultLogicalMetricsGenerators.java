// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INCORRECT_METRIC_FILTER_FORMAT;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INVALID_METRIC_FILTER_CONDITION;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.METRICS_MISSING;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.METRICS_UNDEFINED;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.UNSUPPORTED_FILTERED_METRIC_CATEGORY;

import com.yahoo.bard.webservice.config.BardFeatureFlag;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.FilterBuilderException;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.druid.util.FieldConverterSupplier;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.TimedPhase;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;
import com.yahoo.bard.webservice.web.MetricParser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Utility class to hold generator code for metrics.
 */

public class DefaultLogicalMetricsGenerators {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultLogicalMetricsGenerators.class);

    public static DefaultLogicalMetricsGenerators INSTANCE = new DefaultLogicalMetricsGenerators();

    /**
     * Extracts the list of metrics from the url metric query string and generates a set of LogicalMetrics.
     *
     * @param apiMetricQuery  URL query string containing the metrics separated by ','.
     * @param metricDictionary  Metric dictionary contains the map of valid metric names and logical metric objects.
     * @param dimensionDictionary  Dimension dictionary to look the dimension up in
     * @param table  The logical table for the data request
     *
     * @return set of metric objects
     * @throws BadApiRequestException if the metric dictionary returns a null or if the apiMetricQuery is invalid.
     */
    public LinkedHashSet<LogicalMetric> generateLogicalMetrics(
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

            //If INTERSECTION_REPORTING_ENABLED flag is true, convert the aggregators into FilteredAggregators and
            //replace old PostAggs with new postAggs in order to generate a new Filtered Logical Metric
            if (BardFeatureFlag.INTERSECTION_REPORTING.isOn()) {
                generated = generateIntersectionMetrics(
                        table,
                        apiMetricQuery,
                        metricDictionary,
                        dimensionDictionary
                );
            } else {
                //Feature flag for intersection reporting is disabled
                // list of metrics extracted from the query string
                generated = generateLogicalMetrics(apiMetricQuery, metricDictionary);
            }
            LOG.trace("Generated set of logical metric: {}", generated);
            return generated;
        }
    }

    /**
     * Rewrite a metric using intersections across filter expressions.
     *
     * @param table  The logical table for metrics
     * @param apiMetricQuery  The apiMetric clause from the request
     * @param metricDictionary  The dictionary of query metrics
     * @param dimensionDictionary  The dictionary of query
     *
     * @return the modified logical metrics
     */
    public LinkedHashSet<LogicalMetric> generateIntersectionMetrics(
            LogicalTable table,
            String apiMetricQuery,
            MetricDictionary metricDictionary,
            DimensionDictionary dimensionDictionary
    ) {
        LinkedHashSet<LogicalMetric> generated = new LinkedHashSet<>();
        List<String> invalidMetricNames = new ArrayList<>();

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
                    if (logicalMetric.getCategory().equals(DataApiRequest.RATIO_METRIC_CATEGORY)) {
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
                                table
                        );
                    } catch (FilterBuilderException dimRowException) {
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

        if (!invalidMetricNames.isEmpty()) {
            String message = ErrorMessageFormat.METRICS_UNDEFINED.logFormat(invalidMetricNames);
            LOG.error(message);
            throw new BadApiRequestException(message);
        }
        return generated;
    }

    /**
     * Extracts the list of metrics from the url metric query string and generates a set of LogicalMetrics.
     *
     * @param apiMetricQuery  URL query string containing the metrics separated by ','.
     * @param metricDictionary  Metric dictionary contains the map of valid metric names and logical metric objects.
     *
     * @return Set of metric objects.
     * @throws BadApiRequestException if the metric dictionary returns a null or if the apiMetricQuery is invalid.
     */
    public LinkedHashSet<LogicalMetric> generateLogicalMetrics(
            String apiMetricQuery,
            MetricDictionary metricDictionary
    ) throws BadApiRequestException {
        try (TimedPhase timer = RequestLog.startTiming("GeneratingLogicalMetrics")) {
            LOG.trace("Metric dictionary: {}", metricDictionary);

            if (apiMetricQuery == null || "".equals(apiMetricQuery)) {
                return new LinkedHashSet<>();
            }

            // set of logical metric objects
            LinkedHashSet<LogicalMetric> generated = new LinkedHashSet<>();
            List<String> invalidMetricNames = new ArrayList<>();

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

            if (!invalidMetricNames.isEmpty()) {
                LOG.debug(METRICS_UNDEFINED.logFormat(invalidMetricNames.toString()));
                throw new BadApiRequestException(METRICS_UNDEFINED.format(invalidMetricNames.toString()));
            }
            LOG.trace("Generated set of logical metric: {}", generated);
            return generated;
        }
    }
}
