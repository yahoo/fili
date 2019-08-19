// Copyright 2018 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.binders;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.METRICS_NOT_IN_TABLE;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.util.Incubating;
import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Generates an ordered set of logical metrics based on a String representing the metrics requested by the user.
 */
@Incubating
public interface LogicalMetricGenerator {

    /**
     * Given a single dimension filter string, generate a metric name extension.
     *
     * @param filterString  Single dimension filter string.
     *
     * @return Metric name extension created for the filter.
     */
    static String generateMetricName(String filterString) {
        return filterString.replace("|", "_").replace("-", "_").replace(",", "_").replace("]", "").replace("[", "_");
    }

    /**
     * Extracts the list of metrics from the url metric query string and generates a set of LogicalMetrics.
     * <p>
     * If the query contains undefined metrics, {@link com.yahoo.bard.webservice.web.BadApiRequestException} will be
     * thrown.
     *
     * @param apiMetricQuery  URL query string containing the metrics separated by ','
     * @param metricDictionary  Metric dictionary contains the map of valid metric names and logical metric objects
     *
     * @return set of metric objects
     */
    LinkedHashSet<LogicalMetric> generateLogicalMetrics(String apiMetricQuery, MetricDictionary metricDictionary);

    /**
     * Validate that all metrics are part of the logical table.
     *
     * @param logicalMetrics  The set of metrics being validated
     * @param table  The logical table for the request
     *
     * @throws BadApiRequestException if the requested metrics are not in the logical table
     */
    void validateMetrics(Set<LogicalMetric> logicalMetrics, LogicalTable table) throws BadApiRequestException;

    /**
     * Default implementation of this interface.
     */
    LogicalMetricGenerator DEFAULT_LOGICAL_METRIC_GENERATOR =
            new LogicalMetricGenerator() {
                @Override
                public LinkedHashSet<LogicalMetric> generateLogicalMetrics(
                        final String apiMetricQuery, final MetricDictionary metricDictionary
                ) {
                    LinkedHashSet<LogicalMetric> metrics = new LinkedHashSet<>();
                    List<String> invalidMetricNames = new ArrayList<>();

                    String[] parsedMetrics = apiMetricQuery.split(",");
                    if (parsedMetrics.length == 1 && parsedMetrics[0].isEmpty()) {
                        parsedMetrics = new String[0];
                    }

                    // TODO extract into checkInvalidMetricNames method
                    for (String metricName : parsedMetrics) {
                        LogicalMetric logicalMetric = metricDictionary.get(metricName);
                        if (logicalMetric == null) {
                            invalidMetricNames.add(metricName);
                        } else {
                            metrics.add(logicalMetric);
                        }
                    }
                    if (!invalidMetricNames.isEmpty()) {
                        String message = ErrorMessageFormat.METRICS_UNDEFINED.logFormat(invalidMetricNames);
                        throw new BadApiRequestException(message);
                    }
                    return metrics;
                }

                @Override
                public void validateMetrics(final Set<LogicalMetric> logicalMetrics, final LogicalTable table)
                        throws BadApiRequestException {
                    //get metric names from the logical table
                    Set<String> validMetricNames = table.getLogicalMetrics().stream()
                            .map(LogicalMetric::getName)
                            .collect(Collectors.toSet());

                    //get metric names from logicalMetrics and remove all the valid metrics
                    Set<String> invalidMetricNames = logicalMetrics.stream()
                            .map(LogicalMetric::getName)
                            .filter(it -> !validMetricNames.contains(it))
                            .collect(Collectors.toSet());

                    //requested metrics names are not present in the logical table metric names set
                    if (!invalidMetricNames.isEmpty()) {
                        throw new BadApiRequestException(
                                METRICS_NOT_IN_TABLE.format(invalidMetricNames, table.getName())
                        );
                    }
                }
            };
}
