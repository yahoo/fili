// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.metric;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.METRICS_NOT_IN_TABLE;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.METRICS_NOT_IN_TABLE_WITH_VALID_GRAINS;
import static com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder.RequestResource.LOGICAL_METRICS;
import static com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder.RequestResource.LOGICAL_TABLE;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder;
import com.yahoo.bard.webservice.web.apirequest.RequestParameters;
import com.yahoo.bard.webservice.web.apirequest.generator.Generator;
import com.yahoo.bard.webservice.web.apirequest.generator.UnsatisfiedApiRequestConstraintsException;
import com.yahoo.bard.webservice.web.util.BardConfigResources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Default generator implementation for binding logical metrics. Binding logical metrics is dependent on the logical
 * table being queried. Ensure the logical table has been bound before using this class to generate logical metrics.
 */
public class DefaultLogicalMetricGenerator
        implements Generator<LinkedHashSet<LogicalMetric>>, ApiRequestLogicalMetricBinder {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultLogicalMetricGenerator.class);

    @Override
    public LinkedHashSet<LogicalMetric> bind(
            DataApiRequestBuilder builder,
            RequestParameters params,
            BardConfigResources resources
    ) {
        return generateLogicalMetrics(
                params.getLogicalMetrics().orElse(""),
                resources.getMetricDictionary()
        );
    }

    /**
     * Validates that the bound logical metrics are valid for the table being queried.
     *
     * Throws {@link UnsatisfiedApiRequestConstraintsException} if logical metrics are bound before the queried logical
     * table has been bound.
     *
     * @param entity  The resource constructed by the {@code bind}} method
     * @param builder  The builder object representing the in progress DataApiRequest
     * @param params  The request parameters sent by the client
     * @param resources  Resources used to build the request
     *
     *
     */
    @Override
    public void validate(
            LinkedHashSet<LogicalMetric> entity,
            DataApiRequestBuilder builder,
            RequestParameters params,
            BardConfigResources resources
    ) {
        if (!builder.isLogicalTableInitialized()) {
            throw new UnsatisfiedApiRequestConstraintsException(
                    LOGICAL_METRICS.getResourceName(),
                    Collections.singleton(LOGICAL_TABLE.getResourceName())
            );
        }

        validateMetrics(entity, builder.getLogicalTableIfInitialized()
                .orElseThrow(() -> new BadApiRequestException("A logical table is required for all data queries")),
                resources.getLogicalTableDictionary());
    }


    @Override
    public LinkedHashSet<LogicalMetric> bind(
            DataApiRequest request,
            String parameter,
            ResourceDictionaries dictionaries
    ) {
        return generateLogicalMetrics(
                Optional.ofNullable(parameter).orElse(""),
                dictionaries.getMetricDictionary()
        );

    }

    @Override
    public void validate(
            LinkedHashSet<LogicalMetric> entity,
            DataApiRequest request,
            String parameter,
            ResourceDictionaries dictionaries
    ) {
        validateMetrics(entity, request.getTable());
    }
    /**
     * Extracts the list of metrics from the url metric query string and generates a set of LogicalMetrics.
     * <p>
     * If the query contains undefined metrics, {@link BadApiRequestException} will be
     * thrown.
     *
     * This method is meant for backwards compatibility. If you do not need to use this method for that reason please
     * prefer using a generator instance instead.
     *
     * @param apiMetricQuery  URL query string containing the metrics separated by ','
     * @param metricDictionary  Metric dictionary contains the map of valid metric names and logical metric objects
     *
     * @return set of metric objects
     */
    @Override
    public LinkedHashSet<LogicalMetric> generateLogicalMetrics(
            String apiMetricQuery,
            MetricDictionary metricDictionary
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
            LOG.error(message);
            throw new BadApiRequestException(message);
        }
        return metrics;
    }

    /**
     * Extracts the list of metrics from the url metric query string and generates a set of LogicalMetrics with
     * granularity.
     * <p>
     * If the query contains undefined metrics, {@link BadApiRequestException} will be
     * thrown.
     *
     * This method is meant for backwards compatibility. If you do not need to use this method for that reason please
     * prefer using a generator instance instead.
     *
     * @param apiMetricQuery  URL query string containing the metrics separated by ','
     * @param requestGranularity Granularity of the request
     * @param metricDictionary  Metric dictionary contains the map of valid metric names and logical metric objects
     *
     * @return set of metric objects
     */
    @Override
    public LinkedHashSet<LogicalMetric> generateLogicalMetrics(
            String apiMetricQuery,
            Granularity requestGranularity,
            MetricDictionary metricDictionary
    ) {
        return generateLogicalMetrics(apiMetricQuery, metricDictionary);
    }

    /**
     * Validate that all metrics are part of the logical table.
     *
     * This method is meant for backwards compatibility. If you do not need to use this method for that reason please
     * prefer using a generator instance instead.
     *
     * @param logicalMetrics  The set of metrics being validated
     * @param table  The logical table for the request
     *
     */
    @Override
    public void validateMetrics(
            Set<LogicalMetric> logicalMetrics,
            LogicalTable table,
            LogicalTableDictionary logicalTableDictionary
    ) throws BadApiRequestException {
        //get metric names from the logical table
        Set<String> validMetricNames = table.getLogicalMetrics().stream()
                .map(LogicalMetric::getName)
                .collect(Collectors.toSet());

        //get metric names from logicalMetrics and remove all the valid metrics
        Set<LogicalMetric> invalidMetrics = logicalMetrics.stream()
                .filter(it -> !validMetricNames.contains(it.getName()))
                .collect(Collectors.toSet());

        Map<LogicalMetric, Set<String>> invaldMetricGrainMap =
                createValidGrainMap(invalidMetrics, logicalTableDictionary);

        //requested metrics names are not present in the logical table metric names set
        if (!invalidMetrics.isEmpty()) {
            Set<String> invalidMetricNames = invalidMetrics.stream()
                    .map(LogicalMetric::getName)
                    .collect(Collectors.toSet());
            errorMessagingForInvalidGrain(invalidMetricNames, invaldMetricGrainMap, table, logicalTableDictionary);
        }
    }

    /**
     * Create a map of metrics with their corresponding valid grains.
     *
     * @param invalidMetrics  The set of metrics for which valid grain map is being generated
     * @param logicalTableDictionary  The logical table dictionary
     *
     *
     * @return map of metrics and their valid grains
     */
    public Map<LogicalMetric, Set<String>> createValidGrainMap(
            Set<LogicalMetric> invalidMetrics,
            LogicalTableDictionary logicalTableDictionary
    ) {
        Map<LogicalMetric, Set<String>> invaldMetricGrainMap = new HashMap<>();
        if (logicalTableDictionary != null) {
            for (LogicalMetric invalidMetric : invalidMetrics) {
                Set<LogicalTable> validLogicalTables = logicalTableDictionary
                        .findByLogicalMetricName(invalidMetric.getName());
                Set<String> validGrainsSet = validLogicalTables.stream()
                        .map(val -> val.getGranularity().getName())
                        .collect(Collectors.toSet());
                invaldMetricGrainMap.put(invalidMetric, validGrainsSet);
            }
        }
        return invaldMetricGrainMap;
    }

    /**
     * Error messaging for invalid grain on requested metrics.
     *
     * @param invalidMetricNames  The set of metrics that have invalid grains
     * @param invaldMetricGrainMap  The map of invalid metrics and their corresponding valid grains
     * @param table  The logical table for the request
     * @param logicalTableDictionary  The logical table dictionary
     *
     *
     * @throws BadApiRequestException if the requested metrics are not in the logical table
     */
    public void errorMessagingForInvalidGrain(
            Set<String> invalidMetricNames,
            Map<LogicalMetric, Set<String>> invaldMetricGrainMap,
            LogicalTable table,
            LogicalTableDictionary logicalTableDictionary
    ) throws BadApiRequestException {
        if (logicalTableDictionary != null) {
            Set<String> grainSet = new HashSet<>();
            for (Set<String> set : invaldMetricGrainMap.values()) {
                grainSet.addAll(set);
            }
            List<String> grainList = new ArrayList<>(grainSet);
            Collections.sort(grainList);
            LOG.debug(METRICS_NOT_IN_TABLE_WITH_VALID_GRAINS.logFormat(invalidMetricNames, table.getName(),
                    table.getGranularity(), grainList));
            throw new BadApiRequestException(
                    METRICS_NOT_IN_TABLE_WITH_VALID_GRAINS.format(invalidMetricNames, table.getName(),
                            table.getGranularity(), grainList)
            );
        } else {
            LOG.debug(METRICS_NOT_IN_TABLE.logFormat(invalidMetricNames, table.getName(), table.getGranularity()));
            throw new BadApiRequestException(
                    METRICS_NOT_IN_TABLE.format(invalidMetricNames, table.getName(), table.getGranularity())
            );
        }
    }
}
