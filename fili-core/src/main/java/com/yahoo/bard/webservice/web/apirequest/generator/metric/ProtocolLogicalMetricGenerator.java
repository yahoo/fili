// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.metric;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.METRICS_NOT_IN_TABLE;
import static com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder.RequestResource.LOGICAL_METRICS;
import static com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder.RequestResource.LOGICAL_TABLE;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.metric.protocol.DefaultSystemMetricProtocols;
import com.yahoo.bard.webservice.data.metric.protocol.GeneratedMetricInfo;
import com.yahoo.bard.webservice.data.metric.protocol.Protocol;
import com.yahoo.bard.webservice.data.metric.protocol.ProtocolDictionary;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder;
import com.yahoo.bard.webservice.web.apirequest.RequestParameters;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException;
import com.yahoo.bard.webservice.web.apirequest.generator.Generator;
import com.yahoo.bard.webservice.web.apirequest.generator.UnsatisfiedApiRequestConstraintsException;
import com.yahoo.bard.webservice.web.apirequest.generator.metric.antlr.ProtocolAntlrApiMetricParser;
import com.yahoo.bard.webservice.web.apirequest.generator.metric.protocol.ProtocolChain;
import com.yahoo.bard.webservice.web.apirequest.metrics.ApiMetric;
import com.yahoo.bard.webservice.web.apirequest.metrics.ApiMetricAnnotater;
import com.yahoo.bard.webservice.web.util.BardConfigResources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Default generator implementation for binding protocol logical metrics.
 * Protocol Metrics are parsed, using a grammar from an apirequest string and a set of parameters.
 * The base metric name is retrieved from the metric dictionary and then the parameters are used to apply zero or more
 * transformations.  The resulting protocol metric is validated as having a baseMetric on the logical table.
 */
public class ProtocolLogicalMetricGenerator
        implements Generator<LinkedHashSet<LogicalMetric>>, ApiRequestLogicalMetricBinder {
    private static final Logger LOG = LoggerFactory.getLogger(ProtocolLogicalMetricGenerator.class);

    private final ApiMetricAnnotater apiMetricAnnotater;
    private final ApiMetricParser apiMetricParser;
    private final ProtocolChain protocolChain;

    /**
     * Constructor.
     *
     * @param apiMetricAnnotater  The metric annotator to apply
     * @param protocolNames  The list of protocols supported in the System
     */
    public ProtocolLogicalMetricGenerator(ApiMetricAnnotater apiMetricAnnotater, List<String> protocolNames) {
        this(
                apiMetricAnnotater,
                protocolNames,
                new ProtocolAntlrApiMetricParser(),
                DefaultSystemMetricProtocols.getDefaultProtocolDictionary()
        );
    }

    public ProtocolLogicalMetricGenerator(
            ApiMetricAnnotater apiMetricAnnotater,
            List<String> protocolNames,
            ApiMetricParser metricParser,
            ProtocolDictionary protocolDictionary
    ) {
        this.apiMetricAnnotater = apiMetricAnnotater;
        this.apiMetricParser = metricParser;
        LinkedHashSet<Protocol> protocols = protocolNames.stream()
                .map(protocolDictionary::get)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        this.protocolChain = new ProtocolChain(new ArrayList<>(protocols));
    }

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

        if (!builder.getLogicalTableIfInitialized().isPresent()) {
            throw new BadApiRequestException("A logical table is required for all data queries");
        }

        validateMetrics(entity, builder.getLogicalTableIfInitialized().get());
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
    public LinkedHashSet<LogicalMetric> generateLogicalMetrics(
            String apiMetricQuery,
            MetricDictionary metricDictionary
    ) {
        List<ApiMetric> apiMetrics = parseApiMetricQuery(apiMetricQuery);
        return applyProtocols(apiMetrics, metricDictionary);
    }

    private List<ApiMetric> parseApiMetricQuery(String apiMetricQuery) {
        return apiMetricParser.apply(apiMetricQuery)
                .stream()
                .map(apiMetricAnnotater)
                .collect(Collectors.toList());
    }

    private LinkedHashSet<LogicalMetric> applyProtocols(List<ApiMetric> apiMetrics, MetricDictionary metricDictionary) {
        LinkedHashSet<LogicalMetric> metrics = new LinkedHashSet<>();
        List<String> invalidMetricNames = new ArrayList<>();

        for (ApiMetric metric : apiMetrics) {
            GeneratedMetricInfo generatedMetricInfo = new GeneratedMetricInfo(
                    metric.getRawName(),
                    metric.getBaseApiMetricId()
            );

            LogicalMetric baseLogicalMetrics = metricDictionary.get(metric.getBaseApiMetricId());
            if (baseLogicalMetrics == null) {
                invalidMetricNames.add(metric.getRawName());
                continue;
            }
            LogicalMetric logicalMetric = protocolChain.applyProtocols(generatedMetricInfo, metric, baseLogicalMetrics);
            metrics.add(logicalMetric);
        }

        if (!invalidMetricNames.isEmpty()) {
            String message = ErrorMessageFormat.METRICS_UNDEFINED.logFormat(invalidMetricNames);
            LOG.error(message);
            throw new BadApiRequestException(message);
        }

        return metrics;
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
     * @throws BadApiRequestException if the requested metrics are not in the logical table
     */
    public void validateMetrics(Set<LogicalMetric> logicalMetrics, LogicalTable table)
            throws BadApiRequestException {
        //get metric names from the logical table
        Set<String> validMetricNames = table.getLogicalMetrics().stream()
                .map(LogicalMetric::getLogicalMetricInfo)
                .map(this::getBaseName)
                .collect(Collectors.toSet());

        //get metric names from logicalMetrics and remove all the valid metrics
        Set<String> invalidMetricNames = logicalMetrics.stream()
                .map(LogicalMetric::getLogicalMetricInfo)
                .map(this::getBaseName)
                .filter(it -> !validMetricNames.contains(it))
                .collect(Collectors.toSet());

        //requested metrics names are not present in the logical table metric names set
        if (!invalidMetricNames.isEmpty()) {
            LOG.debug(METRICS_NOT_IN_TABLE.logFormat(invalidMetricNames, table.getName()));
            throw new BadApiRequestException(
                    METRICS_NOT_IN_TABLE.format(invalidMetricNames, table.getName())
            );
        }
    }

    private String getBaseName(LogicalMetricInfo metricInfo) {
        return metricInfo instanceof GeneratedMetricInfo ? ((GeneratedMetricInfo) metricInfo).getBaseMetricName() :
                metricInfo
                .getName();
    }
}
