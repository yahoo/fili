// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.mapimpl;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.METRICS_UNDEFINED;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.web.apirequest.MetricsApiRequest;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException;
import com.yahoo.bard.webservice.web.apirequest.exceptions.MissingResourceApiRequestException;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MetricsApiRequestMapImpl extends ApiRequestMapImpl implements MetricsApiRequest {

    public static final String METRIC_DICTIONARY = "metricDictionary";
    public static final String METRICS = "metrics";

    static Function<String, LinkedHashSet<LogicalMetric>> buildFactory(MetricDictionary metricDictionary) {
        return (String metricName) ->
            metricDictionary.values().stream()
                .filter(logicalMetric -> metricName == null || metricName.equals(logicalMetric.getName()))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Constructor.
     *
     * @param requestParams  Parameters from the request.
     * @param metricDictionary  Dictionary to hold the metrics
     */
    public MetricsApiRequestMapImpl(
            final Map<String, String> requestParams,
            MetricDictionary metricDictionary
    ) {
        super(requestParams, Collections.emptyMap(), Collections.emptyMap());
        putResource(METRIC_DICTIONARY, metricDictionary);
        putBinder(METRICS, buildFactory(metricDictionary));
    }

    /**
     * Copy Constructor.
     *
     * @param requestParams  Parameters from the request.
     * @param resources  Resources used for the request.
     * @param binders Binders used for the request.
     * @param boundMetrics Bound metrics object.
     */
    private MetricsApiRequestMapImpl(
            Map<String, String> requestParams,
            Map<String, Object> resources,
            Map<String, Object> binders,
            Collection<LogicalMetric> boundMetrics
    ) {
        super(requestParams, resources, binders);
        putBoundObject(METRICS, boundMetrics);
    }

    /**
     * Binds and validates logical metrics.
     *
     * @return Set of metric objects.
     * @throws BadApiRequestException if an invalid metric is requested or the metric dictionary is empty.
     */
    protected LinkedHashSet<LogicalMetric> bindAndValidateMetrics()
            throws BadApiRequestException {
        String requestParameter = getRequestParameter(METRICS);
        LinkedHashSet<LogicalMetric> logicalMetrics =
                (LinkedHashSet<LogicalMetric>) bindAndGetNonOptionalProperty(METRICS);

        if (logicalMetrics.isEmpty()) {
            String msg = METRICS_UNDEFINED.logFormat(requestParameter);
            LOG.error(msg);
            throw new MissingResourceApiRequestException(msg);
        }

        LOG.trace("Generated set of metrics: {}", logicalMetrics);
        return logicalMetrics;
    }

    @Override
    public LinkedHashSet<LogicalMetric> getMetrics() {
        return bindAndValidateMetrics();
    }

    @Override
    public LogicalMetric getMetric() {
        return getMetrics().stream().findFirst().orElse(null);
    }

    @Override
    public MetricsApiRequest withMetrics(LinkedHashSet<LogicalMetric> metrics) {
        return new MetricsApiRequestMapImpl(requestParams, resources, binders, (List<LogicalMetric>) metrics);
    }
}
