// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.web.apirequest.generator.having;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.HAVING_METRICS_NOT_IN_QUERY_FORMAT;

import com.yahoo.bard.webservice.data.config.ConfigurationLoader;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.TimedPhase;
import com.yahoo.bard.webservice.web.ApiHaving;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadHavingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Generates having objects based on the having query in the api request.
 */
public class DefaultHavingApiGenerator implements HavingGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultHavingApiGenerator.class);
    private static final String COMMA_AFTER_BRACKET_PATTERN = "(?<=]),";

    private final MetricDictionary metricDictionary;

    /**
     * Constructor.
     * <p>
     * Uses the globally scoped set of metrics to resolve having clauses.
     *
     * @param loader  Configuration loader that connects resource dictionaries with the loader.
     */
    public DefaultHavingApiGenerator(ConfigurationLoader loader) {
        this(loader.getMetricDictionary());
    }

    /**
     * Constructor.
     *
     * @param metricDictionary  Dictionary of metrics for generating APIs
     */
    public DefaultHavingApiGenerator(MetricDictionary metricDictionary) {
        this.metricDictionary = metricDictionary;
    }

    /**
     * Generates having objects based on the having query in the api request.
     *
     * @param havingQuery  Expects a URL having query String in the format:
     * (dimension name)-(operation)[(value or comma separated values)]?
     * @param logicalMetrics  The logical metrics used in this query
     * @return Set of having objects.
     *
     * @throws BadApiRequestException if the having query string does not match required syntax.
     */
    @Override
    public Map<LogicalMetric, Set<ApiHaving>> apply(
        String havingQuery,
        Set<LogicalMetric> logicalMetrics
    ) throws BadApiRequestException {
        try (TimedPhase phase = RequestLog.startTiming("GeneratingHavings")) {
            // Havings are optional hence check if havings are requested.
            if (havingQuery == null || "".equals(havingQuery)) {
                return Collections.emptyMap();
            }

            List<String> unmatchedMetrics = new ArrayList<>();

            // split on '],' to get list of havings
            List<String> apiHavings = Arrays.asList(havingQuery.split(COMMA_AFTER_BRACKET_PATTERN));
            Map<LogicalMetric, Set<ApiHaving>> generated = new LinkedHashMap<>();
            for (String apiHaving : apiHavings) {
                try {
                    ApiHaving newHaving = new ApiHaving(apiHaving, metricDictionary);
                    LogicalMetric metric = newHaving.getMetric();
                    if (!logicalMetrics.contains(metric)) {
                        unmatchedMetrics.add(metric.getName());
                    } else {
                        generated.putIfAbsent(metric, new LinkedHashSet<>());
                        generated.get(metric).add(newHaving);
                    }
                } catch (BadHavingException havingException) {
                    throw new BadApiRequestException(havingException.getMessage(), havingException);
                }
            }

            if (!unmatchedMetrics.isEmpty()) {
                List<String> metricNames = logicalMetrics.stream()
                        .map(LogicalMetric::getName)
                        .collect(Collectors.toList());
                LOG.debug(HAVING_METRICS_NOT_IN_QUERY_FORMAT.logFormat(unmatchedMetrics.toString(), metricNames));
                throw new BadApiRequestException(
                    HAVING_METRICS_NOT_IN_QUERY_FORMAT.format(unmatchedMetrics.toString(), metricNames)
                );

            }

            LOG.trace("Generated map of havings: {}", generated);

            return generated;
        }
    }

    /**
     * Rebuild this having generator with a custom metric dictionary.
     *
     * @param metricDictionary  An alternate dictionary.
     *
     * @return  A replacement metric dictionary
     */
    public HavingGenerator withMetricDictionary(MetricDictionary metricDictionary) {
        return new DefaultHavingApiGenerator(metricDictionary);
    }
}
