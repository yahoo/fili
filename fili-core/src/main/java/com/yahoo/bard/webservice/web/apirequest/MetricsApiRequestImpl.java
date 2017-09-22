// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.EMPTY_DICTIONARY;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.METRICS_UNDEFINED;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.ForTesting;
import com.yahoo.bard.webservice.web.MetricsApiRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;
import javax.ws.rs.core.UriInfo;

/**
 * Metrics API Request Implementation binds, validates, and models the parts of a request to the metrics endpoint.
 */
public class MetricsApiRequestImpl extends ApiRequestImpl implements MetricsApiRequest {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsApiRequestImpl.class);

    private final Set<LogicalMetric> metrics;

    /**
     * Parses the API request URL and generates the Api Request object.
     *
     * @param metricName  string corresponding to the metric name specified in the URL
     * <pre>{@code
     * ((field name and operation):((multiple values bounded by [])or(single value))))(followed by , or end of string)
     * }</pre>
     * @param format  response data format JSON or CSV. Default is JSON.
     * @param perPage  number of rows to display per page of results. If present in the original request,
     * must be a positive integer. If not present, must be the empty string.
     * @param page  desired page of results. If present in the original request, must be a positive
     * integer. If not present, must be the empty string.
     * @param metricDictionary  cache containing all the valid metric objects.
     * @param uriInfo  The URI of the request object.
     *
     * @throws BadApiRequestException is thrown in the following scenarios:
     * <ol>
     *     <li>Invalid logical metric in the API request.</li>
     *     <li>Pagination parameters in the API request that are not positive integers.</li>
     * </ol>
     */
    public MetricsApiRequestImpl(
            String metricName,
            String format,
            @NotNull String perPage,
            @NotNull String page,
            MetricDictionary metricDictionary,
            UriInfo uriInfo
    ) throws BadApiRequestException {
        super(format, perPage, page, uriInfo);

        this.metrics = generateMetrics(metricName, metricDictionary);

        LOG.debug(
                "Api request: \nMetrics: {},\nFormat: {}\nPagination: {}",
                this.metrics,
                this.format,
                this.paginationParameters
        );
    }

    /**
     * No argument constructor, meant to be used only for testing.
     *
     * @deprecated it's not a good practice to have testing code here. This constructor will be removed entirely.
     */
    @Deprecated
    @ForTesting
    protected MetricsApiRequestImpl() {
        super();
        this.metrics = null;
    }

    /**
     * Generates the set of all available metrics.
     *
     * @param metricName  string corresponding to the metric name specified in the URL
     * @param metricDictionary  metric dictionary that contains the map of valid metric names to metric objects.
     *
     * @return Set of metric objects.
     * @throws BadApiRequestException if an invalid metric is requested or the metric dictionary is empty.
     */
    protected Set<LogicalMetric> generateMetrics(String metricName, MetricDictionary metricDictionary)
            throws BadApiRequestException {
        Set<LogicalMetric> generated = metricDictionary.values().stream()
                .filter(logicalMetric -> metricName == null || metricName.equals(logicalMetric.getName()))
                .collect(Collectors.toCollection(LinkedHashSet::new));

        if (generated.isEmpty()) {
            String msg;
            if (metricDictionary.isEmpty()) {
                msg = EMPTY_DICTIONARY.logFormat("Metric");
            } else {
                msg = METRICS_UNDEFINED.logFormat(metricName);
            }
            LOG.error(msg);
            throw new BadApiRequestException(msg);
        }

        LOG.trace("Generated set of metrics: {}", generated);
        return generated;
    }

    @Override
    public Set<LogicalMetric> getMetrics() {
        return this.metrics;
    }

    @Override
    public LogicalMetric getMetric() {
        return this.metrics.iterator().next();
    }
}
