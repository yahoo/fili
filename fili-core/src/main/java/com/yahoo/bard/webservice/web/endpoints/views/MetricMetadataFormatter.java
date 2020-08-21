// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints.views;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.web.MetadataObject;
import com.yahoo.bard.webservice.web.endpoints.MetricsServlet;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

import javax.ws.rs.core.UriInfo;

/**
 * Methods to provide serialization of Metric Metadata.
 */
public class MetricMetadataFormatter {
    public static MetricMetadataFormatter INSTANCE = new MetricMetadataFormatter();

    /**
     * Get the summary list view of the logical metrics.
     *
     * @param logicalMetrics  Collection of logical metrics to get the summary view for
     * @param uriInfo  UriInfo of the request
     *
     * @return Summary list view of the logical metrics
     */
    public Set<MetadataObject> formatMetricSummaryList(
            Collection<LogicalMetric> logicalMetrics,
            UriInfo uriInfo
    ) {
        return logicalMetrics.stream()
                .map(logicalMetric -> formatMetricSummary(logicalMetric, uriInfo))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Get the summary view of the logical metric.
     *
     * @param logicalMetric  Logical metric to get the view of
     * @param uriInfo  UriInfo of the request
     *
     * @return Summary view of the logical metric
     */
    public MetadataObject formatMetricSummary(LogicalMetric logicalMetric, UriInfo uriInfo) {
        MetadataObject resultRow = new MetadataObject();
        resultRow.put("category", logicalMetric.getCategory());
        resultRow.put("name", logicalMetric.getName());
        resultRow.put("longName", logicalMetric.getLongName());
        resultRow.put("type", logicalMetric.getType());
        resultRow.put("uri", MetricsServlet.getLogicalMetricUrl(logicalMetric, uriInfo));
        return resultRow;
    }

    /**
     * Get the full view of the logical metric.
     *
     * @param logicalMetric  Logical metric to get the view of
     * @param logicalTableDictionary  Logical Table Dictionary to look up the logical tables this metric is on
     * @param uriInfo  UriInfo of the request
     *
     * @return Full view of the logical metric
     */
    public MetadataObject formatLogicalMetricWithJoins(
            LogicalMetric logicalMetric,
            LogicalTableDictionary logicalTableDictionary,
            UriInfo uriInfo
    ) {
        MetadataObject resultRow = new MetadataObject();
        resultRow.put("category", logicalMetric.getCategory());
        resultRow.put("name", logicalMetric.getName());
        resultRow.put("longName", logicalMetric.getLongName());
        resultRow.put("type", logicalMetric.getType());
        resultRow.put("description", logicalMetric.getDescription());
        resultRow.put(
                "tables",
                DefaultMetadataViewFormatters.tableMetadataFormatter.formatTables(
                        logicalTableDictionary.findByLogicalMetric(logicalMetric),
                        uriInfo
                )
        );
        return resultRow;
    }
}
