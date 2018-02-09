// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application.metadataViews;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.web.endpoints.MetricsServlet;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.ws.rs.container.ContainerRequestContext;

public class LogicalMetricSummaryViewProvider implements MetadataViewProvider<LogicalMetric> {

    @Override
    public Object apply(ContainerRequestContext containerRequestContext, LogicalMetric logicalMetric) {
        Map<String, String> resultRow = new LinkedHashMap<>();
        resultRow.put("category", logicalMetric.getCategory());
        resultRow.put("name", logicalMetric.getName());
        resultRow.put("longName", logicalMetric.getLongName());
        resultRow.put("type", logicalMetric.getType());
        resultRow.put("uri", MetricsServlet.getLogicalMetricUrl(logicalMetric, containerRequestContext.getUriInfo()));
        return resultRow;
    }
}
