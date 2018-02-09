// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application.metadataViews;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.ws.rs.container.ContainerRequestContext;

public class TableFullViewLogicalMetricViewProvider implements MetadataViewProvider<LogicalMetric> {

    // TODO implement full view
    @Override
    public Object apply(ContainerRequestContext containerRequestContext, LogicalMetric logicalMetric) {
        Map<String, Object> resultRow = new LinkedHashMap<>();
        resultRow.put("category", logicalMetric.getCategory());
        resultRow.put("name", logicalMetric.getName());
        resultRow.put("longName", logicalMetric.getLongName());
        resultRow.put("description", logicalMetric.getDescription());
        return resultRow;
    }
}
