// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application.metadataViews;

import static com.yahoo.bard.webservice.web.endpoints.TablesServlet.getLogicalTableUrl;

import com.yahoo.bard.webservice.table.LogicalTable;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.ws.rs.container.ContainerRequestContext;

public class TableSummaryViewProvider implements MetadataViewProvider<LogicalTable> {

    @Override
    public Object apply(ContainerRequestContext containerRequestContext, LogicalTable logicalTable) {
        Map<String, String> resultRow = new LinkedHashMap<>();
        resultRow.put("category", logicalTable.getCategory());
        resultRow.put("name", logicalTable.getName());
        resultRow.put("longName", logicalTable.getLongName());
        resultRow.put("granularity", logicalTable.getGranularity().getName());
        resultRow.put("uri", getLogicalTableUrl(logicalTable, containerRequestContext.getUriInfo()));
        return resultRow;
    }
}
