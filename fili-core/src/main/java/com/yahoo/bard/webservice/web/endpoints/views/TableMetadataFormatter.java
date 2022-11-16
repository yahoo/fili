// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints.views;

import static com.yahoo.bard.webservice.web.endpoints.views.DefaultMetadataViewFormatters.dimensionMetadataFormatter;
import static com.yahoo.bard.webservice.web.endpoints.views.DefaultMetadataViewFormatters.metricMetadataFormatter;

import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.util.TableUtils;
import com.yahoo.bard.webservice.web.MetadataObject;
import com.yahoo.bard.webservice.web.endpoints.TablesServlet;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.stream.Collectors;

import javax.ws.rs.core.UriInfo;

/**
 * Standard formatting methods of table metadata for serialization.
 */
public class TableMetadataFormatter {

    public static TableMetadataFormatter INSTANCE = new TableMetadataFormatter();

    /**
     * Get a collection of table metadata.
     *
     * @param logicalTables  Collection of logical tables
     * @param uriInfo  Uri information to construct the uri's
     *
     * @return List of table views which contains full vew of each table
     */
    public LinkedHashSet<MetadataObject> formatTables(Collection<LogicalTable> logicalTables, final UriInfo uriInfo) {
        return logicalTables.stream()
                .map(logicalTable -> formatTableSummary(logicalTable, uriInfo))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Get a representation of a table, possibly joining to other metadata entities.
     *
     * @param logicalTable  Logical Table
     * @param uriInfo  Uri information to construct the uri's
     *
     * @return Table which contains complete view
     */
    public MetadataObject formatTable(LogicalTable logicalTable, UriInfo uriInfo) {
        MetadataObject resultRow = new MetadataObject();
        resultRow.put("category", logicalTable.getCategory());
        resultRow.put("name", logicalTable.getName());
        resultRow.put("longName", logicalTable.getLongName());
        resultRow.put("retention", logicalTable.getRetention() != null ? logicalTable.getRetention().toString() : "");
        resultRow.put("granularity", logicalTable.getGranularity().getName());
        resultRow.put("description", logicalTable.getDescription());
        resultRow.put(
                "dimensions",
                dimensionMetadataFormatter.formatDimensionSummaryList(logicalTable.getDimensions(), uriInfo)
        );
        resultRow.put(
                "metrics",
                metricMetadataFormatter.formatMetricSummaryList(logicalTable.getLogicalMetrics(), uriInfo)
        );
        resultRow.put(
                "availableIntervals",
                TableUtils.logicalTableAvailability(logicalTable)
        );
        return resultRow;
    }

    /**
     * Get a representation of a table, possibly joining to other metadata entities.
     *
     * @param logicalTable  Logical Table
     * @param uriInfo Uri information to construct the uri's
     *
     * @return Table details with all the metrics and dimension details for given grain
     */
    public MetadataObject formatTableSummary(LogicalTable logicalTable, UriInfo uriInfo) {
        MetadataObject resultRow = new MetadataObject();
        resultRow.put("category", logicalTable.getCategory());
        resultRow.put("name", logicalTable.getName());
        resultRow.put("longName", logicalTable.getLongName());
        resultRow.put("granularity", logicalTable.getGranularity().getName());
        resultRow.put("uri", TablesServlet.getLogicalTableUrl(logicalTable, uriInfo));
        return resultRow;
    }
}
