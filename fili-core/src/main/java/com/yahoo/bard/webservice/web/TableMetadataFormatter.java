// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.application.metadataViews.MetadataViewProvider;
import com.yahoo.bard.webservice.table.LogicalTable;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.container.ContainerRequestContext;

/**
 * Interface for transforming logical tables into views that are grain-aware.
 */
public interface TableMetadataFormatter {

    /**
     * Get a list of TableViews which have a complete view of all the tables and underlying information.
     *
     * @param logicalTableList  List of logical tables
     * @param containerRequestContext  Uri information to construct the uri's
     *
     * @return List of table views which contains full vew of each table
     */
    List<TableView> formatTables(Set<LogicalTable> logicalTableList, ContainerRequestContext containerRequestContext, Map<String, MetadataViewProvider<?>> metadataBuilders);

    /**
     * Get a representation of a table and underlying information.
     *
     * @param logicalTable  Logical Table
     * @param uriInfo  Uri information to construct the uri's
     *
     * @return Table which contains complete view
     */
    TableView formatTable(LogicalTable logicalTable, ContainerRequestContext containerRequestContext, MetadataViewProvider<LogicalTable> tableMetadataViewProvider);

    /**
     * Get a representation of a table at grain level.
     *
     * @param logicalTable  Logical Table
     * @param grain  Table grain
     * @param containerRequestContext Uri information to construct the uri's
     *
     * @return Table details with all the metrics and dimension details for given grain
     */
    TableGrainView formatTableGrain(LogicalTable logicalTable, String grain, ContainerRequestContext
            containerRequestContext);
}
