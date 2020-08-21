// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints.views;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.web.MetadataObject;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.ws.rs.core.UriInfo;

/**
 * Class to support custom endpoint of the tables with complete view of each table grains and associated metrics
 * &amp; dimensions.
 */
public class TableFullViewFormatter extends TableMetadataFormatter {
    public static TableFullViewFormatter INSTANCE = new TableFullViewFormatter();

    /**
     * Method to provide full view of the tables which includes grains, metrics and dimensions.
     *
     * @param logicalTables  Set of logical tables
     * @param uriInfo  Uri information to construct the uri's
     *
     * @return List of table details with all the associated meta info
     */
    public LinkedHashSet<MetadataObject> formatTables(Collection<LogicalTable> logicalTables, UriInfo uriInfo) {

        //Map to keep meta info of the logical table
        Map<String, MetadataObject> tablesMeta = new HashMap<>();
        //Map to keep list of time grain details for the logical table
        Map<String, List<MetadataObject>> nameToGrainView = new HashMap<>();

        for (LogicalTable logicalTable : logicalTables) {
            //An array list to store grain level definition of given logical table
            List<MetadataObject> grains = nameToGrainView
                    .computeIfAbsent(logicalTable.getName(), (ignore) -> new ArrayList<>());

            grains.add(tableSchemaForGrain(logicalTable, uriInfo));

            tablesMeta.computeIfAbsent(logicalTable.getName(), k -> rollupGroupHeader(logicalTable, uriInfo));
        }

        LinkedHashSet<MetadataObject> metadataCollectionList = new LinkedHashSet<>();

        Set<Map.Entry<String, MetadataObject>> entrySet = tablesMeta.entrySet();
        for (Map.Entry<String, MetadataObject> entry : entrySet) {
            MetadataObject metadataCollection = new MetadataObject(entry.getValue());
            metadataCollection.put("timeGrains", nameToGrainView.get(entry.getKey()));
            metadataCollectionList.add(metadataCollection);
        }

        return metadataCollectionList;
    }

    /**
     * Get a representation of a table, possibly joining to other metadata entities.
     *
     * @param logicalTable  Logical Table
     * @param uriInfo  Uri information to construct the uri's
     *
     * @return Table which contains complete view
     */
    public MetadataObject formatTable(final LogicalTable logicalTable, final UriInfo uriInfo) {
        return tableSchemaForGrain(logicalTable, uriInfo);
    }

    /**
     * Method to provide metadata of the table.
     *
     * @param logicalTable  Logical Table Ex: Network, SpaceId
     * @param uriInfo  Uri information to construct the uri's
     *
     * @return  Meta data details of the given table
     */
    protected MetadataObject rollupGroupHeader(LogicalTable logicalTable, UriInfo uriInfo) {
        MetadataObject resultRow = new MetadataObject();
        resultRow.put("name", logicalTable.getName());
        resultRow.put("longName", logicalTable.getLongName());
        resultRow.put("description", logicalTable.getDescription());
        resultRow.put("category", logicalTable.getCategory());
        return resultRow;
    }

    /**
     * Method to provide grain level details(with metrics and dimensions) of the given logical table.
     *
     * @param logicalTable  Logical Table at the grain level
     * @param uriInfo Uri information to construct the uri's
     *
     * @return logical table details at grain level with all the associated meta data
     */
    protected MetadataObject tableSchemaForGrain(LogicalTable logicalTable, UriInfo uriInfo) {
        String grain = logicalTable.getGranularity().getName();
        MetadataObject resultRow = new MetadataObject();
        resultRow.put("name", logicalTable.getGranularity().toString());
        resultRow.put("longName", StringUtils.capitalize(logicalTable.getGranularity().getName()));
        resultRow.put("description", "The " + logicalTable.getName() + " " + grain + " grain");
        resultRow.put("retention", logicalTable.getRetention() != null ? logicalTable.getRetention().toString() : "");
        resultRow.put("dimensions", formatDimensionRollup(logicalTable.getDimensions()));
        resultRow.put("metrics", formatMetricRollup(logicalTable.getLogicalMetrics()));
        return resultRow;
    }

    /**
     * Get the summary list view of the dimensions.
     *
     * @param dimensions  Collection of dimensions to get the summary view for
     *
     * @return Summary list view of the dimensions
     */
    protected Set<Map<String, Object>> formatDimensionRollup(Collection<Dimension> dimensions) {
        return dimensions.stream()
                .map(dimension -> {
                    Map<String, Object> resultRow = new LinkedHashMap<>();
                    resultRow.put("category", dimension.getCategory());
                    resultRow.put("name", dimension.getApiName());
                    resultRow.put("longName", dimension.getLongName());
                    resultRow.put("cardinality", dimension.getCardinality());
                    resultRow.put("fields", dimension.getDimensionFields());
                    resultRow.put("storageStrategy", dimension.getStorageStrategy());
                    return resultRow;
                })
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }
    /**
     * Get the summary of the logical metrics used in the full view format.
     * Unlike the regular summary, rollup doesn't want URIs
     *
     * @param logicalMetrics  Collection of logical metrics to get the rollup view for
     *
     * @return Summary view of the logical metric
     */
    protected Set<Map<String, String>> formatMetricRollup(
            Collection<LogicalMetric> logicalMetrics
    ) {
        return logicalMetrics.stream()
                .map(logicalMetric -> {
                    Map<String, String> resultRow = new LinkedHashMap<>();
                    resultRow.put("category", logicalMetric.getCategory());
                    resultRow.put("name", logicalMetric.getName());
                    resultRow.put("longName", logicalMetric.getLongName());
                    resultRow.put("type", logicalMetric.getType());
                    return resultRow;
                }).collect(Collectors.toCollection(LinkedHashSet::new));
    }
}
