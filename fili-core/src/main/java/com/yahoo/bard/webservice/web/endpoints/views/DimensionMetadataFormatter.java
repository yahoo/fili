// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints.views;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.web.MetadataObject;
import com.yahoo.bard.webservice.web.endpoints.DimensionsServlet;

import java.util.LinkedHashSet;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.ws.rs.core.UriInfo;

/**
 * Standard formatting methods of dimension metadata for serialization.
 */
public class DimensionMetadataFormatter {

    public static DimensionMetadataFormatter INSTANCE = new DimensionMetadataFormatter();

    /**
     * Get the summary list view of the dimensions.
     *
     * @param dimensions  Collection of dimensions to get the summary view for
     * @param uriInfo  UriInfo of the request
     *
     * @return Summary list view of the dimensions
     */
    public LinkedHashSet<MetadataObject> formatDimensionSummaryList(
            Iterable<Dimension> dimensions,
            final UriInfo uriInfo
    ) {
        return StreamSupport.stream(dimensions.spliterator(), false)
                .map(dimension -> formatDimensionSummary(dimension, uriInfo))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Get the summary view of the dimension.
     *
     * @param dimension  Dimension to get the view of
     * @param uriInfo  UriInfo of the request
     *
     * @return Summary view of the dimension
     */
    public MetadataObject formatDimensionSummary(Dimension dimension, final UriInfo uriInfo) {
        MetadataObject resultRow = new MetadataObject();
        resultRow.put("category", dimension.getCategory());
        resultRow.put("name", dimension.getApiName());
        resultRow.put("longName", dimension.getLongName());
        resultRow.put("uri", DimensionsServlet.getDimensionUrl(dimension, uriInfo));
        resultRow.put("cardinality", dimension.getCardinality());
        resultRow.put("storageStrategy", dimension.getStorageStrategy());
        return resultRow;
    }

    /**
     * Get the full view of the dimension.
     *
     * @param dimension  Dimension to get the view of
     * @param logicalTableDictionary  Logical Table Dictionary to look up the logical tables this dimension is on
     * @param uriInfo  UriInfo of the request
     *
     * @return Full view of the dimension
     */
    public MetadataObject formatDimensionWithJoins(
            Dimension dimension,
            LogicalTableDictionary logicalTableDictionary,
            final UriInfo uriInfo
    ) {
        MetadataObject resultRow = new MetadataObject();
        resultRow.put("category", dimension.getCategory());
        resultRow.put("name", dimension.getApiName());
        resultRow.put("longName", dimension.getLongName());
        resultRow.put("description", dimension.getDescription());
        resultRow.put("fields", dimension.getDimensionFields());
        resultRow.put("values", DimensionsServlet.getDimensionValuesUrl(dimension, uriInfo));
        resultRow.put("cardinality", dimension.getCardinality());
        resultRow.put("storageStrategy", dimension.getStorageStrategy());
        resultRow.put(
                "tables",
                DefaultMetadataViewFormatters.tableMetadataFormatter.formatTables(
                        logicalTableDictionary.findByDimension(dimension),
                        uriInfo
                )
        );
        return resultRow;
    }
}
