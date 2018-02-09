package com.yahoo.bard.webservice.application.metadataViews;

import static com.yahoo.bard.webservice.web.endpoints.DimensionsServlet.getDimensionValuesUrl;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.web.endpoints.TablesServlet;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.ws.rs.container.ContainerRequestContext;

public class DimensionFullViewProvider implements MetadataViewProvider<Dimension> {

    private final MetadataViewProvider<LogicalTable> tableMetadataViewProvider;
    private final LogicalTableDictionary logicalTableDictionary;

    public DimensionFullViewProvider(
            LogicalTableDictionary logicalTableDictionary,
            MetadataViewProvider<LogicalTable> tableMetadataViewProvider
    ) {
        this.tableMetadataViewProvider = tableMetadataViewProvider;
        this.logicalTableDictionary = logicalTableDictionary;
    }

    @Override
    public Object apply(
            ContainerRequestContext containerRequestContext,
            Dimension dimension
    ) {
        Map<String, Object> resultRow = new LinkedHashMap<>();
        resultRow.put("category", dimension.getCategory());
        resultRow.put("name", dimension.getApiName());
        resultRow.put("longName", dimension.getLongName());
        resultRow.put("description", dimension.getDescription());
        resultRow.put("fields", dimension.getDimensionFields());
        resultRow.put("values", getDimensionValuesUrl(dimension, containerRequestContext.getUriInfo()));
        resultRow.put("cardinality", dimension.getCardinality());
        resultRow.put("storageStrategy", dimension.getStorageStrategy());
        resultRow.put(
                "tables",
                TablesServlet.getLogicalTableListSummaryView(
                        logicalTableDictionary.findByDimension(dimension),
                        containerRequestContext,
                        tableMetadataViewProvider
                )
        );
        return resultRow;
    }
}
