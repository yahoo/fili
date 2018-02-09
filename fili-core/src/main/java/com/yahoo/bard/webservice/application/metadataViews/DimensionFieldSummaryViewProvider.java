package com.yahoo.bard.webservice.application.metadataViews;

import com.yahoo.bard.webservice.data.dimension.DimensionField;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.ws.rs.container.ContainerRequestContext;

public class DimensionFieldSummaryViewProvider implements MetadataViewProvider<DimensionField> {

    @Override
    public Object apply(ContainerRequestContext containerRequestContext, DimensionField dimensionField) {
        Map<String, String> resultRow = new LinkedHashMap<>();
        resultRow.put("name", dimensionField.getName());
        resultRow.put("description", dimensionField.getDescription());
        return resultRow;
    }
}
