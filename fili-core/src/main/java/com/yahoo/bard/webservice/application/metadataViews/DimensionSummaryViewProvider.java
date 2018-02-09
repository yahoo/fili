package com.yahoo.bard.webservice.application.metadataViews;

import static com.yahoo.bard.webservice.web.endpoints.DimensionsServlet.getDimensionUrl;

import com.yahoo.bard.webservice.data.dimension.Dimension;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.ws.rs.container.ContainerRequestContext;

public class DimensionSummaryViewProvider implements MetadataViewProvider<Dimension> {

    @Override
    public Object apply(
            final ContainerRequestContext containerRequestContext, final Dimension dimension
    ) {
        Map<String, Object> resultRow = new LinkedHashMap<>();
        resultRow.put("category", dimension.getCategory());
        resultRow.put("name", dimension.getApiName());
        resultRow.put("longName", dimension.getLongName());
        resultRow.put("uri", getDimensionUrl(dimension, containerRequestContext.getUriInfo()));
        resultRow.put("cardinality", dimension.getCardinality());
        resultRow.put("storageStrategy", dimension.getStorageStrategy());
        return resultRow;
    }
}
