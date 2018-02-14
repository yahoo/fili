package com.yahoo.bard.webservice.application.metadataViews;

import com.yahoo.bard.webservice.data.dimension.Dimension;

import java.util.Map;

import javax.ws.rs.container.ContainerRequestContext;

public class DimensionSummaryViewWithFieldsProvider extends DimensionSummaryViewProvider {

    @Override
    public Object apply(ContainerRequestContext containerRequestContext, Dimension dimension) {
        Map<String, Object> summaryMap = (Map<String, Object>) super.apply(containerRequestContext, dimension);
        summaryMap.put("fields", dimension.getDimensionFields());
        return summaryMap;
    }
}
