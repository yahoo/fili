// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import static com.yahoo.bard.webservice.config.BardFeatureFlag.PSEUDO_DIMENSION_SUPPORT;

import com.yahoo.bard.webservice.web.apirequest.generator.metric.ProtocolLogicalMetricGenerator;
import com.yahoo.bard.webservice.web.apirequest.requestParameters.RequestColumn;
import com.yahoo.bard.webservice.web.util.BardConfigResources;

import org.apache.commons.collections4.MultiValuedMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * An implementation of DataApiRequestFactory that does not modify the initial parameters at all.
 */
public class DefaultDataApiRequestFactory implements DataApiRequestFactory {

    @Override
    public DataApiRequest buildApiRequest(
            String tableName,
            String granularity,
            List<RequestColumn> dimensions,
            String logicalMetrics,
            String intervals,
            String apiFilters,
            String havings,
            String sorts,
            String count,
            String topN,
            String format,
            String timeZoneId,
            String asyncAfter,
            String perPage,
            String page,
            BardConfigResources bardConfigResources
    ) {
        return buildApiRequest(
                tableName,
                granularity,
                dimensions,
                logicalMetrics,
                intervals,
                apiFilters,
                havings,
                sorts,
                count,
                topN,
                format,
                timeZoneId,
                asyncAfter,
                null,
                perPage,
                page,
                bardConfigResources
        );
    }

    @Override
    public DataApiRequest buildApiRequest(
            String tableName,
            String granularity,
            List<RequestColumn> dimensions,
            String logicalMetrics,
            String intervals,
            String apiFilters,
            String havings,
            String sorts,
            String count,
            String topN,
            String format,
            String downloadFilename,
            String timeZoneId,
            String asyncAfter,
            String perPage,
            String page,
            MultiValuedMap<String, String> queryParams,
            BardConfigResources bardConfigResources
    ) {
        List<RequestColumn> dimensionsFiltered = dimensions;

        Map<String, Object> extendedObjects = new HashMap<>();
        if (PSEUDO_DIMENSION_SUPPORT.isOn()) {
            List<RequestColumn> pseudoDimensions = dimensions.stream()
                    .filter(column -> column.getApiName().startsWith("__"))
                    .collect(Collectors.toList());
            dimensionsFiltered = dimensions.stream()
                    .filter(column -> ! column.getApiName().startsWith("__"))
                    .collect(Collectors.toList());
            extendedObjects.put(ExtensibleDataApiReqestImpl.PSEUDO_DIMENSION_KEY, pseudoDimensions);
        }

        if (bardConfigResources.getMetricBinder() instanceof ProtocolLogicalMetricGenerator) {
            return new ProtocolMetricDataApiReqestImpl(
                    tableName,
                    granularity,
                    dimensionsFiltered,
                    logicalMetrics,
                    intervals,
                    apiFilters,
                    havings,
                    sorts,
                    count,
                    topN,
                    format,
                    downloadFilename,
                    timeZoneId,
                    asyncAfter,
                    perPage,
                    page,
                    queryParams,
                    extendedObjects,
                    bardConfigResources
            );
        }
        return new ExtensibleDataApiRequestImpl(
                tableName,
                granularity,
                dimensionsFiltered,
                logicalMetrics,
                intervals,
                apiFilters,
                havings,
                sorts,
                count,
                topN,
                format,
                downloadFilename,
                timeZoneId,
                asyncAfter,
                perPage,
                page,
                queryParams,
                extendedObjects,
                bardConfigResources
        );
    }
}
