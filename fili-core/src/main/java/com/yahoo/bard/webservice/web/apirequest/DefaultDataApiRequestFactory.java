// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import com.yahoo.bard.webservice.web.apirequest.generator.metric.ProtocolLogicalMetricGenerator;
import com.yahoo.bard.webservice.web.util.BardConfigResources;

import java.util.List;

import javax.ws.rs.core.PathSegment;

/**
 * An implementation of DataApiRequestFactory that does not modify the initial parameters at all.
 */
public class DefaultDataApiRequestFactory implements DataApiRequestFactory {

    @Override
    public DataApiRequest buildApiRequest(
            String tableName,
            String granularity,
            List<PathSegment> dimensions,
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
            List<PathSegment> dimensions,
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
            BardConfigResources bardConfigResources
    ) {
        if (bardConfigResources.getMetricBinder() instanceof ProtocolLogicalMetricGenerator) {
            return new ProtocolMetricDataApiReqestImpl(
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
                    downloadFilename,
                    timeZoneId,
                    asyncAfter,
                    perPage,
                    page,
                    bardConfigResources
            );
        }
        return new DataApiRequestImpl(
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
                downloadFilename,
                timeZoneId,
                asyncAfter,
                perPage,
                page,
                bardConfigResources
        );
    }
}
