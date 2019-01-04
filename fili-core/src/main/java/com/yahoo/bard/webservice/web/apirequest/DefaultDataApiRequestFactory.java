// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import com.yahoo.bard.webservice.web.util.BardConfigResources;

import java.util.List;

import javax.ws.rs.core.PathSegment;

/**
 * An implementation of DataApiRequestFactory that does not modify the initial parameters at all.
 */
public class DefaultDataApiRequestFactory implements DataApiRequestFactory {

    @Override
    public DataApiRequest buildApiRequest(
            final String tableName,
            final String granularity,
            final List<PathSegment> dimensions,
            final String logicalMetrics,
            final String intervals,
            final String apiFilters,
            final String havings,
            final String sorts,
            final String count,
            final String topN,
            final String format,
            final String downloadFilename,
            final String timeZoneId,
            final String asyncAfter,
            final String perPage,
            final String page,
            final BardConfigResources bardConfigResources
    ) {
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
                bardConfigResources.getDimensionDictionary(),
                bardConfigResources.getMetricDictionary(),
                bardConfigResources.getLogicalTableDictionary(),
                bardConfigResources.getSystemTimeZone(),
                bardConfigResources.getGranularityParser(),
                bardConfigResources.getFilterBuilder(),
                bardConfigResources.getHavingApiGenerator()
        );
    }
}
