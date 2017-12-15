// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

<<<<<<< 298fce66f1668fcfde4f429ae6eafa21d00ac9ee
import com.yahoo.bard.webservice.web.util.BardConfigResources;
=======
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.web.util.PaginationParameters;
>>>>>>> temp

import java.util.Optional;

import javax.ws.rs.container.ContainerRequestContext;

/**
 * An implementation of DataApiRequestFactory that does not modify the initial parameters at all.
 */
public class DefaultDataApiRequestFactory implements DataApiRequestFactory {

    //@Override
    /*
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
            ContainerRequestContext requestContext,
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
                requestContext,
                bardConfigResources
        );
<<<<<<< 298fce66f1668fcfde4f429ae6eafa21d00ac9ee
=======
    }*/

    @Override
    public DataApiRequest buildDataApiRequest(
            final DataApiRequestModel model,
            final String asyncAfter,
            final Optional<PaginationParameters> paginationParameters,
            final ContainerRequestContext requestContext,
            final MetricDictionary dictionary
    ) {
        return null;
>>>>>>> temp
    }
}
