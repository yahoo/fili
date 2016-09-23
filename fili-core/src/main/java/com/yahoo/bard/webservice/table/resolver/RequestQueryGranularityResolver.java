// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.QUERY_GRAIN_NOT_SATISFIED;

import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.data.time.ZonelessTimeGrain;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.web.DataApiRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiFunction;

/**
 * A function to take an ApiRequest and TemplateDruidQuery and determine a Granularity which must be satisfied by a
 * fact source in order to satisfy this request.
 */
public class RequestQueryGranularityResolver implements BiFunction<DataApiRequest, TemplateDruidQuery, Granularity> {

    private static final Logger LOG = LoggerFactory.getLogger(RequestQueryGranularityResolver.class);

    /**
     * Get the a granularity which must be satisfied by any answering table granularity for this request.
     * <p>
     * If a query has a time grain constraint, it will be returned as a granularity with the request time zone
     * applied.  If the query has no constraint, the request grain will be returned as a granularity.
     *
     * @param apiRequest  DataApiRequest from the user which may specify a coarsest satisfying grain
     * @param query  Query which may apply a coarsest satisfying grain
     *
     * @return The coarsest valid table grain to satisfy the query
     */
    public Granularity resolveAcceptingGrain(DataApiRequest apiRequest, TemplateDruidQuery query) {
        // Gather any specified time grains
        Granularity requestGranularity = apiRequest.getGranularity();
        ZonelessTimeGrain queryGrain = query.getInnermostQuery().getTimeGrain();

        // The query makes no restrictions, so use the apiRequest only
        if (queryGrain == null) {
            return requestGranularity;
        }

        if (requestGranularity.satisfiedBy(queryGrain)) {
            return queryGrain.buildZonedTimeGrain(apiRequest.getTimeZone());
        }

        LOG.error(QUERY_GRAIN_NOT_SATISFIED.format(queryGrain, requestGranularity));
        throw new IllegalArgumentException(QUERY_GRAIN_NOT_SATISFIED.format(queryGrain, requestGranularity));
    }

    @Override
    public Granularity apply(DataApiRequest request, TemplateDruidQuery templateDruidQuery) {
        return resolveAcceptingGrain(request, templateDruidQuery);
    }
}
