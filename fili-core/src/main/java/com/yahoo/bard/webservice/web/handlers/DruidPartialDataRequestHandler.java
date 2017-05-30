// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.responseprocessors.DruidPartialDataResponseProcessor;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor;

import javax.validation.constraints.NotNull;

/**
 * A request handler that builds responses for Druid partial data
 * <p>
 * The handler inject "uncoveredIntervalsLimit: $druid_uncovered_interval_limit" context to Druid query.
 */
public class DruidPartialDataRequestHandler implements DataRequestHandler {

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    private final DataRequestHandler next;
    private final int druidUncoveredIntervalLimit = SYSTEM_CONFIG.getIntProperty(
            SYSTEM_CONFIG.getPackageVariableName("druid_uncovered_interval_limit"), 0
    );

    /**
     * Constructor.
     *
     * @param next  Next Handler in the chain
     */
    public DruidPartialDataRequestHandler(@NotNull DataRequestHandler next) {
        this.next = next;
    }

    @Override
    public boolean handleRequest(
            RequestContext context,
            DataApiRequest request,
            DruidAggregationQuery<?> druidQuery,
            ResponseProcessor response
    ) {
        return next.handleRequest(
                context,
                request,
                druidQuery.withContext(
                        druidQuery.getContext().withUncoveredIntervalsLimit(druidUncoveredIntervalLimit)
                ),
                new DruidPartialDataResponseProcessor(response)
        );
    }
}
