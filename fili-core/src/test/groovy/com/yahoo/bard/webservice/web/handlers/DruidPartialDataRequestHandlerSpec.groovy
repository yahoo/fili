// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers

import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigProvider
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.bard.webservice.druid.model.query.QueryContext
import com.yahoo.bard.webservice.web.DataApiRequest
import com.yahoo.bard.webservice.web.responseprocessors.DruidPartialDataResponseProcessor
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor

import spock.lang.Specification

class DruidPartialDataRequestHandlerSpec extends Specification {
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    def "New query context is passed to next handler"() {
        given:
        SystemConfig systemConfig = SystemConfigProvider.getInstance()
        String uncoveredKey = SYSTEM_CONFIG.getPackageVariableName("druid_uncovered_interval_limit")
        systemConfig.setProperty(uncoveredKey, '10')

        DataRequestHandler nextHandler = Mock(DataRequestHandler)
        ResponseProcessor responseProcessor = Mock(ResponseProcessor)
        RequestContext requestContext = Mock(RequestContext)
        DruidAggregationQuery druidQuery = Mock(DruidAggregationQuery)
        QueryContext queryContext = Mock(QueryContext)
        DataApiRequest apiRequest = Mock(DataApiRequest)

        druidQuery.getContext() >> queryContext

        DruidPartialDataRequestHandler druidPartialDataRequestHandler = new DruidPartialDataRequestHandler(
                nextHandler
        )

        when:
        druidPartialDataRequestHandler.handleRequest(requestContext, apiRequest, druidQuery, responseProcessor)

        then:
        1 * druidQuery.withContext(queryContext) >> druidQuery
        1 * queryContext.withUncoveredIntervalsLimit(10) >> queryContext
        1 * nextHandler.handleRequest(requestContext, apiRequest, druidQuery, _ as DruidPartialDataResponseProcessor)

        cleanup:
        systemConfig.clearProperty(uncoveredKey)
    }
}
