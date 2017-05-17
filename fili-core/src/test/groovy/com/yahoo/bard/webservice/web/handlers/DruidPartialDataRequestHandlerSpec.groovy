// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers

import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery

import spock.lang.Specification

class DruidPartialDataRequestHandlerSpec extends Specification {
    def "New query context is passed to next handler"() {
        given:
        DruidPartialDataRequestHandler druidPartialDataRequestHandler = new DruidPartialDataRequestHandler(
                Mock(DataRequestHandler)
        )
        druidPartialDataRequestHandler.DRUID_UNCOVERED_INTERVAL_LIMIT << 10

        DruidAggregationQuery druidAggregationQuery = Mock(DruidAggregationQuery)
        druidAggregationQuery
    }
}
