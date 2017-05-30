// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers

import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.datasource.DataSource
import com.yahoo.bard.webservice.druid.model.filter.Filter
import com.yahoo.bard.webservice.druid.model.having.Having
import com.yahoo.bard.webservice.druid.model.orderby.LimitSpec
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.bard.webservice.druid.model.query.Granularity
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.druid.model.query.QueryContext

import org.joda.time.Interval

import spock.lang.Specification

class DruidPartialDataRequestHandlerSpec extends Specification {
    def "New query context is passed to next handler"() {
        given:
        DruidPartialDataRequestHandler druidPartialDataRequestHandler = new DruidPartialDataRequestHandler(
                Mock(DataRequestHandler)
        )
        druidPartialDataRequestHandler.druidUncoveredIntervalLimit = 10

        QueryContext queryContext = new QueryContext([:])
        DruidAggregationQuery druidAggregationQuery = new GroupByQuery(
                Mock(DataSource),
                Mock(Granularity),
                Mock(Collection),
                Mock(Filter),
                Mock(Having),
                Collections.emptyList(),
                Collections.emptyList(),
                Mock(Collection),
                Mock(LimitSpec),
                queryContext,
                false
        )

        expect:
        druidPartialDataRequestHandler.addDruidUncoveredIntervalLimitTo(druidAggregationQuery)
                .getContext()
                .getUncoveredIntervalsLimit() == 10
    }
}
