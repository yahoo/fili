// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.protocol.protocols

import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.LogicalMetricImpl
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper
import com.yahoo.bard.webservice.data.metric.protocol.GeneratedMetricInfo
import com.yahoo.bard.webservice.data.metric.protocol.Protocol
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation

import spock.lang.Specification
import spock.lang.Unroll

class ReaggregationProtocolSpec extends Specification {

    Protocol protocol = ReaggregationProtocol.INSTANCE

    @Unroll
    def "Accepted value #acceptedValue is accepted by this protocol's transformer"() {
        setup:
        Aggregation agg = new LongSumAggregation("foo", "bar")
        TemplateDruidQuery templateDruidQuery = new TemplateDruidQuery([agg], [])
        LogicalMetric logicalMetric = new LogicalMetricImpl(templateDruidQuery, new NoOpResultSetMapper(), "foo");
        GeneratedMetricInfo resultLmi = new GeneratedMetricInfo("result", "base")

        Map params = [(ReaggregationProtocol.REAGG_CORE_PARAMETER): acceptedValue]

        when:
        LogicalMetric a = protocol.getMetricTransformer().apply(resultLmi, logicalMetric, protocol, params)

        then:
        a.templateDruidQuery.innermostQuery.granularity != null

        where:
        acceptedValue << ReaggregationProtocol.acceptedValues()
    }
}
