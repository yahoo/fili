// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operand

import com.yahoo.bard.webservice.data.config.metric.makers.LongSumMaker
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.druid.model.aggregation.FilteredAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation
import com.yahoo.bard.webservice.druid.model.filter.SelectorFilter

import spock.lang.Specification

public class FilteredAggMetricNodeSpec extends Specification {

    def "should correctly construct a filtered aggregate"() {
        setup:
        MetricDictionary metricDictionary = new MetricDictionary()

        LogicalMetric metric = new LongSumMaker(metricDictionary).make("impressions", "impressions")
        def selectedDimension = Mock(Dimension)
        def expectedFilter = new SelectorFilter(selectedDimension, "3")

        // The filter
        def filter = Mock(SelectorFilterNode, {
            getFilterNode() >> it
            buildFilter() >> expectedFilter
        })

        def identifier = Mock(IdentifierNode, {
            getIdentifierNode() >> it
            getMetricNode() >> it
            make(_, metricDictionary) >> metric
        })

        def expectedQuery = new TemplateDruidQuery(
                [new FilteredAggregation("filteredImpressions", "impressions", new LongSumAggregation("impressions", "impressions"), expectedFilter)],
                []
        )

        def filteredAgg = new FilteredAggMetricNode(identifier, filter)
        def logicalMetric = filteredAgg.make("filteredImpressions", metricDictionary)

        expect:
        logicalMetric.getTemplateDruidQuery() == expectedQuery
    }
}
