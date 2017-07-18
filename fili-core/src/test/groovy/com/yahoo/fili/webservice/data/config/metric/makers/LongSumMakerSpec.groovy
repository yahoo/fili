// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.data.config.metric.makers

import com.yahoo.fili.webservice.data.metric.LogicalMetric
import com.yahoo.fili.webservice.data.metric.TemplateDruidQuery
import com.yahoo.fili.webservice.data.metric.mappers.NoOpResultSetMapper
import com.yahoo.fili.webservice.druid.model.aggregation.Aggregation
import com.yahoo.fili.webservice.druid.model.aggregation.LongSumAggregation

import spock.lang.Specification;

class LongSumMakerSpec extends Specification{

    private static final String METRIC_NAME = "pageViews"
    private static final String DEPENDENT_METRIC_NAME = "totalPageViews"

    def "A long sum logical metric is made correctly"(){
        given: "The name of the metric the maker depends on, and the maker itself"
        //LongSum is a RawAggregationQuery, so it does not depend on a MetricDictionary.
        MetricMaker maker = new LongSumMaker(null)

        and: "The expected metric"
        Aggregation sumAggregation = new LongSumAggregation(METRIC_NAME, DEPENDENT_METRIC_NAME)
        Set<Aggregation> aggregations = [sumAggregation] as Set
        TemplateDruidQuery query = new TemplateDruidQuery(aggregations, [] as Set)
        LogicalMetric expectedMetric = new LogicalMetric(query, new NoOpResultSetMapper(), METRIC_NAME)

        expect:
        maker.make(METRIC_NAME, [DEPENDENT_METRIC_NAME]) == expectedMetric
    }
}
