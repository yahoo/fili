// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers

import com.yahoo.bard.webservice.data.config.metric.MetricInstance
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.druid.model.aggregation.CardinalityAggregation

import spock.lang.Specification

/**
 * Test cardinality maker creates a metric correctly
 */
class CardinalityMakerSpec extends Specification {

    MetricDictionary metricDictionary = new MetricDictionary()
    DimensionDictionary dimensionDictionary = new DimensionDictionary()

    Dimension dimension = Mock(Dimension)
    String dimensionApiName = "ApiName"

    def setup() {
        dimension.getApiName() >> dimensionApiName

        dimensionDictionary.add(dimension)
    }

    def "Test building a cardinality metric via maker meets expected values"() {
        setup:
        CardinalityMaker cardinalityMaker = new CardinalityMaker(
                metricDictionary,
                dimensionDictionary,
                true
        )
        LogicalMetric actualMetric = cardinalityMaker.make("metricName", dimensionApiName)
        CardinalityAggregation actual = (CardinalityAggregation) actualMetric.getTemplateDruidQuery().getMetricField("metricName")

        expect:
        actual.getName() == "metricName"
        actual.dependentDimensions == [dimension] as Set
        actual.byRow
        actual.fieldName == ""
    }


    def "Test building a cardinality metric from Metric Instance builds correct metric"() {
        setup:
        CardinalityMaker cardinalityMaker = new CardinalityMaker(
                metricDictionary,
                dimensionDictionary,
                true
        )
        MetricInstance metricInstance = new MetricInstance("metricName", cardinalityMaker, dimensionApiName)
        LogicalMetric actualMetric = metricInstance.make()
        CardinalityAggregation actual = (CardinalityAggregation) actualMetric.getTemplateDruidQuery().getMetricField("metricName")

        expect:
        actual.getName() == "metricName"
        actual.dependentDimensions == [dimension] as Set
        actual.byRow
        actual.fieldName == ""
    }
}
