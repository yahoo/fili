// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers

import com.yahoo.bard.webservice.data.metric.LogicalMetricImpl
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.data.metric.mappers.SketchRoundUpMapper
import com.yahoo.bard.webservice.data.metric.protocol.ProtocolMetricImpl
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.aggregation.DoubleMaxAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.DoubleMinAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.DoubleSumAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.LongMaxAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.LongMinAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.ThetaSketchAggregation

import spock.lang.Specification
import spock.lang.Unroll
/**
 * Tests for raw aggregation makers.
 */
class RawAggregationMetricMakerImplsSpec extends Specification {

    public static String NAME = "FOO"
    public static String FIELD_NAME = "BAR"
    LogicalMetricInfo info = new LogicalMetricInfo(NAME)

    @Unroll
    def "Expected numeric aggregation is produced for #makerClass.simpleName"() {
        setup:
        RawAggregationMetricMaker maker = makerClass.newInstance()

        expect:
        maker.make(NAME, FIELD_NAME) == makeNumericMetric(aggregation)

        where:
        makerClass     | aggregation
        DoubleSumMaker | new DoubleSumAggregation(NAME, FIELD_NAME)
        DoubleMaxMaker | new DoubleMaxAggregation(NAME, FIELD_NAME)
        DoubleMinMaker | new DoubleMinAggregation(NAME, FIELD_NAME)
        LongSumMaker   | new LongSumAggregation(NAME, FIELD_NAME)
        LongMaxMaker   | new LongMaxAggregation(NAME, FIELD_NAME)
        LongMinMaker   | new LongMinAggregation(NAME, FIELD_NAME)
    }

    @Unroll
    def "Expected sketch aggregation is produced for #makerClass.simpleName"() {
        setup:
        RawAggregationMetricMaker maker = makerClass.newInstance((MetricDictionary) null, 5)

        expect:
        maker.make(NAME, FIELD_NAME) == makeSketchMetric(aggregation)

        where:
        makerClass       | aggregation
        ThetaSketchMaker | new ThetaSketchAggregation(NAME, FIELD_NAME, 5)
    }

    /*
       It feels like cheating to duplicate so much of the makeInner from the class under test, but Mocking a logical
       metric can't be more accurate than this and the test primarily tests the subclasses integrating correctly.
     */
    def makeNumericMetric(Aggregation aggregation) {
        new ProtocolMetricImpl(
                info,
                new TemplateDruidQuery(Collections.singleton(aggregation), Collections.emptySet()),
                MetricMaker.NO_OP_MAPPER,
        );
    }

    def makeSketchMetric(Aggregation aggregation) {
        new ProtocolMetricImpl(
                info,
                new TemplateDruidQuery(Collections.singleton(aggregation), Collections.emptySet()),
                new SketchRoundUpMapper(aggregation.getName())
        );
    }
}
