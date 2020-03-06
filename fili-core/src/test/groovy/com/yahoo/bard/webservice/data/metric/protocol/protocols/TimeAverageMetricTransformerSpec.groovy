// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.protocol.protocols

import com.yahoo.bard.webservice.data.config.metric.makers.LongSumMaker
import com.yahoo.bard.webservice.data.config.metric.makers.MetricMaker
import com.yahoo.bard.webservice.data.config.metric.makers.ThetaSketchMaker
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.data.metric.protocol.Protocol
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.druid.model.aggregation.DoubleSumAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.ConstantPostAggregation
import com.yahoo.bard.webservice.druid.util.FieldConverterSupplier
import com.yahoo.bard.webservice.druid.util.ThetaSketchFieldConverter

import spock.lang.Specification
import spock.lang.Unroll

class TimeAverageMetricTransformerSpec extends Specification {

    Protocol protocol = ReaggregationProtocol.INSTANCE

    @Unroll
    def "Create a time average for #grain"() {
        setup:
        FieldConverterSupplier.sketchConverter = new ThetaSketchFieldConverter()
        TimeAverageMetricTransformer timeAverageTransformer = TimeAverageMetricTransformer.INSTANCE

        MetricDictionary metricDictionary = new MetricDictionary();
        MetricMaker maker = new LongSumMaker(metricDictionary)
        MetricMaker sketchMaker = new ThetaSketchMaker(metricDictionary, 128)

        LogicalMetric longSum = maker.make("foo", "bar")
        LogicalMetric sketchUnion = sketchMaker.make("foo", "bar")
        Map params = [(protocol.coreParameterName): value]

        LogicalMetric averageMetric = timeAverageTransformer.apply(longSum, protocol, params)
        LogicalMetric sketchAverage = timeAverageTransformer.apply(sketchUnion, protocol, params)

        expect:
        TemplateDruidQuery innerQuery = averageMetric.templateDruidQuery.getInnermostQuery()
        innerQuery.getMetricField("foo") instanceof LongSumAggregation
        ((LongSumAggregation) innerQuery.getMetricField("foo")).fieldName == "bar"
        innerQuery.getMetricField("one") instanceof ConstantPostAggregation
        ((ConstantPostAggregation) innerQuery.getMetricField("one")).value == 1
        TemplateDruidQuery outerQuery = averageMetric.templateDruidQuery
        outerQuery.getMetricField("foo") instanceof LongSumAggregation
        grain.satisfies(innerQuery.getTimeGrain())

        sketchAverage.templateDruidQuery.getMetricField("foo_estimate_sum") instanceof DoubleSumAggregation
        ((DoubleSumAggregation) sketchAverage.
                templateDruidQuery.
                getMetricField("foo_estimate_sum")).fieldName == "foo_estimate"

        where:
        value      | grain                  | longNameBase
        "dayAvg"   | DefaultTimeGrain.DAY   | "Daily Average"
        "weekAvg"  | DefaultTimeGrain.WEEK  | "Weekly Average"
        "monthAvg" | DefaultTimeGrain.MONTH | "Monthly Average"
    }

    @Unroll
    def "LogicalName transformer"() {
        setup:
        TimeAverageMetricTransformer timeAverageTransformer = TimeAverageMetricTransformer.INSTANCE
        TimeAverageMetricTransformer.TimeAverageMetricMakerConfig config = new TimeAverageMetricTransformer.TimeAverageMetricMakerConfig(value, value, longNameBase, DefaultTimeGrain.DAY)
        MetricDictionary metricDictionary = new MetricDictionary();

        String longName = "foo (" + longNameBase + ")"

        MetricMaker maker = new LongSumMaker(metricDictionary)
        LogicalMetric longSum = maker.make("foo", "bar")
        Map params = [(protocol.coreParameterName): value]

        LogicalMetricInfo info = timeAverageTransformer.makeNewLogicalMetricInfo(longSum.logicalMetricInfo, config)

        expect:
        info.getName().startsWith(value)
        info.getLongName() == longName
        info.getDescription().contains(longNameBase)

        where:
        value      | grain                  | longNameBase
        "dayAvg"   | DefaultTimeGrain.DAY   | "Daily Average"
        "weekAvg"  | DefaultTimeGrain.WEEK  | "Weekly Average"
        "monthAvg" | DefaultTimeGrain.MONTH | "Monthly Average"
    }
}
