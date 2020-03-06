// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.bard.webservice.data.config.metric.MetricInstance
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper
import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper
import com.yahoo.bard.webservice.data.metric.mappers.SketchRoundUpMapper
import com.yahoo.bard.webservice.data.metric.protocol.ProtocolMetricImpl
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.ThetaSketchEstimatePostAggregation
import com.yahoo.bard.webservice.druid.util.FieldConverterSupplier
import com.yahoo.bard.webservice.druid.util.FieldConverters
import com.yahoo.bard.webservice.druid.util.ThetaSketchFieldConverter

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.util.function.Function

class ArithmeticMakerSpec extends Specification {
    public static final String AGG_AVERAGE_NAME = "aggregationMetric"
    public static final String METRIC_FIELD_NAME = "aggregationField"
    public static final String AVERAGE_PER_OTHER_METRIC_NAME = "averagePerOtherMetric"
    public static final String AVERAGE_PER_OTHER_METRIC_ROUNDED_METRIC_NAME = "averagePerOtherMetricRounded"
    LogicalMetric unRoundedMetric
    LogicalMetric roundedUpMetric

    private static final int SKETCH_SIZE = 16000
    //Neither of these relies on a metric dictionary.
    private static final ConstantMaker CONSTANT_MAKER = new ConstantMaker(null)
    private static final ThetaSketchMaker SKETCH_MAKER = new ThetaSketchMaker(null, SKETCH_SIZE)
    private static final OPERATION = ArithmeticPostAggregation.ArithmeticPostAggregationFunction.PLUS

    @Shared
    FieldConverters converter = FieldConverterSupplier.sketchConverter

    def setup() {
        MetricDictionary metricDictionary = new MetricDictionary()
        FieldConverterSupplier.sketchConverter = new ThetaSketchFieldConverter();

        ArithmeticMaker divisionMaker = new ArithmeticMaker(
                metricDictionary,
                ArithmeticPostAggregation.ArithmeticPostAggregationFunction.DIVIDE
        )
        ArithmeticMaker roundUpDivisionMaker = new ArithmeticMaker(
                metricDictionary,
                ArithmeticPostAggregation.ArithmeticPostAggregationFunction.DIVIDE,
                (Function<String, ResultSetMapper>) {String it-> new SketchRoundUpMapper(it)}
        )

        MetricInstance pageViews = new MetricInstance(METRIC_FIELD_NAME, new LongSumMaker(metricDictionary),
                METRIC_FIELD_NAME
        )
        metricDictionary.add(pageViews.make())

        MetricInstance aggregationAverageMetric = new MetricInstance(
                AGG_AVERAGE_NAME,
                new AggregationAverageMaker(metricDictionary, DAY),
                METRIC_FIELD_NAME
        )
        metricDictionary.add(aggregationAverageMetric.make())

        MetricInstance averagePerOtherMetricMetric = new MetricInstance(
                AVERAGE_PER_OTHER_METRIC_NAME,
                divisionMaker,
                AGG_AVERAGE_NAME,
                METRIC_FIELD_NAME
        )
        unRoundedMetric = averagePerOtherMetricMetric.make()
        metricDictionary.add(unRoundedMetric)

        MetricInstance dayAvgPageViewsPerTotalPageViewsRoundedUpPM = new MetricInstance(
                AVERAGE_PER_OTHER_METRIC_ROUNDED_METRIC_NAME,
                roundUpDivisionMaker,
                AGG_AVERAGE_NAME,
                METRIC_FIELD_NAME
        )
        roundedUpMetric = dayAvgPageViewsPerTotalPageViewsRoundedUpPM.make()
        metricDictionary.add(roundedUpMetric)
    }

    def cleanupSpec() {
        FieldConverterSupplier.sketchConverter = converter
    }

    @Unroll
    def "Build a metric that operates on #numOperands metrics built with #operandMaker."(){
        given: "Two or more operands and the name of the build metric"
        //The ConstantMaker relies on the dependent metric string being a number, the
        //the SketchCountMaker doesn't care.
        List<LogicalMetric> operands = (1..numOperands).collect{
            operandMaker.make("metric$it", it as String)
        }
        String metricName = "sum"
        List<TemplateDruidQuery> operandQueries = operands*.getTemplateDruidQuery()
        Set<Aggregation> aggregations = operandQueries*.getAggregations().flatten() as Set<Aggregation>


        and: "the PostAggregations used as fields for the arithmetic"
        List<PostAggregation> postAggregationsForArithmetic
        if (operandMaker == CONSTANT_MAKER) {
            //Constants are themselves post aggregations, so we can just use them directly as the fields to
            //the arithmetic.
            postAggregationsForArithmetic = operandQueries*.getPostAggregations().flatten() as List<PostAggregation>
        }
        else {
            //The sketch count is actually an aggregation that we're using like a sketch merge. So we need to
            //create post aggregations that estimate the size of said sketches. We can then sum up those sizes.
            postAggregationsForArithmetic = aggregations.collect(){
                new ThetaSketchEstimatePostAggregation("${it.name}_estimate", new FieldAccessorPostAggregation(it))
            }
        }

        and: "the expected LogicalMetric"
        PostAggregation sumPostAggregation = new ArithmeticPostAggregation(
                metricName,
                OPERATION,
                postAggregationsForArithmetic
        )
        TemplateDruidQuery expectedQuery = new TemplateDruidQuery(
                aggregations,
                [sumPostAggregation] as Set
        )

        LogicalMetric expectedMetric = new ProtocolMetricImpl(
                new LogicalMetricInfo(metricName),
                expectedQuery,
                MetricMaker.NO_OP_MAPPER
        )

        and: "a populated metric dictionary for the maker"
        MetricMaker maker = new ArithmeticMaker(new MetricDictionary(), OPERATION)
        operands.each{maker.metrics.add(it)}

        expect:
        maker.make(new LogicalMetricInfo(metricName), operands*.name) == expectedMetric

        where: "We test on 2, 3 and 4 operands using both constants and sketches."
        numOperands << [2, 3, 4] * 2
        operandMaker << [CONSTANT_MAKER] * 3 + [SKETCH_MAKER] * 3
    }

    def "ArithmeticMaker supports dependent metrics with nested query"() {
        expect:
        unRoundedMetric.getTemplateDruidQuery().nested
    }

    def "When a ResultMapper is explicitly passed, it creates the LogicalMetric using the correct mapper"() {
        expect:

        unRoundedMetric.calculation.class == NoOpResultSetMapper
    }

    def "When a ResultMapper is not passed, it creates the LogicalMetric using the default ThetaSketchSetOperationMaker"() {
        expect:
        roundedUpMetric.calculation.class == SketchRoundUpMapper
    }
}
