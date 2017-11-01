// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers

import static com.yahoo.bard.webservice.data.config.metric.makers.MetricMaker.INCORRECT_NUMBER_OF_DEPS_FORMAT
import static com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation.ArithmeticPostAggregationFunction.MULTIPLY
import static com.yahoo.bard.webservice.druid.model.postaggregation.SketchSetOperationPostAggFunction.UNION

import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.config.LogicalMetricInfo
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper
import com.yahoo.bard.webservice.data.metric.mappers.SketchRoundUpMapper
import com.yahoo.bard.webservice.druid.model.MetricField
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.ThetaSketchAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.ConstantPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.ThetaSketchSetOperationPostAggregation
import com.yahoo.bard.webservice.druid.util.FieldConverterSupplier
import com.yahoo.bard.webservice.druid.util.FieldConverters
import com.yahoo.bard.webservice.druid.util.ThetaSketchFieldConverter

import spock.lang.Specification
import spock.lang.Unroll
/**
 * Tests the code implemented by MetricMaker, using a stub for code that is meant to be implemented by the children.
 * Primarily tests the sad paths. The specifications of the children of MetricMaker test the various happy paths.
 */
class MetricMakerSpec extends Specification {

    static final String METRIC_NAME = "metric name"
    static final int DEPENDENT_METRICS = 3

    static final FieldConverters originalConverter = FieldConverterSupplier.sketchConverter
    static final FieldConverters CONVERTER = new ThetaSketchFieldConverter()

    MetricDictionary dictionary = new MetricDictionary()

    static LogicalMetric longSumMetric
    static PostAggregation longSumFieldAccessor
    static LogicalMetric squareMetric
    static PostAggregation number

    static LogicalMetric sketchAggregationMetric
    static PostAggregation sketchFieldAccessor
    static LogicalMetric sketchEstimateMetric
    static LogicalMetric sketchUnionMetric
    static LogicalMetric sketchUnionEstimateMetric

    static LogicalMetric constantMetric

    MetricMaker maker = getMakerInstance()

    def setupSpec() {

        String sketchName = "all_users"
        String sketchUnionName = "union_users"
        String constantName = "constant1"
        String sumName = "longSum"
        String squareName = "square"

        number = new ConstantPostAggregation(constantName, 1.0)
        FieldConverterSupplier.sketchConverter = CONVERTER

        TemplateDruidQuery queryTemplate
        queryTemplate = new TemplateDruidQuery([] as Set, [number] as Set)
        constantMetric = new LogicalMetric(queryTemplate, new NoOpResultSetMapper(), constantName)

        Aggregation longSum = new LongSumAggregation(sumName, "columnName")
        queryTemplate = new TemplateDruidQuery([longSum] as Set, [] as Set)
        longSumMetric = new LogicalMetric(queryTemplate, new NoOpResultSetMapper(), longSum.name)

        longSumFieldAccessor = new FieldAccessorPostAggregation(longSum)
        PostAggregation square = new ArithmeticPostAggregation(squareName, MULTIPLY, [longSumFieldAccessor, longSumFieldAccessor])

        queryTemplate = new TemplateDruidQuery([longSum] as Set, [square] as Set)
        squareMetric = new LogicalMetric(queryTemplate, new NoOpResultSetMapper(), square.name)

        // Theta Sketches
        Aggregation sketchAggregation = new ThetaSketchAggregation(sketchName, "columnName", 16000)
        queryTemplate = new TemplateDruidQuery([sketchAggregation] as Set, [] as Set)
        sketchAggregationMetric = new LogicalMetric(queryTemplate, new SketchRoundUpMapper(sketchAggregation.name), sketchAggregation.name)

        PostAggregation sketchEstimateAggregation = CONVERTER.asSketchEstimate(sketchAggregation)

        sketchFieldAccessor = sketchEstimateAggregation.getField()

        queryTemplate = new TemplateDruidQuery([sketchAggregation] as Set, [sketchEstimateAggregation] as Set)
        sketchEstimateMetric = new LogicalMetric(queryTemplate, new SketchRoundUpMapper(sketchEstimateAggregation.name), sketchEstimateAggregation.name)

        PostAggregation sketchSetAggregation = new ThetaSketchSetOperationPostAggregation(
                sketchUnionName,
                UNION,
                [sketchFieldAccessor, sketchFieldAccessor]
        )
        queryTemplate = new TemplateDruidQuery([sketchAggregation] as Set, [sketchSetAggregation] as Set)
        sketchUnionMetric = new LogicalMetric(queryTemplate, new SketchRoundUpMapper(sketchSetAggregation.name), sketchSetAggregation.name)

        PostAggregation sketchSetEstimate = CONVERTER.asSketchEstimate(sketchSetAggregation)
        queryTemplate = new TemplateDruidQuery([sketchAggregation] as Set, [sketchSetEstimate] as Set)
        sketchUnionEstimateMetric = new LogicalMetric(queryTemplate, new SketchRoundUpMapper(sketchSetEstimate.name), sketchSetEstimate.name)
    }

    def cleanupSpec() {
        FieldConverterSupplier.sketchConverter = originalConverter
    }

    private static final LogicalMetric DEFAULT_METRIC = new LogicalMetric(
            new TemplateDruidQuery([] as Set, [] as Set),
            new NoOpResultSetMapper(),
            "no name",
            "no description"
    )

    /**
     * Builds and returns an instance of the MetricMaker under test.
     * In order to avoid cross-test pollination, a new instance should be built
     * and returned every time this method is invoked.
     * @return A new instance of the MetricMaker under test.
     */
    MetricMaker getMakerInstance(){
        new MetricMaker(dictionary) {
            @Override
            protected LogicalMetric makeInner(LogicalMetricInfo logicalMetricInfo, List<String> dependentMetrics) {
                DEFAULT_METRIC
            }
            
            @Override
            protected int getDependentMetricsRequired() {
                DEPENDENT_METRICS
            }
        }
    }

    /**
     * Builds a list of placeholder metric names of the form: [metric1, metric2, ...]
     * @param numMetrics The number of metrics names to be built
     * @return The list of metric names
     */
    List<String> buildDependentMetricNames(int numMetrics){
        //Can't use string interpolation because that treats the Strings to GStrings. Java then throws a
        //ClassCastExceptions when the GStrings are passed in where Strings are expected.
        (1..numMetrics).collect{"metric" + it}
    }

    /**
     * Given a list of names, returns a dictionary that assigns to each name in the set a fresh placeholder
     * LogicalMetric.
     * @param metricNames The names to construct LogicalMetrics for.
     * @return The mapping from metric names to fresh LogicalMetrics.
     */
    Map<String, LogicalMetric> makeEmptyMetrics(List<String> metricNames){
        metricNames.collectEntries {
            [(it): new LogicalMetric(null, null, it)]
        }
    }

    def "When a claimed dependent metric does not exist in the maker's dictionary, an exception is thrown"(){
        given: "A list of dependent metrics, one of which doesn't exist"
        List<String> dependentMetricNames = buildDependentMetricNames(DEPENDENT_METRICS)
        dictionary.putAll(makeEmptyMetrics(dependentMetricNames))
        String badName = "I don't exist!"
        dependentMetricNames.add(badName)

        when:
        maker.make(METRIC_NAME, dependentMetricNames)

        then:
        Exception exception = thrown(IllegalArgumentException)
        exception.message == String.format(MetricMaker.MISSING_DEP_FORMAT, badName)
    }

    @Unroll
    def "When a metric has too #adjective dependent metrics, an exception is thrown."(){
        given: "One fewer dependent metrics than this metric depends on"
        List<String> dependentMetricNames = buildDependentMetricNames(DEPENDENT_METRICS+2)
        dictionary.putAll(makeEmptyMetrics(dependentMetricNames))

        when:
        getMakerInstance().make(METRIC_NAME, dependentMetricNames.subList(0, DEPENDENT_METRICS + adjustment))

        then:
        Exception exception = thrown(IllegalArgumentException)
        exception.getMessage() == String.format(INCORRECT_NUMBER_OF_DEPS_FORMAT, METRIC_NAME, DEPENDENT_METRICS+adjustment, DEPENDENT_METRICS)

        where:
        adjective | adjustment
        "many"    | 1
        "few"     | -1
    }

    @Unroll
    def "getNumericField returns a numeric metric #expected.type for metric named: #fromMetric.name "() {
        expect:
        MetricMaker.getNumericField(fromMetric.metricField) == expected

        where:
        fromMetric                | expected
        constantMetric            | constantMetric.metricField
        squareMetric              | squareMetric.metricField
        longSumMetric             | longSumFieldAccessor
        sketchEstimateMetric      | sketchEstimateMetric.metricField
        sketchAggregationMetric   | sketchEstimateMetric.metricField
        sketchUnionMetric         | sketchUnionEstimateMetric.metricField
        sketchUnionEstimateMetric | sketchUnionEstimateMetric.metricField
    }

    @Unroll
    def "getSketchField returns a sketch aggregation of type #expected.type for metric named: #fromMetric.name "() {
        expect:
        MetricMaker.getSketchField(fromMetric.metricField) == expected

        where:
        fromMetric                | expected
        sketchEstimateMetric      | sketchFieldAccessor
        sketchAggregationMetric   | sketchFieldAccessor
        sketchUnionMetric         | sketchUnionMetric.metricField
        sketchUnionEstimateMetric | sketchUnionMetric.metricField
    }

    @Unroll
    def "getSketchField throws error when given a non sketch based field #fromMetric.name "() {
        when:
        MetricField field = fromMetric.metricField
        MetricMaker.getSketchField(field)

        then:
        Exception exception = thrown(IllegalArgumentException)
        exception.getMessage() == String.format(MetricMaker.SKETCH_REQUIRED_FORMAT, field.name, field)

        where:
        fromMetric  << [constantMetric, squareMetric, longSumMetric]
    }
}
