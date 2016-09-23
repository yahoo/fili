// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers

import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.SketchMergeAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.ConstantPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation
import com.yahoo.bard.webservice.druid.util.FieldConverterSupplier
import com.yahoo.bard.webservice.druid.util.FieldConverters
import com.yahoo.bard.webservice.druid.util.SketchFieldConverter

import spock.lang.Shared
import spock.lang.Specification

/**
 * Tests the code implemented by MetricMaker, using a stub for code that is meant to be implemented by the children.
 * Primarily tests the sad paths. The specifications of the children of MetricMaker test the various happy paths.
 */
class MetricMakerSpec extends Specification {

    static final String METRIC_NAME = "metric name"

    MetricMaker maker
    @Shared
    FieldConverters converter = FieldConverterSupplier.sketchConverter

    private final LogicalMetric METRIC = new LogicalMetric(
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
    MetricMaker getInstance(){
        new MetricMaker(new MetricDictionary()) {
            @Override
            protected LogicalMetric makeInner(String metricName, List<String> dependentMetrics) {
                METRIC
            }

            @Override
            protected int getDependentMetricsRequired() {
                1
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

    def setup(){
        maker = getInstance()
        //Initializing the Sketch field converter
        FieldConverterSupplier.sketchConverter = new SketchFieldConverter();
    }

    def cleanupSpec() {
        FieldConverterSupplier.sketchConverter = converter
    }

    def "When a metric has too few dependent metrics, an exception is thrown."(){
        given: "One fewer dependent metrics than this metric depends on"
        List<String> dependentMetricNames = buildDependentMetricNames(maker.getDependentMetricsRequired()-1)
        maker.metrics.putAll(makeEmptyMetrics(dependentMetricNames))

        when:
        maker.make(METRIC_NAME, dependentMetricNames)

        then:
        thrown(IllegalArgumentException)
    }

   def "When a claimed dependent metric does not exist in the maker's dictionary, an exception is thrown"(){
       given: "A list of dependent metrics, one of which doesn't exist"
       List<String> dependentMetricNames = buildDependentMetricNames(maker.getDependentMetricsRequired()-1)
       maker.metrics.putAll(makeEmptyMetrics(dependentMetricNames))
       dependentMetricNames.add("I don't exist!")

       when:
       maker.make(METRIC_NAME, dependentMetricNames)

       then:
       thrown(IllegalArgumentException)
   }

    def "getNumericField returns a numeric post aggregation unchanged."(){
        given: "A logical metric containing a constant post aggregation"
        PostAggregation number = new ConstantPostAggregation("constant1", 1.0)
        TemplateDruidQuery queryTemplate = new TemplateDruidQuery([] as Set, [number] as Set)
        LogicalMetric constantMetric = new LogicalMetric(queryTemplate, new NoOpResultSetMapper(), "constant1")

        and: "The maker, with populated metric dictionary"
        maker.metrics.add(constantMetric)

        expect:
        maker.getNumericField("constant1") == number
    }

    def """Given the metric name of an aggregation metric field, getNumericField wraps the
        aggregation in a field accessor and returns the new PostAggregation."""(){
        given: "A logical metric containing a metric aggregation"
        Aggregation longSumAggregation = new LongSumAggregation("num_users_sum", "num_users")
        TemplateDruidQuery queryTemplate = new TemplateDruidQuery([longSumAggregation] as Set, [] as Set)
        LogicalMetric longSumMetric = new LogicalMetric(queryTemplate, new NoOpResultSetMapper(), "num_users_sum")

        and: "The maker, with populated metric dictionary"
        maker.metrics.add(longSumMetric)

        and: "The expected post aggregation: a field access wrapping the long sum aggregation"
        PostAggregation longSumFieldAccess = new FieldAccessorPostAggregation(longSumAggregation)

        expect:
        maker.getNumericField("num_users_sum") == longSumFieldAccess
    }

    def "Given a sketch aggregation, getNumericField wraps the sketch in a sketch estimate."(){
        given: "A sketch aggregation"
        Aggregation sketchMerge = new SketchMergeAggregation("all_users", "users", 16000)
        TemplateDruidQuery queryTemplate = new TemplateDruidQuery([sketchMerge] as Set, [] as Set)
        LogicalMetric metric = new LogicalMetric(queryTemplate, new NoOpResultSetMapper(), "all_users", "metric")

        and: "The maker, with populated metric dictionary"
        MetricMaker maker = getInstance()
        maker.metrics.add(metric)

        and: "The expected post aggregation: A sketch estimate wrapping the sketch aggregation."
        PostAggregation sketchEstimate = FieldConverterSupplier.sketchConverter.asSketchEstimate(sketchMerge)

        expect:
        maker.getNumericField("all_users") == sketchEstimate

    }

    def "When a metric has too many dependent metrics, an exception is thrown"(){
        given: "One more than the number of dependent metrics this maker depends on."
        List<String> dependentMetricNames = buildDependentMetricNames(maker.getDependentMetricsRequired()+1)
        maker.metrics.putAll(makeEmptyMetrics(dependentMetricNames))

        when:
        maker.make(METRIC_NAME, dependentMetricNames)

        then:
        thrown(IllegalArgumentException)
    }
}
