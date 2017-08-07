// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper
import com.yahoo.bard.webservice.data.time.TimeGrain
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.aggregation.DoubleSumAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.SketchCountAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.SketchMergeAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.SketchEstimatePostAggregation
import com.yahoo.bard.webservice.druid.util.FieldConverterSupplier
import com.yahoo.bard.webservice.druid.util.FieldConverters
import com.yahoo.bard.webservice.druid.util.SketchFieldConverter

import spock.lang.Shared
import spock.lang.Specification

class AggregationAverageMakerSpec extends Specification{

    static final String NAME = "users"
    static final String DESCRIPTION  = NAME
    static final String ESTIMATE_NAME = "users_estimate"
    static final String ESTIMATE_SUM_NAME = "users_estimate_sum"
    static final int SKETCH_SIZE = 16000
    static final TimeGrain INNER_GRAIN = DAY

    @Shared
    FieldConverters converter = FieldConverterSupplier.sketchConverter
    Aggregation sketchMerge

    def setup(){
        sketchMerge = new SketchMergeAggregation(NAME, NAME, SKETCH_SIZE)
        //Initializing the Sketch field converter
        FieldConverterSupplier.sketchConverter = new SketchFieldConverter();
    }

    def cleanupSpec() {
        FieldConverterSupplier.sketchConverter = converter
     }

    def "Build a correct LogicalMetric when passed a sketch count aggregation."(){
        given: "a logical metric for counting the number of users each day"
        Aggregation userSketchCount = new SketchCountAggregation(NAME, NAME, 16000)
        TemplateDruidQuery sketchQuery = new TemplateDruidQuery(
                [userSketchCount] as Set,
                [] as Set
        )
        LogicalMetric sketchCountMetric = buildDependentMetric(sketchQuery)

        and: """an AggregationAverageMaker. Note that the AggregationAverageMaker takes the _inner_ grain as an
                argument not the _outer_ grain."""
        MetricMaker maker = new AggregationAverageMaker(new MetricDictionary(), INNER_GRAIN)
        maker.metrics.add(sketchCountMetric)

        and: """a test-specific inner post aggregation and the expected metric. The test-specific inner post aggregation
                estimates the size of userSketchCount."""
        PostAggregation sketchEstimate = new SketchEstimatePostAggregation(
                ESTIMATE_NAME,
                new FieldAccessorPostAggregation(userSketchCount)
        )
        LogicalMetric expectedMetric = buildExpectedMetric(sketchEstimate)

        expect:
        maker.make(NAME, NAME).equals(expectedMetric)
    }

    def "Build a correct LogicalMetric when passed a sketch merge and sketch estimate"(){
        given: """A logical metric for counting the number of users each day, using a sketch merge and sketch
            estimate rather than a sketch count."""
        PostAggregation sketchEstimate = new SketchEstimatePostAggregation(
                ESTIMATE_NAME,
                new FieldAccessorPostAggregation(sketchMerge)
        )
        TemplateDruidQuery sketchMergeAndEstimateQuery = new TemplateDruidQuery(
                [sketchMerge] as Set,
                [sketchEstimate] as Set
        )
        LogicalMetric sketchMergeAndEstimateMetric = buildDependentMetric(sketchMergeAndEstimateQuery)

        and:
        MetricMaker maker = new AggregationAverageMaker(new MetricDictionary(), INNER_GRAIN)
        maker.metrics.add(sketchMergeAndEstimateMetric)

        and: """The expected metric. Identical to the expected metric from the previous test, except that the
            sketchEstimate post aggregation is accessing a sketch merge, rather than a sketch count aggregation."""
        LogicalMetric expectedMetric = buildExpectedMetric(sketchEstimate)

        expect:
        maker.make(NAME, NAME).equals(expectedMetric)
    }

    def "Build a correct LogicalMetric when passed only a sketch merge."(){
        given: "A Logical Metric containing only a sketch estimate"
        Aggregation sketchMerge = new SketchMergeAggregation(NAME, NAME, SKETCH_SIZE)
        TemplateDruidQuery sketchEstimateQuery = new TemplateDruidQuery(
                [sketchMerge] as Set,
                [] as Set
        )
        LogicalMetric sketchEstimateMetric = buildDependentMetric(sketchEstimateQuery)

        and:
        MetricMaker maker = new AggregationAverageMaker(new MetricDictionary(), INNER_GRAIN)
        maker.metrics.add(sketchEstimateMetric)

        and: """the expected metric. Note that a sketch estimate is expected to be added automatically by the
                AggregationAverageMaker."""
        PostAggregation sketchEstimate = new SketchEstimatePostAggregation(
                ESTIMATE_NAME,
                new FieldAccessorPostAggregation(sketchMerge)
        )
        LogicalMetric expectedMetric = buildExpectedMetric(sketchEstimate)

        expect:
        maker.make(NAME, NAME).equals(expectedMetric)
    }

    LogicalMetric buildDependentMetric(TemplateDruidQuery dependentQuery){
        return new LogicalMetric(dependentQuery, new NoOpResultSetMapper(), NAME, DESCRIPTION)
    }
    /**
     * Builds the LogicalMetric expected by the tests.
     * <p>
     * The expected metric contains a nested query. The inner query performs a sketch merge
     * of users across the time grain of day, giving us a sketch of all users for each day.
     * The inner query then performs a user provided post aggregation (typically some kind of sketch estimate), as
     * well as a constant post aggregation that assigns to each record the number 1 (which
     * will be used for counting).
     * <p>
     * The outer query computes the average daily users across each week. First, it sums up all the estimates
     * from the inner query. Then it sums up all of the counts (giving us the number of days with data).
     * Finally, it divides the sketch estimate sum by the count sum.
     * <p>
     * Note that manually counting the days is a workaround because the Druid count aggregator didn't do
     * what we needed when the AggregationAverageMaker was implemented. That may have changed.
     * <p>
     * Building the usersSketchEstimate post aggregation is a little bit weird, because of some workarounds to
     * get around sketch count weirdness. A sketch count aggregation is converted into a sketch merge aggregation
     * and sketch estimate post aggregation. However, the sketch estimate post aggregation uses the original sketch
     * aggregation (i.e. sketch count) as the aggregation in its field, not the constructed sketch merge. This
     * doesn't seem to affect the correctness of the program, but it does mean that the manually constructed
     * PostAggregation has to use a sketch count as its field in order to pass the equality test.
     *
     * @param innerPostAggregation The test-specific post aggregation to be added to the inner query.
     * @return The LogicalMetric expected by the tests
     */
    LogicalMetric buildExpectedMetric(PostAggregation innerPostAggregation){
        Set<Aggregation> innerAggregations = [sketchMerge] as LinkedHashSet
        Set<PostAggregation> innerPostAggregations = [innerPostAggregation, AggregationAverageMaker.COUNT_INNER]
        TemplateDruidQuery innerQueryTemplate = new TemplateDruidQuery(
                innerAggregations,
                innerPostAggregations,
                DAY
        )

        Aggregation outerSum = new DoubleSumAggregation(ESTIMATE_SUM_NAME, ESTIMATE_NAME)
        FieldAccessorPostAggregation outerSumLookup = new FieldAccessorPostAggregation(outerSum)
        PostAggregation average = new ArithmeticPostAggregation(
                NAME,
                ArithmeticPostAggregation.ArithmeticPostAggregationFunction.DIVIDE,
                [outerSumLookup, AggregationAverageMaker.COUNT_FIELD_OUTER]
        )
        TemplateDruidQuery outerQuery = new TemplateDruidQuery(
                [outerSum, AggregationAverageMaker.COUNT_OUTER] as LinkedHashSet,
                [average] as LinkedHashSet,
                innerQueryTemplate
        )

        return new LogicalMetric(outerQuery, new NoOpResultSetMapper(), NAME, DESCRIPTION)
    }
}
