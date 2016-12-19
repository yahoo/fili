// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers;

import static com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation
        .ArithmeticPostAggregationFunction.DIVIDE;
import static com.yahoo.bard.webservice.druid.util.FieldConverterSupplier.sketchConverter;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.data.time.ZonelessTimeGrain;
import com.yahoo.bard.webservice.druid.model.MetricField;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.aggregation.DoubleSumAggregation;
import com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation;
import com.yahoo.bard.webservice.druid.model.aggregation.SketchAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.ConstantPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;

import com.google.common.collect.ImmutableSet;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * Makes a LogicalMetric that wraps existing metrics and average them across a coarser time grain.
 * <p>
 * The constructed metric
 * takes aggregated data from a finer time grain (i.e. DefaultTimeGrain.DAY) and computes an average across a coarser
 * time grain (i.e. DefaultTimeGrain.WEEK). For example, given the total number of visitors to www.example.com for each
 * day of the week, we can compute the average number of daily visitors to example.com for the entire week.
 * <p>
 * A nested average requires the following columns, an inner query constant, and outer query sum of that constant,
 * an inner query aggregation or post aggregation field to sum, and outer query sum of the inner field, and
 * finally a post aggregation in the outer query taking the division of the sum and the constant sum.
 */
public class AggregationAverageMaker extends MetricMaker {

    private static final int DEPENDENT_METRICS_REQUIRED = 1;

    public static final PostAggregation COUNT_INNER = new ConstantPostAggregation("one", 1);
    public static final @NotNull Aggregation COUNT_OUTER = new LongSumAggregation("count", "one");
    public static final PostAggregation COUNT_FIELD_OUTER = new FieldAccessorPostAggregation(COUNT_OUTER);

    private final ZonelessTimeGrain innerGrain;

    /**
     * Constructor.
     *
     * @param metrics  A mapping of metric names to the corresponding LogicalMetrics. Used to resolve metric names
     * when making the logical metric.
     * @param innerGrain  The time grain across which queries should aggregate.
     */
    public AggregationAverageMaker(MetricDictionary metrics, ZonelessTimeGrain innerGrain) {
        super(metrics);
        this.innerGrain = innerGrain;
    }

    @Override
    protected LogicalMetric makeInner(String metricName, List<String> dependentMetrics) {
        // Get the Metric that is being averaged over
        LogicalMetric dependentMetric = metrics.get(dependentMetrics.get(0));

        // Get the field being subtotalled in the inner query
        MetricField sourceMetric = convertToSketchEstimateIfNeeded(dependentMetric.getMetricField());

        // Build the TemplateDruidQuery for the metric
        TemplateDruidQuery innerQuery = buildInnerQuery(sourceMetric, dependentMetric.getTemplateDruidQuery());
        TemplateDruidQuery outerQuery = buildOuterQuery(metricName, sourceMetric, innerQuery);

        return new LogicalMetric(outerQuery, NO_OP_MAPPER, metricName);
    }

    /**
     * Build the outer query for the average. It will have a sum, a count, and a post agg to divide them.
     *
     * @param metricDictionary  The metric dictionary name for the metric being created (usually an api name)
     * @param sourceMetric  The metric in the inner query being averaged
     * @param innerQuery  The inner template query being summed over
     *
     * @return The query for creating a logical metric
     */
    private TemplateDruidQuery buildOuterQuery(
            String metricDictionary,
            MetricField sourceMetric,
            TemplateDruidQuery innerQuery
    ) {
        // Build the outer aggregations
        Aggregation sum = createSummingAggregator(sourceMetric);
        Set<Aggregation> outerAggs = new LinkedHashSet<>(Arrays.asList(sum, COUNT_OUTER));

        // Build the average post aggregation
        FieldAccessorPostAggregation sumPost = new FieldAccessorPostAggregation(sum);
        PostAggregation average = new ArithmeticPostAggregation(
                metricDictionary,
                DIVIDE,
                Arrays.asList(sumPost, COUNT_FIELD_OUTER)
        );
        Set<PostAggregation> outerPostAggs = Collections.singleton(average);

        // Build the resulting query template
        return new TemplateDruidQuery(outerAggs, outerPostAggs, innerQuery);
    }

    /**
     * Create the inner query for an average.
     *
     * @param sourceMetric  The metric being averaged over
     * @param innerDependentQuery  The original query supporting the metric being averaged
     *
     * @return A template query representing the inner aggregation
     */
    private TemplateDruidQuery buildInnerQuery(MetricField sourceMetric, TemplateDruidQuery innerDependentQuery) {
        Set<Aggregation> newInnerAggregations = convertSketchesToSketchMerges(innerDependentQuery.getAggregations());

        Set<PostAggregation> newInnerPostAggregations = !(sourceMetric instanceof PostAggregation) ?
                Collections.emptySet() : ImmutableSet.of((PostAggregation) sourceMetric);

        // Build the inner query with the new aggregations and with the count
        return innerDependentQuery.withAggregations(newInnerAggregations)
                .withPostAggregations(newInnerPostAggregations)
                .merge(buildTimeGrainCounterQuery());
    }

    /**
     * If the aggregation being averaged is a sketch, the inner query must convert it to a numerical type so that it
     * can be summed.
     *
     * @param originalSourceMetric  The metric being target for sums
     *
     * @return Either the original MetricField, or a new SketchEstimate post aggregation
     */
    private MetricField convertToSketchEstimateIfNeeded(MetricField originalSourceMetric) {
        return !(originalSourceMetric instanceof SketchAggregation) ?
                originalSourceMetric :
                sketchConverter.asSketchEstimate((SketchAggregation) originalSourceMetric);
    }

    /**
     * Copy a set of aggregations, replacing any sketch aggregations with sketchMerge aggregations.
     * This is an artifact of earlier sketch code which didn't have a single sketch type that could be used without
     * finalization in inner queries.
     *
     * @param originalAggregations  The read-only original aggregations
     *
     * @return The new aggregation set
     */
    private Set<Aggregation> convertSketchesToSketchMerges(Set<Aggregation> originalAggregations) {
        return originalAggregations.stream()
                .map(agg -> ! agg.isSketch() ? agg : sketchConverter.asInnerSketch((SketchAggregation) agg))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Create an Aggregation for summing on a metric from an inner query.
     * The base metric for this maker has a name in the request schema.  Daily average aggregates over the summand it
     * creates in the inner query, which is based on the original metric name.  If the original metric field is also a
     * sum, the outer aggregation of that metric and the outer aggregation of the daily average do not need to be
     * distinguished.  However if the original metric field was aggregating on a non numeric type, the daily average
     * will appeaed '_sum' to the original name for it's outer sum.
     *
     * @param innerMetric  The metric on the inner query being summed
     *
     * @return The aggregator that sums across the inner query quantity
     */
    private @NotNull Aggregation createSummingAggregator(MetricField innerMetric) {
        // Pick a name for the outer (summing) aggregation name
        String outerSummingName = (!innerMetric.isSketch() && innerMetric instanceof Aggregation) ?
                innerMetric.getName() :
                innerMetric.getName() + "_sum";

        // Make sure we don't drop precision
        return innerMetric.isFloatingPoint() ?
                new DoubleSumAggregation(outerSummingName, innerMetric.getName()) :
                new LongSumAggregation(outerSummingName, innerMetric.getName());

    }

    @Override
    protected int getDependentMetricsRequired() {
        return DEPENDENT_METRICS_REQUIRED;
    }

    /**
     * Create a query with a counter field and a grain.
     *
     * @return The created query
     */
    private TemplateDruidQuery buildTimeGrainCounterQuery() {
        return new TemplateDruidQuery(Collections.emptySet(), Collections.singleton(COUNT_INNER), innerGrain);
    }
}
