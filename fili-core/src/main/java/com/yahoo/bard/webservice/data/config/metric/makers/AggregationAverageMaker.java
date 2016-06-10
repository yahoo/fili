// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers;

import static com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation.ArithmeticPostAggregationFunction.DIVIDE;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper;
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
import com.yahoo.bard.webservice.druid.util.FieldConverterSupplier;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.validation.constraints.NotNull;

/**
 * Makes a LogicalMetric that wraps existing metrics and average them across a coarser time grain.
 *
 * The constructed metric
 * takes aggregated data from a finer time grain (i.e. DefaultTimeGrain.DAY) and computes an average across a coarser
 * time grain (i.e. DefaultTimeGrain.WEEK). For example, given the total number of visitors to www.example.com for each
 * day of the week, we can compute the average number of daily visitors to example.com for the entire week.
 */
public class AggregationAverageMaker extends MetricMaker {

    private static final int DEPENDENT_METRICS_REQUIRED = 1;

    public static final PostAggregation COUNT_INNER = new ConstantPostAggregation("one", 1);
    public static final @NotNull Aggregation COUNT_OUTER = new LongSumAggregation("count", "one");
    public static final PostAggregation COUNT_FIELD_OUTER = new FieldAccessorPostAggregation(COUNT_OUTER);

    private final ZonelessTimeGrain innerGrain;

    /**
     * Constructor
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
        String dependantMetricName = dependentMetrics.get(0);
        TemplateDruidQuery innerDependentQuery = getDependentQuery(dependantMetricName);
        MetricField sourceMetric = innerDependentQuery.getMetricField(dependantMetricName);

        // Convert if needed
        sourceMetric = convertToSketchEstimateIfNeeded(sourceMetric);

        // Build the TemplateDruidQuery for the metric
        TemplateDruidQuery innerQuery = buildInnerQuery(sourceMetric, innerDependentQuery);
        TemplateDruidQuery outerQuery = buildOuterQuery(metricName, sourceMetric, innerQuery);

        return new LogicalMetric(outerQuery, new NoOpResultSetMapper(), metricName);
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
        PostAggregation average = new ArithmeticPostAggregation(metricDictionary, DIVIDE, sumPost, COUNT_FIELD_OUTER);
        Set<PostAggregation> outerPostAggs = new LinkedHashSet<>(Arrays.asList(average));

        // Build the resulting query template
        return new TemplateDruidQuery(outerAggs, outerPostAggs, innerQuery);
    }

    /**
     * Create the inner query for an average
     *
     * @param sourceMetric  The metric being averaged over
     * @param innerDependentQuery  The original query supporting the metric being averaged
     *
     * @return A template query representing the inner aggregation
     */
    private TemplateDruidQuery buildInnerQuery(MetricField sourceMetric, TemplateDruidQuery innerDependentQuery) {

        Set<Aggregation> newInnerAggregations = convertSketchesToSketchMerges(innerDependentQuery.getAggregations());
        Set<PostAggregation> newInnerPostAggregations = new LinkedHashSet<>();

        // If the sourceMetric is a Post Aggregator, we need to preserve it in the inner query
        if (sourceMetric instanceof PostAggregation) {
            newInnerPostAggregations.add((PostAggregation) sourceMetric);
        }

        // Build the inner query with the new aggregations and with the count
        TemplateDruidQuery innerQuery = innerDependentQuery;
        innerQuery = innerQuery.withAggregations(newInnerAggregations);
        innerQuery = innerQuery.withPostAggregations(newInnerPostAggregations);
        innerQuery = innerQuery.merge(buildTimeGrainCounterQuery());
        return innerQuery;
    }

    /**
     * If the aggregation being averaged is a sketch, it will become a sketch merge, and need to be estimated for
     * summing to work at the outer level
     *
     * @param originalSourceMetric  The metric being target for sums
     *
     * @return Either the original MetricField, or a new SketchEstimate post aggregation
     */
    private MetricField convertToSketchEstimateIfNeeded(MetricField originalSourceMetric) {
        if (originalSourceMetric instanceof SketchAggregation) {
            return FieldConverterSupplier.sketchConverter.asSketchEstimate((SketchAggregation) originalSourceMetric);
        }
        return originalSourceMetric;
    }

    /**
     * Copy a set of aggregations, replacing any sketch aggregations with sketchMerge aggregations
     *
     * @param originalAggregations  The read-only original aggregations
     *
     * @return The new aggregation set
     */
    private Set<Aggregation> convertSketchesToSketchMerges(Set<Aggregation> originalAggregations) {
        Set<Aggregation> result = new LinkedHashSet<>();
        for (Aggregation agg : originalAggregations) {
            if (agg.isSketch()) {
                result.add(FieldConverterSupplier.sketchConverter.asInnerSketch((SketchAggregation) agg));
            } else {
                result.add(agg);
            }
        }
        return result;
    }

    /**
     * Create an Aggregation for summing on a metric from an inner query
     *
     * @param innerMetric  The metric on the inner query being summed
     *
     * @return The aggregator that sums across the inner query quantity
     */
    private @NotNull Aggregation createSummingAggregator(MetricField innerMetric) {
        // Pick a name for the outer (summing) aggregation name
        String outerSummingFieldName;
        // TODO: Explain why we have to go through these naming hoops. Likely due to druid naming requirement issues
        if (!innerMetric.isSketch() && innerMetric instanceof Aggregation) {
            outerSummingFieldName = innerMetric.getName();
        } else {
            outerSummingFieldName = innerMetric.getName() + "_sum";
        }

        // Make sure we don't drop precision
        if (innerMetric.isFloatingPoint()) {
            return new DoubleSumAggregation(outerSummingFieldName, innerMetric.getName());
        }

        return new LongSumAggregation(outerSummingFieldName, innerMetric.getName());
    }

    @Override
    protected int getDependentMetricsRequired() {
        return DEPENDENT_METRICS_REQUIRED;
    }

    /**
     * Create a query with a counter field and a grain
     *
     * @return The created query
     */
    private TemplateDruidQuery buildTimeGrainCounterQuery() {
        Set<Aggregation> timedAggs = Collections.emptySet();
        Set<PostAggregation> timedPostAggs = new LinkedHashSet<>(Arrays.asList(COUNT_INNER));
        return new TemplateDruidQuery(timedAggs, timedPostAggs, innerGrain);
    }
}
