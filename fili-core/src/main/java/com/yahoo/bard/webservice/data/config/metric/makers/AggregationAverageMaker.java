// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers;

import static com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation.ArithmeticPostAggregationFunction.DIVIDE;
import static com.yahoo.bard.webservice.druid.util.FieldConverterSupplier.getSketchConverter;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.data.metric.protocol.DefaultSystemMetricProtocols;
import com.yahoo.bard.webservice.data.metric.protocol.ProtocolSupport;
import com.yahoo.bard.webservice.data.metric.protocol.protocols.ReaggregationProtocol;
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
 * A nested average requires the following columns:
 * <ul>
 *    <li>an inner query constant one</li>
 *    <li>an inner query numeric aggregation or post aggregation field to sum</li>
 *    <li>and outer query sum of the constant which provide a row count</li>
 *    <li>an outer query sum of the inner numeric summand</li>
 *    <li>finally, a post aggregation in the outer query dividing the sum by the count</li>
 * </ul>
 */
public class AggregationAverageMaker extends BaseProtocolMetricMaker {

    private static final int DEPENDENT_METRICS_REQUIRED = 1;

    private static final String RENAMED_AVERAGER_PREFIX = "__averager_renamed_";

    public static final String PROTOCOL_NAME = ReaggregationProtocol.REAGGREGATION_CONTRACT_NAME;

    public static final PostAggregation COUNT_INNER = new ConstantPostAggregation("one", 1);
    public static final @NotNull Aggregation COUNT_OUTER = new LongSumAggregation("count", "one");
    public static final PostAggregation COUNT_FIELD_OUTER = new FieldAccessorPostAggregation(COUNT_OUTER);

    private final ZonelessTimeGrain innerGrain;

    protected int getDependentMetricsRequired() {
        return DEPENDENT_METRICS_REQUIRED;
    }

    /**
     * Constructor.
     *
     * @param metrics  A mapping of metric names to the corresponding LogicalMetrics. Used to resolve metric names
     * when making the logical metric.
     * @param innerGrain  The time grain across which queries should aggregate.
     */
    public AggregationAverageMaker(MetricDictionary metrics, ZonelessTimeGrain innerGrain) {
        this(metrics, innerGrain, DefaultSystemMetricProtocols.getStandardProtocolSupport());
    }

    /**
     * Constructor.
     *
     * Any aggregation average maker will disable the protocol {@link ReaggregationProtocol#INSTANCE}
     *
     * @param metrics  A mapping of metric names to the corresponding LogicalMetrics. Used to resolve metric names
     * when making the logical metric.
     * @param innerGrain  The time grain across which queries should aggregate.
     * @param protocolSupport The base protocol support for this system
     */
    public AggregationAverageMaker(
            MetricDictionary metrics,
            ZonelessTimeGrain innerGrain,
            ProtocolSupport protocolSupport
    ) {
        super(metrics, protocolSupport.blacklistProtocol(PROTOCOL_NAME));
        this.innerGrain = innerGrain;
    }

    @Override
    protected String getRenamedMetricNameWithPrefix(String name) {
        return RENAMED_AVERAGER_PREFIX + name;
    }

    @Override
    protected TemplateDruidQuery makePartialQuery(
            final LogicalMetricInfo logicalMetricInfo,
            final List<LogicalMetric> dependentMetrics
    ) {
        LogicalMetric dependentMetric = dependentMetrics.get(0);
        MetricField sourceMetric = convertToSketchEstimateIfNeeded(dependentMetric.getMetricField());

        TemplateDruidQuery innerQuery = buildInnerQuery(sourceMetric, dependentMetric.getTemplateDruidQuery());
        return buildOuterQuery(logicalMetricInfo.getName(), sourceMetric, innerQuery);
    }

    /**
     * Build the outer query for the average. It will have a sum, a count, and a post agg to divide them.
     *
     * @param metricName  The metric dictionary name for the metric being created (usually an api name)
     * @param sourceMetric  The metric in the inner query being averaged
     * @param innerQuery  The inner template query being summed over
     *
     * @return The query for creating a logical metric
     */
    private TemplateDruidQuery buildOuterQuery(
            String metricName,
            MetricField sourceMetric,
            TemplateDruidQuery innerQuery
    ) {
        // Build the outer aggregations
        Aggregation sum = createSummingAggregator(sourceMetric);
        Set<Aggregation> outerAggs = new LinkedHashSet<>(Arrays.asList(sum, COUNT_OUTER));

        // Build the average post aggregation
        FieldAccessorPostAggregation sumPost = new FieldAccessorPostAggregation(sum);
        PostAggregation average = new ArithmeticPostAggregation(
                metricName,
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
        Set<PostAggregation> newInnerPostAggregations = (sourceMetric instanceof PostAggregation) ?
                ImmutableSet.of((PostAggregation) sourceMetric) :
                Collections.emptySet();

        // Build the inner query with the new aggregations and with the count
        return innerDependentQuery.withAggregations(innerDependentQuery.getAggregations())
                .withPostAggregations(newInnerPostAggregations)
                .merge(buildTimeGrainCounterQuery());
    }

    /**
     * Create an Aggregation for summing on a metric from an inner query.
     * <p>
     * If the original metric that is being averaged is used together in a query with the averaging metric, then both
     * the original aggregator name and the summing aggregator used by the average metric may appear together in the
     * outer query. If they share the same name but different definitions, there would be a conflict.  This can arise
     * if the original metric used a post aggregation or if it contained a non numeric (e.g. sketch) aggregation.
     * <p>
     * We use the convention of changing the name for the average summing aggregation by adding the suffix '_sum'.
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

    /**
     * Create a query with a counter field and a grain.
     *
     * @return The created query
     */
    private TemplateDruidQuery buildTimeGrainCounterQuery() {
        return new TemplateDruidQuery(Collections.emptySet(), Collections.singleton(COUNT_INNER), innerGrain);
    }

    /**
     * Copy a set of aggregations, replacing any sketch aggregations with sketchMerge aggregations.
     * This is an artifact of earlier sketch code which didn't have a single sketch type that could be used without
     * finalization in inner queries.
     *
     * @param originalAggregations  The read-only original aggregations
     *
     * @return The new aggregation set
     *
     * @deprecated This will become unnecessary when the old sketch library is removed
     */
    @Deprecated
    protected Set<Aggregation> convertSketchesToSketchMerges(Set<Aggregation> originalAggregations) {
        return originalAggregations.stream()
                .map(agg -> agg.isSketch() ? getSketchConverter().asInnerSketch((SketchAggregation) agg) : agg)
                .collect(Collectors.toCollection(LinkedHashSet::new));
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
        return originalSourceMetric instanceof SketchAggregation ?
                getSketchConverter().asSketchEstimate((SketchAggregation) originalSourceMetric) :
                originalSourceMetric;
    }
}
