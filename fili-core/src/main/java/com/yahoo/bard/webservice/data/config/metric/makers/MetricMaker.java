// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.config.LogicalMetricInfo;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper;
import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper;
import com.yahoo.bard.webservice.druid.model.MetricField;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.SketchEstimatePostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.ThetaSketchEstimatePostAggregation;
import com.yahoo.bard.webservice.druid.util.FieldConverterSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Metric maker produces new metrics from existing metrics or raw configuration.
 */
public abstract class MetricMaker {

    private static final Logger LOG = LoggerFactory.getLogger(MetricMaker.class);

    public static final NoOpResultSetMapper NO_OP_MAPPER = new NoOpResultSetMapper();
    public static final Function<String, ResultSetMapper> NO_OP_MAP_PROVIDER = (ignore) -> NO_OP_MAPPER;

    private static final String SKETCH_REQUIRED_FORMAT = "Field must be a sketch: %s but is: %s";
    private static final String INCORRECT_NUMBER_OF_DEPS_FORMAT = "%s got %d of %d expected metrics";
    private static final String MISSING_DEP_FORMAT = "Dependent metric %s is not in the metric dictionary";

    /**
     * The metric dictionary from which dependent metrics will be resolved.
     */
    public final MetricDictionary metrics;

    /**
     * Construct a fully specified MetricMaker.
     *
     * @param metrics  A mapping of metric names to the corresponding LogicalMetrics. Used to resolve metric names
     * when making the logical metric.
     */
    public MetricMaker(MetricDictionary metrics) {
        this.metrics = metrics;
    }

    /**
     * Make the metric.
     * <p>
     * This method also sanity-checks the dependent metrics to make sure that they
     * are metrics we have built and are in the metric dictionary.
     * <p>
     * Also sanity-checks that the number of dependent metrics are correct for the maker.
     *
     * @param metricName  Name for the metric we're making
     * @param dependentMetrics  Metrics this metric depends on
     *
     * @return The new logicalMetric
     *
     * @deprecated logical metric needs more config-richness to not just configure metric name, but also metric long
     * name, description, etc. Use {@link #make(LogicalMetricInfo, List)} instead.
     */
    @Deprecated
    public LogicalMetric make(String metricName, List<String> dependentMetrics) {
        return make(new LogicalMetricInfo(metricName), dependentMetrics);
    }

    /**
     * Make the metric.
     * <p>
     * This method also sanity-checks the dependent metrics to make sure that they
     * are metrics we have built and are in the metric dictionary.
     * <p>
     * Also sanity-checks that the number of dependent metrics are correct for the maker.
     *
     * @param logicalMetricInfo  Logical metric info provider
     * @param dependentMetrics  Metrics this metric depends on
     *
     * @return The new logicalMetric
     */
    public LogicalMetric make(LogicalMetricInfo logicalMetricInfo, List<String> dependentMetrics) {
        // Check that all of the dependent metrics are in the dictionary
        assertDependentMetricsExist(dependentMetrics);

        // Check that we have the right number of metrics
        assertRequiredDependentMetricCount(logicalMetricInfo.getName(), dependentMetrics);

        // Have the subclass actually build the metric
        return this.makeInner(logicalMetricInfo, dependentMetrics);
    }

    /**
     * Checks that we have the right number of metrics, and throws an exception if we don't.
     *
     * @param dictionaryName  Name of the metric being made
     * @param dependentMetrics  List of dependent metrics needed to make the metric
     */
    protected void assertRequiredDependentMetricCount(String dictionaryName, List<String> dependentMetrics) {
        int requiredCount = getDependentMetricsRequired();
        int actualCount = dependentMetrics.size();
        if (actualCount != requiredCount) {
            String message = String.format(
                    INCORRECT_NUMBER_OF_DEPS_FORMAT,
                    dictionaryName,
                    actualCount,
                    requiredCount
            );
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Checks that the dependent metrics are in the dictionary, and throws an exception if any are not.
     *
     * @param dependentMetrics  List of dependent metrics needed to check
     */
    protected void assertDependentMetricsExist(List<String> dependentMetrics) {
        for (String dependentMetric : dependentMetrics) {
            if (!metrics.containsKey(dependentMetric)) {
                String message = String.format(
                        MISSING_DEP_FORMAT,
                        dependentMetric
                );
                LOG.error(message);
                throw new IllegalArgumentException(message);
            }
        }
    }

    /**
     * Make the metric.
     *
     * @param metricName  Name for the metric we're making
     * @param dependentMetric  Metric this metric depends on
     *
     * @return The new logicalMetric
     */
    public LogicalMetric make(String metricName, String dependentMetric) {
        return this.make(metricName, Collections.singletonList(dependentMetric));
    }

    /**
     * Delegated to for actually making the metric.
     *
     * @param metricName  Name for the metric we're making
     * @param dependentMetrics  Metrics this metric depends on
     *
     * @return the new logicalMetric
     *
     * @deprecated logical metric needs more config-richness to not just configure metric name, but also metric long
     * name, description, etc. Use {@link #makeInner(LogicalMetricInfo, List)} instead.
     */
    @Deprecated
    protected LogicalMetric makeInner(String metricName, List<String> dependentMetrics) {
        return makeInner(new LogicalMetricInfo(metricName), dependentMetrics);
    }

    /**
     * Delegated to for actually making the metric.
     *
     * @param logicalMetricInfo  Logical metric info provider
     * @param dependentMetrics  Metrics this metric depends on
     *
     * @return the new logicalMetric
     */
    protected LogicalMetric makeInner(LogicalMetricInfo logicalMetricInfo, List<String> dependentMetrics) {
        String message = String.format(
                "Current implementation of MetricMaker '%s' does not support makeInner operation",
                this.getClass().getSimpleName()
        );
        LoggerFactory.getLogger(MetricMaker.class).error(message);
        throw new UnsupportedOperationException(message);
    }

    /**
     * Get the number of dependent metrics the maker requires for making the metric.
     *
     * @return the number of dependent metrics the maker requires for making the metric
     */
    protected abstract int getDependentMetricsRequired();

    /**
     * Fetch the TemplateDruidQuery of the dependent metric from the Metric Dictionary.
     *
     * @param name  Name of the metric to fetch the template druid query from
     *
     * @return The template druid query of the metric
     *
     * @deprecated Instead get the metric in the calling function and then get the TDQ out only if necessary
     */
    @Deprecated
    protected TemplateDruidQuery getDependentQuery(String name) {
        LogicalMetric dependentMetric = metrics.get(name);
        return dependentMetric.getTemplateDruidQuery();
    }

    /**
     * A helper function returning the resulting aggregation set from merging one or more template druid queries.
     *
     * @param names  Names of the metrics to fetch and merge the aggregation clauses from
     *
     * @return The merged query
     */
    protected TemplateDruidQuery getMergedQuery(List<String> names) {
        // Merge in any additional queries
        return names.stream()
                .map(metrics::get)
                .map(LogicalMetric::getTemplateDruidQuery)
                .reduce(TemplateDruidQuery::merge)
                .orElseThrow(() -> {
                    String message = "At least 1 name is needed to merge aggregations";
                    LOG.error(message);
                    return new IllegalArgumentException(message);
                });
    }

    /**
     * Prepare a post aggregation for a field expecting a numeric value.
     * <p>
     * The post-agg is created per the following heuristics:
     * <dl>
     *     <dt>If it's an aggregator
     *     <dd>wrap it in a field accessor
     *     <dt>If it's a sketch
     *     <dd>wrap it in a sketch estimate
     *     <dt>If it's already a numeric post aggregator
     *     <dd>simply return it
     * </dl>
     *
     * @param fieldName  The name for the aggregation or post aggregation column being gotten
     *
     * @return A post aggregator representing a number field value
     *
     * @deprecated use the static version {@link #getNumericField(MetricField)} by preference
     */
    @Deprecated
    protected PostAggregation getNumericField(String fieldName) {
        return getNumericField(metrics.get(fieldName).getMetricField());
    }

    /**
     * Prepare a post aggregation for a field expecting a numeric value.
     * <p>
     * The post-agg is created per the following heuristics:
     * <dl>
     *     <dt>If it's an aggregator
     *     <dd>wrap it in a field accessor
     *     <dt>If it's a sketch
     *     <dd>wrap it in a sketch estimate
     *     <dt>If it's already a numeric post aggregator
     *     <dd>simply return it
     * </dl>
     *
     * @param field  The field being coerced
     *
     * @return A post aggregator representing a number field value
     */
    protected static PostAggregation getNumericField(MetricField field) {
        // If the field is a sketch, wrap it in a sketch estimate, if it's an aggregation, create a post aggregation
        // Otherwise it is a number post aggregation already
        return field.isSketch() ?
                FieldConverterSupplier.sketchConverter.asSketchEstimate(field) :
                field instanceof Aggregation ?
                        new FieldAccessorPostAggregation((Aggregation) field) :
                        (PostAggregation) field;
    }

    /**
     * Prepare a post aggregation for a field expecting a sketch value.
     * <p>
     * The post-agg is created per the following heuristics:
     * <dl>
     *     <dt>If it's an aggregator
     *     <dd>wrap it in a field accessor
     *     <dt>If it's a sketch estimate
     *     <dd>unwrap the sketch estimate
     *     <dt>If it's already a sketch post aggregator
     *     <dd>simply return it
     *     <dt>Otherwise</dt>
     *     <dd>This is an illegal field</dd>
     * </dl>
     *
     * @param fieldName  The name for the aggregation or post aggregation column being gotten
     *
     * @return A post aggregator representing a number field value
     *
     * @deprecated use the static version {@link #getSketchField(MetricField)} by preference
     */
    @Deprecated
    protected PostAggregation getSketchField(String fieldName) {
        // Get the field
        return getSketchField(metrics.get(fieldName).getMetricField());
    }

    /**
     * Prepare a post aggregation for a field expecting a sketch value.
     * <p>
     * The post-agg is created per the following heuristics:
     * <dl>
     *     <dt>If it's an aggregator
     *     <dd>wrap it in a field accessor
     *     <dt>If it's a sketch estimate
     *     <dd>unwrap the sketch estimate
     *     <dt>If it's already a sketch post aggregator
     *     <dd>simply return it
     *     <dt>Otherwise</dt>
     *     <dd>This is an illegal field</dd>
     * </dl>
     *
     * @param field  The field being coerced
     *
     * @return A post aggregator representing a number field value
     */
    protected static PostAggregation getSketchField(MetricField field) {
        // SketchEstimatePostAggregations are just wrappers for the actual PostAggregation we want to return
        if (field instanceof SketchEstimatePostAggregation) {
            return ((SketchEstimatePostAggregation) field).getField();
        }

        if (field instanceof ThetaSketchEstimatePostAggregation) {
            return ((ThetaSketchEstimatePostAggregation) field).getField();
        }

        // Check for sketches, since we require them after this point
        if (!field.isSketch()) {
            String message = String.format(SKETCH_REQUIRED_FORMAT, field.getName(), field);
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }

        // Handle it if it's an Aggregation (ie. wrap it in a fieldAccessorPostAggregation)
        // If it's already a post-agg, we're good, and if it was an agg, we've already wrapped it
        return field instanceof Aggregation ?
                new FieldAccessorPostAggregation((Aggregation) field) :
                (PostAggregation) field;
    }
}
