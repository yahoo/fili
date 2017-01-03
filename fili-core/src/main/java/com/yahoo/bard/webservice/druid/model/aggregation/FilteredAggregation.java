// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.druid.util.FieldConverterSupplier;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.validation.constraints.NotNull;

/**
 * FilteredAggregation wraps aggregation with the associated filter.
 */
public class FilteredAggregation extends Aggregation {
    private final Filter filter;
    private final Aggregation aggregation;

    /**
     * Constructor.
     *
     * @param name  Name of the filtered aggregator
     * @param aggregation  Existing aggregator being filtered
     * @param filter  filter to apply to that aggregator
     */
    public FilteredAggregation(@NotNull String name, Aggregation aggregation, Filter filter) {
        super(name, aggregation.getFieldName());
        this.filter = filter;
        this.aggregation = aggregation.withName(name);
    }

    /**
     * Constructor.
     *
     * @param name  Name of the filtered aggregator
     * @param fieldName  Field name to be considered to apply the metric filter
     * @param aggregation  Existing aggregator being filtered
     * @param filter  filter to apply to that aggregator
     *
     * @deprecated Filtered Aggregations do not have their own field name, they use the one from their aggregator
     */
    @Deprecated
    public FilteredAggregation(@NotNull String name, String fieldName, Aggregation aggregation, Filter filter) {
        super(name, fieldName);
        this.filter = filter;
        this.aggregation = aggregation.withName(name).withFieldName(fieldName);
    }

    /**
     * Splits an Aggregation for 2-pass aggregation into an inner filtered aggregation &amp; outer aggregation. The
     * outer aggregation is obtained by unwrapping the inner filtered aggregation and getting just the aggregation.
     * The outer aggregation fieldName will reference the inner aggregation name. The inner aggregation is unmodified.
     *
     * @return A pair where pair.left is the outer aggregation and pair.right is the inner.
     */
    @Override
    public Pair<Aggregation, Aggregation> nest() {
        String nestingName = this.getName();
        Aggregation outer = this.getAggregation().withFieldName(nestingName);
        return new ImmutablePair<>(outer, this);
    }

    @JsonIgnore
    @Override
    public Set<Dimension> getDependentDimensions() {
        return Stream.concat(aggregation.getDependentDimensions().stream(), getFilterDimensions().stream())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private Set<Dimension> getFilterDimensions() {
        return FieldConverterSupplier.metricsFilterSetBuilder.gatherFilterDimensions(filter);
    }

    @JsonIgnore
    @Override
    public String getFieldName() {
        return aggregation.getFieldName();
    }

    @Override
    @JsonIgnore
    public String getName() {
        return aggregation.getName();
    }

    @JsonProperty(value = "aggregator")
    public Aggregation getAggregation() {
       return aggregation;
    }

    public Filter getFilter() {
        return filter;
    }

    @Override
    public String getType() {
        return "filtered";
    }

    @Override
    public Aggregation withFieldName(String fieldName) {
        return new FilteredAggregation(getName(), getAggregation().withFieldName(fieldName), getFilter());
    }

    @Override
    public Aggregation withName(String name) {
        return new FilteredAggregation(name, getAggregation(), getFilter());
    }

    /**
     * Creates a new Filtered Aggregation with the provided filter.
     *
     * @param filter  metricFilter Filter object
     *
     * @return new FilteredAggregation instance with the provided filter
     */
    public Aggregation withFilter(Filter filter) {
        return new FilteredAggregation(getName(), getAggregation(), filter);
    }

    /**
     * Creates a new Filtered Aggregation with the provided aggregation.
     *
     * @param aggregation  aggregation of the logical metric
     *
     * @return new FilteredAggregation instance with the provided aggregation
     */
    public Aggregation withAggregation(Aggregation aggregation) {
        return new FilteredAggregation(aggregation.getName(), aggregation, getFilter());
    }

    @JsonIgnore
    @Override
    public boolean isSketch() {
        return aggregation.isSketch();
    }

    @JsonIgnore
    @Override
    public boolean isFloatingPoint() {
        return aggregation.isFloatingPoint();
    }
}
