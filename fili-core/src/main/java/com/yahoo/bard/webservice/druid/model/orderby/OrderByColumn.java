// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.orderby;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;

import java.util.Objects;

/**
 * OrderByColumn class.
 */
public class OrderByColumn {
    private final String dimension;
    private final SortDirection direction;

    /**
     * Constructor.
     *
     * @param metric  a LogicalMetric
     * @param direction  sort direction
     *
     * Note: Plan is to remove this LogicalMetric based constructor and have a DruidColumn based constructor.
     */
    public OrderByColumn(LogicalMetric metric, SortDirection direction) {
        this.dimension = metric.getName();
        this.direction = direction;
    }

    /**
     * Constructor.
     *
     * @param aggregation  an Aggregation
     * @param direction  sort direction
     */
    public OrderByColumn(Aggregation aggregation, SortDirection direction) {
        this.dimension = aggregation.getName();
        this.direction = direction;
    }

    /**
     * Constructor.
     *
     * @param postAggregation  a PostAggregation
     * @param direction  sort direction
     */
    public OrderByColumn(PostAggregation postAggregation, SortDirection direction) {
        this.dimension = postAggregation.getName();
        this.direction = direction;
    }

    /**
     * Getter for sort column dimension.
     *
     * @return dimension A dimension name, a metric name, an aggregation name or a post aggregation name
     */
    public String getDimension() {
        return this.dimension;
    }

    /**
     * Getter for sort direction.
     *
     * @return direction
     */
    public SortDirection getDirection() {
        return  this.direction;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (!(o instanceof OrderByColumn)) { return false; }

        OrderByColumn that = (OrderByColumn) o;

        return
                Objects.equals(dimension, that.dimension) &&
                direction == that.direction;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dimension, direction);
    }
}
