// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.data.metric.MetricColumn;
import com.yahoo.bard.webservice.druid.model.MetricField;

import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A parent class for most schema implementations.
 */
public class BaseSchema implements Schema {

    private final LinkedHashSet<Column> columns;

    /**
     * Constructor.
     *
     * @param columns  The columns for this schema.
     */
    protected BaseSchema(Iterable<Column> columns) {
        this.columns = Sets.newLinkedHashSet(columns);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BaseSchema)) {
            return false;
        }

        BaseSchema that = (BaseSchema) o;
        return Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columns);
    }

    @Override
    public LinkedHashSet<Column> getColumns() {
        return columns;
    }


    /**
     * Create a list of columns from a stream of dimensions and metrics.
     *
     * @param dimensions  The stream of dimensions to columnize
     * @param metricNames  A stream of aggregations and PostAggregations to columnize
     *
     * @return A list of columns
     */
    public static List<Column> buildColumns(Stream<Dimension> dimensions, Stream<MetricField> metricNames) {
        return Streams.concat(
                dimensions.map(DimensionColumn::new),
                metricNames.map(MetricField::getName).map(MetricColumn::new)
        ).collect(Collectors.toList());
    }
}
