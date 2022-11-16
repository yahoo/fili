// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.orderby;

import static com.yahoo.bard.webservice.druid.model.orderby.OrderByColumnType.UNKNOWN;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.SORT_DIRECTION_INVALID;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.druid.model.MetricField;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Objects;

/**
 * OrderByColumn class.
 */
public class OrderByColumn {
    public static final SortDirection DEFAULT_DIRECTION = SortDirection.DESC;

    private final String column;
    private final SortDirection direction;
    private final OrderByColumnType type;

    /**
     * Constructor.
     *
     * @param metric  a LogicalMetric
     * @param direction  sort direction
     *
     */
    public OrderByColumn(LogicalMetric metric, SortDirection direction) {
        this(metric.getName(), direction, OrderByColumnType.METRIC);
    }

    /**
     * Constructor.
     *
     * @param metricField  an Aggregation or PostAggregation
     * @param direction  sort direction
     */
    public OrderByColumn(MetricField metricField, SortDirection direction) {
        this(metricField.getName(), direction, OrderByColumnType.METRIC);
    }

    /**
     * Constructor which accepts generic column with direction. For example: dateTime column is not part of aggregation
     * or postAggregation. But still allowed to sort the resultSet based on dateTime value.
     *
     * @param column  a column needs to be associated with the direction
     * @param direction  sort direction
     */
    public OrderByColumn(String column, String direction) {
        this(column, parseSortDirection(direction), UNKNOWN);
    }

    /**
     * Constructor which accepts generic column with direction. For example: dateTime column is not part of aggregation
     * or postAggregation. But still allowed to sort the resultSet based on dateTime value.
     *
     * @param column  a column needs to be associated with the direction
     * @param direction  sort direction
     */
    public OrderByColumn(String column, SortDirection direction) {
        this(column, direction, UNKNOWN);
    }


    /**
     * Constructor which accepts generic column with direction. For example: dateTime column is not part of aggregation
     * or postAggregation. But still allowed to sort the resultSet based on dateTime value.
     *
     * @param column  a column needs to be associated with the direction
     * @param direction  sort direction
     * @param type The type of column being bound for sorting
     */
    public OrderByColumn(String column, SortDirection direction, OrderByColumnType type) {
        this.column = column;
        this.direction = direction;
        this.type = type;
    }

    /**
     * Getter for sort column dimension.
     *
     * @return dimension A dimension name, a metric name, an aggregation name or a post aggregation name
     */
    public String getDimension() {
        return this.column;
    }

    /**
     * Getter for sort direction.
     *
     * @return direction
     */
    public SortDirection getDirection() {
        return this.direction;
    }

    /**
     * Getter for the column type.
     *
     * @return the type of this column
     */
    @JsonIgnore
    public OrderByColumnType getType() {
        return type;
    }

    /**
     * Rewrite the OrderByColumn with a new type.
     *
     * @param type The new column type
     *
     * @return A modified copy with a different type
     */
    public OrderByColumn withType(OrderByColumnType type) {
        return new OrderByColumn(this.getDimension(), this.getDirection(), type);
    }

    /**
     * Bind sort direction request to SortDirection instance.
     *
     * @param sortDirection  The string representing the sort direction
     *
     * @return Sorting direction. If no direction provided then the default one will be DESC
     */
    protected static SortDirection parseSortDirection(String sortDirection) {
        if (sortDirection == null) {
            return DEFAULT_DIRECTION;
        }

        try {
            return SortDirection.valueByName(sortDirection);
        } catch (IllegalArgumentException ignored) {
            throw new BadApiRequestException(SORT_DIRECTION_INVALID.format(sortDirection));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (!(o instanceof OrderByColumn)) { return false; }

        OrderByColumn that = (OrderByColumn) o;

        return
                Objects.equals(column, that.column) &&
                direction == that.direction;
    }

    @Override
    public int hashCode() {
        return Objects.hash(column, direction);
    }
}
