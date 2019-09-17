// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.orderby;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Optional;

/**
 * LimitSpec class.
 */
public class LimitSpec {

    private final String type;
    private final Optional<Integer> limit;
    private final LinkedHashSet<OrderByColumn> columns;

    /**
     * Constructor. Is used to sort the whole result set. No limit is applied on the number of results.
     *
     * @param sortColumns  The set of columns
     */
    public LimitSpec(LinkedHashSet<OrderByColumn> sortColumns) {
        this(sortColumns, Optional.empty());
    }

    /**
     * Constructor. Specifies both a sorting method and a limit upon the number of results.
     *
     * @param sortColumns  The set of columns
     * @param limit  The number of result rows
     */
    public LimitSpec(LinkedHashSet<OrderByColumn> sortColumns, Optional<Integer> limit) {
        //As of Jul 2014, druid supports only "default" limitSpec, we may have other types in future
        this.type = "default";
        this.columns = sortColumns;
        this.limit = limit;
    }

    /**
     * Getter for type of orderBy.
     *
     * @return type
     */
    public String getType() {
        return type;
    }

    /**
     * Getter for limit.
     *
     * @return limit
     */
    @JsonInclude(JsonInclude.Include.NON_ABSENT)
    public Optional<Integer> getLimit() {
        return limit;
    }

    /**
     * Getter for columns.
     *
     * @return orderBy columns
     */
    public LinkedHashSet<OrderByColumn> getColumns() {
        return this.columns;
    }

    // CHECKSTYLE:OFF
    public LimitSpec withColumns(LinkedHashSet<OrderByColumn> sortColumns) {
        return new LimitSpec(sortColumns, limit);
    }

    public LimitSpec withLimit(Optional<Integer> limit) {
        return new LimitSpec(columns, limit);
    }
    // CHECKSTYLE:ON

    @Override
    public boolean equals(final Object o) {
        if (this == o) { return true; }
        if (!(o instanceof LimitSpec)) { return false; }

        LimitSpec limitSpec = (LimitSpec) o;

        return
                Objects.equals(type, limitSpec.type) &&
                Objects.equals(limit, limitSpec.limit) &&
                Objects.equals(columns, limitSpec.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, limit, columns);
    }
}
