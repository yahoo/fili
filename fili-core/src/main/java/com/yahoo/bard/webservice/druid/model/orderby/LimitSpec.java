// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.orderby;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.LinkedHashSet;
import java.util.OptionalInt;

/**
 * LimitSpec class
 */
public class LimitSpec {

    private final String type;
    private final OptionalInt limit;
    private final LinkedHashSet<OrderByColumn> columns;

    /**
     * Constructor. Is used to sort the whole result set. No limit is applied on the number of results.
     *
     * @param sortColumns  The set of columns
     */
    public LimitSpec(LinkedHashSet<OrderByColumn> sortColumns) {
        this(sortColumns, OptionalInt.empty());
    }

    /**
     * Constructor. Specifies both a sorting method and a limit upon the number of results.
     *
     * @param sortColumns  The set of columns
     * @param limit  The number of result rows
     */
    public LimitSpec(LinkedHashSet<OrderByColumn> sortColumns, OptionalInt limit) {
        //As of Jul 2014, druid supports only "default" limitSpec, we may have other types in future
        this.type = "default";
        this.columns = sortColumns;
        this.limit = limit;
    }

    /**
     * Getter for type of orderBy
     *
     * @return type
     */
    public String getType() {
        return this.type;
    }

    /**
     * Getter for limit
     *
     * @return limit
     */
    @JsonInclude(JsonInclude.Include.NON_ABSENT)
    public OptionalInt getLimit() {
        return this.limit;
    }

    /**
     * Getter for columns
     *
     * @return orderBy columns
     */
    public LinkedHashSet<OrderByColumn> getColumns() {
        return this.columns;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) { return true; }
        if (!(o instanceof LimitSpec)) { return false; }

        LimitSpec limitSpec = (LimitSpec) o;

        if (type != null ? !type.equals(limitSpec.type) : limitSpec.type != null) { return false; }
        if (limit != null ? !limit.equals(limitSpec.limit) : limitSpec.limit != null) { return false; }
        return !(columns != null ? !columns.equals(limitSpec.columns) : limitSpec.columns != null);

    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (limit != null ? limit.hashCode() : 0);
        result = 31 * result + (columns != null ? columns.hashCode() : 0);
        return result;
    }
}
