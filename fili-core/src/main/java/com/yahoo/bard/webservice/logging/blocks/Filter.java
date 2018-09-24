// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging.blocks;

import com.yahoo.bard.webservice.logging.LogInfo;
import com.yahoo.bard.webservice.web.ApiFilter;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

/**
 * Inner log block recording information related to the API filters of a query.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class Filter implements LogInfo, Comparable<Filter> {
    protected final String dimension;
    protected final String field;
    protected final String operator;
    protected final int numberOfValues;

    /**
     * Constructor.
     *
     * @param filter  The filter being recorded
     */
    public Filter(ApiFilter filter) {
        this.dimension = filter.getDimension().getApiName();
        this.field = filter.getDimensionField().getName();
        this.operator = filter.getOperation().getName();
        this.numberOfValues = filter.getValues().size();
    }

    @SuppressWarnings("checkstyle:separatorwrap")
    @Override
    public int compareTo(Filter other) {
        // Implements a chaining of comparisons. Each comparison is saved and if inequality is detected at this
        // point the result of the comparison is returned.
        // Checkstyle complains for wrapping the assignment operation
        int returnValue;
        return (returnValue = this.dimension.compareTo(other.dimension)) != 0
                ?  returnValue : (returnValue = this.field.compareTo(other.field)) != 0
                ?  returnValue : (returnValue = this.operator.compareTo(other.operator)) != 0
                ? returnValue : Integer.compare(this.numberOfValues, other.numberOfValues);
    }
}
