// Copyright 2020 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.filter;

import java.util.ArrayList;
import java.util.List;

/**
 * Mutable class representing an ApiFilter that is in the process of being built.
 */
public class FilterDefinition {

    private final String dimensionName;
    private final String fieldName;
    private final String operationName;
    private final List<String> values;

    /**
     * Copy constructor.
     *
     * @param other Filter definition to copy state from
     */
    public FilterDefinition(FilterDefinition other) {
       this(other.getDimensionName(), other.getFieldName(), other.getOperationName(), other.getValues());
    }

    public FilterDefinition(
            String dimensionName,
            String fieldName,
            String operationName,
            List<String> values
    ) {
        this.dimensionName = dimensionName;
        this.fieldName = fieldName;
        this.operationName = operationName;
        this.values = new ArrayList<>(values);
    }

    public String getDimensionName() {
        return dimensionName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getOperationName() {
        return operationName;
    }

    public List<String> getValues() {
        return values;
    }

    @Override
    public String toString() {
        return "FilterDefinition[" +
                (dimensionName == null ? "uninitialized" : dimensionName) +
                "|" + (fieldName == null ? "uninitialized" : fieldName) +
                "-" + (operationName == null ? "uninitialized" : operationName) +
                "[" + String.join(",", values)
                + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (! (o instanceof FilterDefinition)) {
            return false;
        }
        if (this == o) {
            return true;
        }

        FilterDefinition that = (FilterDefinition) o;
        return java.util.Objects.equals(this.dimensionName, that.dimensionName) &&
                java.util.Objects.equals(this.values, that.values) &&
                java.util.Objects.equals(this.fieldName, that.fieldName) &&
                java.util.Objects.equals(this.operationName, that.operationName);
    }

    @Override
    public int hashCode() {
        int result = java.util.Objects.hashCode(dimensionName);
        result = 31 * result + java.util.Objects.hashCode(values);
        result = 31 * result + java.util.Objects.hashCode(fieldName);
        return 31 * result + java.util.Objects.hashCode(operationName);
    }
}
