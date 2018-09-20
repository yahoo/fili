// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_DIMENSION_UNDEFINED;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_ERROR;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_FIELD_NOT_IN_DIMENSIONS;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_INVALID;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_OPERATOR_INVALID;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.util.FilterTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.validation.constraints.NotNull;

/**
 * ApiFilter object. Represents the different pieces of data in the filter clause of a Fili Api Query.
 */
public class ApiFilter {
//    private static final Logger LOG = LoggerFactory.getLogger(ApiFilter.class);

    private final Dimension dimension;
    private final DimensionField dimensionField;
    private final FilterOperation operation;
    private final Set<String> values;

    /**
     * Constructor.
     *
     * @param dimension  Dimension the filter operates on
     * @param dimensionField  Dimension Field the filter operates on
     * @param operation  Operation the filter operates with
     * @param values  The values the filter uses when operating
     */
    public ApiFilter(
            Dimension dimension,
            DimensionField dimensionField,
            FilterOperation operation,
            Set<String> values
    ) {
        this.dimension = dimension;
        this.dimensionField = dimensionField;
        this.operation = operation;
        this.values = Collections.unmodifiableSet(values);
    }

    /**
     * Creates a new ApiFilter based on this ApiFilter, except with the new dimension replacing this ApiFilter's
     * dimension.
     *
     * @param dimension The new dimension to use in the new ApiFilter.
     * @return the new ApiFilter object.
     */
    public ApiFilter withDimension(Dimension dimension) {
        return new ApiFilter(dimension, dimensionField, operation, values);
    }

    /**
     * Creates a new ApiFilter based on this ApiFilter, except with the new DimensionField replacing this ApiFilter's
     * DimensionField.
     *
     * @param dimensionField The new DimensionField to use in the new ApiFilter.
     * @return the new ApiFilter object.
     */
    public ApiFilter withDimensionField(DimensionField dimensionField) {
        return new ApiFilter(dimension, dimensionField, operation, values);
    }

    /**
     * Creates a new ApiFilter based on this ApiFilter, except with the new FilterOperation replacing this ApiFilter's
     * FilterOperation.
     *
     * @param operation The new operation to use in the new ApiFilter.
     * @return the new ApiFilter object.
     */
    public ApiFilter withOperation(FilterOperation operation) {
        return new ApiFilter(dimension, dimensionField, operation, values);
    }

    /**
     * Creates a new ApiFilter based on this ApiFilter, except with the new set of filter values replacing this
     * ApiFilter's filter values.
     *
     * @param values The new set of values to use in the new ApiFilter.
     * @return the new ApiFilter object.
     */
    public ApiFilter withValues(Set<String> values) {
        return new ApiFilter(dimension, dimensionField, operation, values);
    }

    /**
     * Getter for this ApiFilter's dimension.
     *
     * @return the Dimension
     */
    public Dimension getDimension() {
        return this.dimension;
    }

    /**
     * Getter for this ApiFilter's DimensionField.
     *
     * @return the DimensionField
     */
    public DimensionField getDimensionField() {
        return this.dimensionField;
    }

    /**
     * Getter for this ApiFilter's FilterOperation.
     *
     * @return the operation
     */
    public FilterOperation getOperation() {
        return this.operation;
    }

    /**
     * Getter for this ApiFilter's set of filter values.
     *
     * @return the set of values
     */
    public Set<String> getValues() {
        return this.values;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (!(o instanceof ApiFilter)) { return false; }

        ApiFilter apiFilter = (ApiFilter) o;

        return
                Objects.equals(dimension, apiFilter.dimension) &&
                        Objects.equals(dimensionField, apiFilter.dimensionField) &&
                        Objects.equals(operation, apiFilter.operation) &&
                        Objects.equals(values, apiFilter.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dimension, dimensionField, operation, values);
    }

    @Override
    public String toString() {
        return String.format("%s|%s-%s%s", dimension.getApiName(), dimensionField, operation, values);
    }
}
