// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_DIMENSION_NOT_IN_TABLE;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_DIMENSION_UNDEFINED;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_ERROR;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_FIELD_NOT_IN_DIMENSIONS;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_INVALID;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_OPERATOR_INVALID;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.util.FilterTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.validation.constraints.NotNull;

/**
 * ApiFilter object.
 */
public class ApiFilter {
    private static final Logger LOG = LoggerFactory.getLogger(ApiFilter.class);

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

    @SuppressWarnings("checkstyle:javadocmethod")
    public ApiFilter withDimension(@NotNull Dimension dimension) {
        return new ApiFilter(dimension, dimensionField, operation, values);
    }

    @SuppressWarnings("checkstyle:javadocmethod")
    public ApiFilter withDimensionField(@NotNull DimensionField dimensionField) {
        return new ApiFilter(dimension, dimensionField, operation, values);
    }

    @SuppressWarnings("checkstyle:javadocmethod")
    public ApiFilter withOperation(@NotNull FilterOperation operation) {
        return new ApiFilter(dimension, dimensionField, operation, values);
    }

    @SuppressWarnings("checkstyle:javadocmethod")
    public ApiFilter withValues(@NotNull Set<String> values) {
        return new ApiFilter(dimension, dimensionField, operation, values);
    }

    /**
     * Parses the URL filter Query and generates the ApiFilter object.
     *
     * @param filterQuery Expects a URL filter query String in the format:
     * <p>
     * <code>(dimension name)|(field name)-(operation)[?(value or comma separated values)]?</code>
     * @param dimensionDictionary cache containing all the valid dimension objects.
     *
     * @throws BadFilterException Exception when filter pattern is not matched or when any of its properties are not
     * valid.
     */
    public ApiFilter(@NotNull String filterQuery, DimensionDictionary dimensionDictionary) throws BadFilterException {
        this(filterQuery, (LogicalTable) null, dimensionDictionary);
    }

    /**
     * Parses the URL filter Query and generates the ApiFilter object.
     *
     * @param filterQuery  Expects a URL filter query String in the format:
     * <p>
     * <code>(dimension name)|(field name)-(operation)[?(value or comma separated values)]?</code>
     *
     * @param table  The logical table for a data request (if any)
     * @param dimensionDictionary  cache containing all the valid dimension objects.
     *
     * @throws BadFilterException Exception when filter pattern is not matched or when any of its properties are not
     * valid.
     */
    public ApiFilter(
            @NotNull String filterQuery,
            LogicalTable table,
            DimensionDictionary dimensionDictionary
    ) throws BadFilterException {
        LOG.trace("Filter query: {}\n\n DimensionDictionary: {}", filterQuery, dimensionDictionary);

        /*  url filter query pattern:  (dimension name)|(field name)-(operation)[?(value or comma separated values)]?
         *
         *  e.g.    locale|name-in[US,India]
         *          locale|id-eq[5]
         *
         *          dimension name: locale      locale
         *          field name:     name        id
         *          operation:      in          eq
         *          values:         US,India    5
         */
        Pattern pattern = Pattern.compile("([^\\|]+)\\|([^-]+)-([^\\[]+)\\[([^\\]]+)\\]?");
        Matcher matcher = pattern.matcher(filterQuery);

        // if pattern match found, extract values else throw exception
        if (!matcher.matches()) {
            LOG.debug(FILTER_INVALID.logFormat(filterQuery));
            throw new BadFilterException(FILTER_INVALID.format(filterQuery));
        }

        try {
            // Extract filter dimension form the filter query.
            String filterDimensionName = matcher.group(1);
            this.dimension = dimensionDictionary.findByApiName(filterDimensionName);

            // If no filter dimension is found in dimension dictionary throw exception.
            if (dimension == null) {
                LOG.debug(FILTER_DIMENSION_UNDEFINED.logFormat(filterDimensionName));
                throw new BadFilterException(FILTER_DIMENSION_UNDEFINED.format(filterDimensionName));
            }

            // If there is a logical table and the filter is not part of it, throw exception.
            if (table != null && !table.getDimensions().contains(dimension)) {
                LOG.debug(FILTER_DIMENSION_NOT_IN_TABLE.logFormat(filterDimensionName, table));
                throw new BadFilterException(
                        FILTER_DIMENSION_NOT_IN_TABLE.format(filterDimensionName, table.getName())
                );
            }

            String dimensionFieldName = matcher.group(2);
            try {
                this.dimensionField = this.dimension.getFieldByName(dimensionFieldName);
            } catch (IllegalArgumentException ignored) {
                LOG.debug(FILTER_FIELD_NOT_IN_DIMENSIONS.logFormat(dimensionFieldName, filterDimensionName));
                throw new BadFilterException(
                        FILTER_FIELD_NOT_IN_DIMENSIONS.format(dimensionFieldName, filterDimensionName)
                );
            }
            String operationName = matcher.group(3);
            try {
                this.operation = FilterOperation.valueOf(operationName);
            } catch (IllegalArgumentException ignored) {
                LOG.debug(FILTER_OPERATOR_INVALID.logFormat(operationName));
                throw new BadFilterException(FILTER_OPERATOR_INVALID.format(operationName));
            }

            // replaceAll takes care of any leading ['s or trailing ]'s which might mess up this.values
            this.values = new LinkedHashSet<>(
                    FilterTokenizer.split(
                            matcher.group(4)
                                    .replaceAll("\\[", "")
                                    .replaceAll("\\]", "")
                                    .trim()
                    )
            );
        } catch (IllegalArgumentException e) {
            LOG.debug(FILTER_ERROR.logFormat(filterQuery, e.getMessage()), e);
            throw new BadFilterException(FILTER_ERROR.format(filterQuery, e.getMessage()), e);
        }
    }

    public Dimension getDimension() {
        return this.dimension;
    }

    public DimensionField getDimensionField() {
        return this.dimensionField;
    }

    public FilterOperation getOperation() {
        return this.operation;
    }

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
