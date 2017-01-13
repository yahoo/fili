// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.web.filterparser.ApiFilterListener;
import com.yahoo.bard.webservice.web.filterparser.FiltersLex;
import com.yahoo.bard.webservice.web.filterparser.FiltersParser;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_INVALID_WITH_DETAIL;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.util.ExceptionErrorListener;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

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

        ApiFilterListener apiFilterExtractor = new ApiFilterListener(table, dimensionDictionary);

        ANTLRInputStream input = new ANTLRInputStream(filterQuery);
        FiltersLex lexer = new FiltersLex(input);
        lexer.removeErrorListeners();
        lexer.addErrorListener(ExceptionErrorListener.INSTANCE);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        FiltersParser parser = new FiltersParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(ExceptionErrorListener.INSTANCE);

        try {
            FiltersParser.FilterContext tree = parser.filter();
            ParseTreeWalker.DEFAULT.walk(apiFilterExtractor, tree);
        } catch (ParseCancellationException ex) {
            LOG.debug(FILTER_INVALID_WITH_DETAIL.logFormat(filterQuery, ex.getMessage()));
            throw new BadFilterException(
                            FILTER_INVALID_WITH_DETAIL.format(filterQuery, ex.getMessage()),
                            ex.getCause());
        }

        this.dimension = apiFilterExtractor.getDimension();
        this.dimensionField = apiFilterExtractor.getDimensionField();
        this.operation = apiFilterExtractor.getOperation();
        this.values = apiFilterExtractor.getValues();
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
