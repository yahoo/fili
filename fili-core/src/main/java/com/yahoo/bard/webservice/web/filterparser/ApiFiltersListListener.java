// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.filterparser;

import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.yahoo.bard.webservice.config.BardFeatureFlag;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.BadFilterException;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;
import com.yahoo.bard.webservice.web.FilterOperation;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_DIMENSION_NOT_IN_TABLE;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_DIMENSION_UNDEFINED;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_FIELD_NOT_IN_DIMENSIONS;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_OPERATOR_INVALID;

/**
 * Listener used to walk the Filters parse tree and build ApiFilter objects.
 */
public class ApiFiltersListListener extends FiltersBaseListener {
    private static final Logger LOG = LoggerFactory.getLogger(ApiFiltersListListener.class);

    private final Map<FiltersParser.FilterComponentContext, ApiFilter> filters = new HashMap<> ();
    private final Map<Dimension, Set<ApiFilter>> dimensionFiltersMap = new LinkedHashMap<>();
    private final List<Exception> errors = new LinkedList<>();
    private final LogicalTable table;
    private final DimensionDictionary dimensionDictionary;

    /**
     * Constructor.
     *
     * @param table logical table that the filter applies to
     * @param dimensionDictionary dictionary of all the valid dimensions
     */
    public ApiFiltersListListener(LogicalTable table, DimensionDictionary dimensionDictionary) {
        this.table = table;
        this.dimensionDictionary = dimensionDictionary;
    }

    @Override
    public void exitFilters(@NotNull FiltersParser.FiltersContext ctx) {
        for (FiltersParser.FilterComponentContext f : ctx.filterComponent()) {
            ApiFilter filter = filters.get(f);
            if (filter == null) {
                // there was some error and it has been saved in the errors list
                break;
            }
            Dimension dimension = filter.getDimension();
            if (!dimensionFiltersMap.containsKey(dimension)) {
                dimensionFiltersMap.put(dimension, new LinkedHashSet<>());
            }
            Set<ApiFilter> filterSet = dimensionFiltersMap.get(dimension);
            filterSet.add(filter);
            dimensionFiltersMap.put(dimension, filterSet);
        }
    }

    @Override
    public void exitFilterComponent(@NotNull FiltersParser.FilterComponentContext f) {
        Dimension dimension = extractDimension(f);
        if (dimension == null) {
            return;
        }

        DimensionField dimensionField = extractField(f, dimension);
        if (dimensionField == null) {
            return;
        }

        FilterOperation operation = extractOperation(f);
        if (operation == null) {
            return;
        }

        Set<String> values = extractValues(f);

        ApiFilter filter = new ApiFilter(dimension, dimensionField, operation, values);

        filters.put(f, filter);
    }

    /**
     * Extract values from the filter.
     * @param f The filter context
     * @return the list of values
     */
    protected Set<String> extractValues(FiltersParser.FilterComponentContext f) {
        Set<String> values = new LinkedHashSet<>();
        for (TerminalNode tok : f.values().VALUE()) {
            String val = tok.getText();
            values.add(val);
        }
        return values;
    }

    /**
     * Extract the operation from the filter.
     * @param f THe filter context
     * @return the operation
     */
    protected FilterOperation extractOperation(FiltersParser.FilterComponentContext f) {
        String operationName = f.OPERATOR().getText();
        FilterOperation operation;
        try {
            operation = FilterOperation.valueOf(operationName);
            if ((operation == FilterOperation.startswith || operation == FilterOperation.contains) &&
                            !BardFeatureFlag.DATA_FILTER_SUBSTRING_OPERATIONS.isOn()) {
                errors.add(new BadApiRequestException(
                        ErrorMessageFormat.FILTER_SUBSTRING_OPERATIONS_DISABLED.format())
                );
            }

        } catch (IllegalArgumentException ignored) {
            LOG.debug(FILTER_OPERATOR_INVALID.logFormat(operationName));
            errors.add(new BadFilterException(FILTER_OPERATOR_INVALID.format(operationName)));
            return null;
        }
        return operation;
    }

    /**
     * Extract the field portion of a filter.
     * @param f The filter context
     * @param dimension The dimension for the field
     * @return the DimensionField
     */
    protected DimensionField extractField(FiltersParser.FilterComponentContext f, Dimension dimension) {
        String dimensionFieldName;
        dimensionFieldName = f.field().getText();
        DimensionField dimensionField;
        try {
            dimensionField = dimension.getFieldByName(dimensionFieldName);
        } catch (IllegalArgumentException ignored) {
            LOG.debug(FILTER_FIELD_NOT_IN_DIMENSIONS.logFormat(dimensionFieldName, dimension.getApiName()));
            errors.add(new BadFilterException(
                    FILTER_FIELD_NOT_IN_DIMENSIONS.format(dimensionFieldName, dimension.getApiName()))
            );
            return null;
        }
        return dimensionField;
    }

    /**
     * Extract the dimension portion of a filter.
     * @param f The filter context
     * @return the Dimension
     */
    protected Dimension extractDimension(FiltersParser.FilterComponentContext f) {
        String dimensionName = f.dimension().getText();
        Dimension dimension = dimensionDictionary.findByApiName(dimensionName);

        // If no filter dimension is found in dimension dictionary throw exception.
        if (dimension == null) {
            LOG.debug(FILTER_DIMENSION_UNDEFINED.logFormat(dimensionName));
            errors.add(new BadFilterException(FILTER_DIMENSION_UNDEFINED.format(dimensionName)));
            return null;
        }

        // If there is a logical table and the filter is not part of it, throw exception.
        if (table != null && !table.getDimensions().contains(dimension)) {
            LOG.debug(FILTER_DIMENSION_NOT_IN_TABLE.logFormat(dimensionName, table));
            errors.add(new BadFilterException(
                    FILTER_DIMENSION_NOT_IN_TABLE.format(dimensionName, table.getName()))
            );
            return null;
        }
        return dimension;
    }

    /**
     * Gets the list of parsed dimensionFiltersMap. If a parsing error occured or an invalid filter was
     * specified, throws BadFilterException
     *
     * @return a Map of ApiFilter keyed by Dimension
     * @throws BadFilterException Thrown when an invalid filter is specified
     * @throws BadApiRequestException Thrown when a filter isn't supported
     */
    public Map<Dimension, Set<ApiFilter>> getDimensionFiltersMap () throws BadFilterException, BadApiRequestException {
        processErrors();
        return dimensionFiltersMap;
    }

    /**
     * Throw any pending exceptions.
     *
     * @throws BadFilterException Thrown when the filter is invalid
     * @throws BadApiRequestException Thrown for valid filtes that are invalid for the current table or dimensions
     */
    protected void processErrors() throws BadFilterException, BadApiRequestException {
        for (Exception ex : errors) {
            if (ex instanceof BadFilterException) {
                throw (BadFilterException) ex;
            } else if (ex instanceof BadApiRequestException) {
                throw (BadApiRequestException) ex;
            }
        }
    }

    protected List<Exception> getErrors () {
        return errors;
    }
}
