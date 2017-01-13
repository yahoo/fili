// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.filterparser;

import java.util.Set;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.BadFilterException;
import com.yahoo.bard.webservice.web.FilterOperation;
import com.yahoo.bard.webservice.web.filterparser.FiltersParser.FilterContext;

/**
 * Listener to construct single ApiFilter objects.
 */
public class ApiFilterListener extends ApiFiltersListListener {
    private Dimension dimension = null;
    private DimensionField dimensionField = null;
    private FilterOperation operation = null;
    private Set<String> values = null;

    /**
     * Construct the listener.
     *
     * @param table LogicalTable to use for the filter
     * @param dimensionDictionary Dictionary of dimensions
     */
    public ApiFilterListener(LogicalTable table, DimensionDictionary dimensionDictionary) {
        super(table, dimensionDictionary);
    }

    /* (non-Javadoc)
     * @see com.yahoo.bard.webservice.web.filterparser.ApiFiltersListListener#exitFilter(
     * com.yahoo.bard.webservice.web.filterparser.FiltersParser.FilterContext)
     */
    @Override
    public void exitFilter(FilterContext ctx) {
        FiltersParser.FilterComponentContext f = ctx.filterComponent();
        dimension = extractDimension(f);
        if (dimension == null) {
            return;
        }

        dimensionField = extractField(f, dimension);
        if (dimensionField == null) {
            return;
        }

        operation = extractOperation(f);
        if (operation == null) {
            return;
        }

        values = extractValues(f);
    }

    /**
     * Simple getter for the Dimension that also checks for any pending errors and throws exceptions for them.
     * @return the Dimension
     * @throws BadFilterException invalid filter expression
     * @throws BadApiRequestException filter expression that doesn't match the current table or dimensions
     */
    public Dimension getDimension() throws BadFilterException, BadApiRequestException {
        processErrors();
        return dimension;
    }

    public DimensionField getDimensionField() {
        return dimensionField;
    }

    public FilterOperation getOperation() {
        return operation;
    }

    public Set<String> getValues() {
        return values;
    }
}
