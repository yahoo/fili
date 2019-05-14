// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl;

import com.yahoo.bard.webservice.data.cache.HashDataCache;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.util.AllPagesPagination;
import com.yahoo.bard.webservice.util.Pagination;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * MapSearchProvider allows for a read only view on a simple immutable map of in memory dimension rows.
 */
public class MapSearchProvider extends ScanSearchProvider {

    private Dimension dimension;

    private static Map<String, DimensionRow> dimensionRows = new TreeMap<>();

    /**
     * Constructor.
     *
     * @param dimensionRows Map of dimension rows 'indexed' only by key field
     */
    public MapSearchProvider(Map<String, DimensionRow> dimensionRows) {
        this.dimensionRows = new TreeMap<>(dimensionRows);
    }

    @Override
    public void setDimension(Dimension dimension) {
        this.dimension = dimension;
    }

    @Override
    public void setKeyValueStore(KeyValueStore keyValueStore) {
        // do nothing
    }

    /**
     * Get cardinality for the dimension.
     * <p>
     * @return The dimension cardinality
     */
    @Override
    public int getDimensionCardinality() {
        return dimensionRows.size();
    }

    /**
     * For dimensions with NoOpSearchProvider, the dimension rows are unknown.
     * So, returning an empty Set would prevent any NullPointerException.
     *
     * @return empty Set of dimension rows.
     */
    @Override
    public TreeSet<DimensionRow> findAllOrderedDimensionRows() {
        return new TreeSet<>(dimensionRows.values());
    }

    @Override
    public void refreshIndex(String rowId, DimensionRow dimensionRow, DimensionRow dimensionRowOld) {
        // do nothing
    }

    @Override
    public void refreshIndex(Map<String, HashDataCache.Pair<DimensionRow, DimensionRow>> changedRows) {
        // do nothing
    }

    @Override
    public void clearDimension() {
        //do nothing
    }

    @Override
    public boolean isHealthy() {
        return true;
    }

    /**
     * Make a DimensionRow by setting all of the field values to the given value.
     *
     * @param value Value for dimension fields
     * @return a DimensionRow
     */
    private DimensionRow makeDimensionRow(String value) {
        return dimension.createEmptyDimensionRow(value);
    }

    @Override
    public Pagination<DimensionRow> findAllDimensionRowsPaged(PaginationParameters paginationParameters) {
        return new AllPagesPagination<>(dimensionRows.values(), paginationParameters);
    }

    @Override
    public Pagination<DimensionRow> findFilteredDimensionRowsPaged(
            Set<ApiFilter> filters,
            PaginationParameters paginationParameters
    ) {
        return super.findFilteredDimensionRowsPaged(filters, paginationParameters);
    }
}
