// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl;

import com.yahoo.bard.webservice.data.cache.HashDataCache;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.dimension.ImmutableSearchProvider;
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
public class MapSearchProvider extends ScanSearchProvider implements ImmutableSearchProvider {

    protected static final String UNSUPPORTED_OP_EXCEPTION_MESSAGE = "MapSearchProvider does not support %s operation";

    private Dimension dimension;

    private final Map<String, DimensionRow> dimensionRows;

    /**
     * Constructor.
     *
     * @param dimensionRows Map of dimension rows 'indexed' only by key field
     */
    public MapSearchProvider(Map<String, DimensionRow> dimensionRows) {
        // TODO is there a defined ordering of a tree set
        this.dimensionRows = new TreeMap<>(dimensionRows);
    }

    @Override
    public void setDimension(Dimension dimension) {
        this.dimension = dimension;
    }

    @Override
    public void setKeyValueStore(KeyValueStore keyValueStore) {
        // Do nothing. This search provider is not backed by a key values store, so quietly ignore it
    }

    @Override
    public int getDimensionCardinality() {
        return dimensionRows.size();
    }

    @Override
    public int getDimensionCardinality(boolean refresh) {
        // no way to refresh cardinality.
        return getDimensionCardinality();
    }

    @Override
    public TreeSet<DimensionRow> findAllOrderedDimensionRows() {
        return new TreeSet<>(dimensionRows.values());
    }

    @Override
    public void refreshIndex(String rowId, DimensionRow dimensionRow, DimensionRow dimensionRowOld) {
        throw new UnsupportedOperationException(
                String.format(
                        UNSUPPORTED_OP_EXCEPTION_MESSAGE,
                        "refresh index"
                )
        );
    }

    @Override
    public void refreshIndex(Map<String, HashDataCache.Pair<DimensionRow, DimensionRow>> changedRows) {
        throw new UnsupportedOperationException(
                String.format(
                        UNSUPPORTED_OP_EXCEPTION_MESSAGE,
                        "refresh index"
                )
        );

    }

    @Override
    public void clearDimension() {
        throw new UnsupportedOperationException(
            String.format(
                    UNSUPPORTED_OP_EXCEPTION_MESSAGE,
                    "clear dimension"
            )
    );

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
}
