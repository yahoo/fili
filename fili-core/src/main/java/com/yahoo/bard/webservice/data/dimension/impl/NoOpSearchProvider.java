// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl;

import com.yahoo.bard.webservice.data.cache.HashDataCache;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.util.AllPagesPagination;
import com.yahoo.bard.webservice.util.Pagination;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * NoSearchProvider class
 * <p>
 * This class is for supporting search dimensions which don't have any defined set of dimension rows
 * The find* method's on this class would not throw an exception if the filterValue is not found, instead
 * it would just return a new dimension row matching the filter.
 */
public class NoOpSearchProvider implements SearchProvider {

    private final int queryWeightLimit;

    private Dimension dimension;

    private static TreeSet<DimensionRow> dimensionRows = new TreeSet<>();

    /**
     * Constructor.
     *
     * @param queryWeightLimit  Weight limit for the query, used as a cardinality approximation for this dimension
     */
    public NoOpSearchProvider(int queryWeightLimit) {
        this.queryWeightLimit = queryWeightLimit;
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
     * Get cardinality for the dimension
     * <p>
     * For dimensions with NoOpSearchProvider,
     * cardinality is unknown because it doesn't have defined set of dimension rows.
     * For such dimensions we should always calculate the query weight and if query weight is larger than what is
     * configured as package_name__query_weight_limit, the query weight is computed always otherwise it is skipped.
     * <p>
     * Hence, returning the cardinality value as package_name__query_weight_limit
     *
     * @return The dimension cardinality
     */
    @Override
    public int getDimensionCardinality() {
        return queryWeightLimit;
    }

    /**
     * For dimensions with NoOpSearchProvider, the dimension rows are unknown.
     * So, returning an empty Set would prevent any NullPointerException.
     *
     * @return empty Set of dimension rows.
     */
    @Override
    public TreeSet<DimensionRow> findAllOrderedDimensionRows() {
        return dimensionRows;
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
        LinkedHashMap<DimensionField, String> map = new LinkedHashMap<>();
        for (DimensionField dimensionField: dimension.getDimensionFields()) {
            map.put(dimensionField, value);
        }

        return new DimensionRow(dimension.getKey(), map);
    }

    @Override
    public Pagination<DimensionRow> findAllDimensionRowsPaged(PaginationParameters paginationParameters) {
        return new AllPagesPagination<>(dimensionRows, paginationParameters);
    }

    @Override
    public Pagination<DimensionRow> findFilteredDimensionRowsPaged(
            Set<ApiFilter> filters,
            PaginationParameters paginationParameters
    ) {
        return new AllPagesPagination<>(
                filters.stream()
                        .flatMap(f -> f.getValues().stream())
                        .map(this::makeDimensionRow)
                        .collect(Collectors.toCollection(TreeSet::new)),
                paginationParameters
        );
    }
}
