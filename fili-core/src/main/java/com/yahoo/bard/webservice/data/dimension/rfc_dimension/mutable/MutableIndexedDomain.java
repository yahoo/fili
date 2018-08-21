// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.rfc_dimension.mutable;

import com.yahoo.bard.webservice.data.cache.HashDataCache.Pair;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.dimension.rfc_dimension.IndexedDomain;

import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Provides the capability to search through dimension metadata. For example, finding all dimensions whose name
 * contains "page_views."
 */
public interface MutableIndexedDomain extends IndexedDomain, MutableDomain {

    /**
     * Method to add / update indexes.
     *
     * @param rowId  Unique ID for the dimension row
     * @param dimensionRow  New, updated dimension row
     * @param dimensionRowOld  Old dimension row if we're updating one, or null if there is no row for the rowId
     */
    void refreshIndex(String rowId, DimensionRow dimensionRow, DimensionRow dimensionRowOld);

    /**
     * Method to add / update indexes.
     *
     * @param changedRows Collection of newRow / oldRow pairs keyed by rowId to update the index with. Each row is a
     * pair of dimension rows (newRow is the key, oldRow is the value), with the rowId (ie. unique key in the dimension)
     * as the top-level Map key.
     */
    void refreshIndex(Map<String, Pair<DimensionRow, DimensionRow>> changedRows);


    /**
     * Replaces index with a new index in hot-swap manner.
     *
     * @param newIndexPathString  The location of directory that contains the the new index.
     */
    default void replaceIndex(String newIndexPathString) {
        String message = String.format(
                "Current implementation of SearchProvider: %s does not support index replacement operation.",
                this.getClass().getSimpleName()
        );
        LoggerFactory.getLogger(MutableIndexedDomain.class).error(message);
        throw new UnsupportedOperationException(message);
    }
}
