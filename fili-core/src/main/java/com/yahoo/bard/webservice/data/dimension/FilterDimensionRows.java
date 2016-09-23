// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

import com.yahoo.bard.webservice.web.ApiFilter;

import java.util.TreeSet;

/**
 * Interface representing an object which can filter dimension row sets.
 */
public interface FilterDimensionRows {

    /**
     * In-filter operation.
     *
     * @param dimensionRows  The unfiltered set of dimension rows
     * @param filter  The api filter to be applied to the dimension rows
     *
     * @return Tree set of DimensionRows
     */
    TreeSet<DimensionRow> inFilterOperation(TreeSet<DimensionRow> dimensionRows, ApiFilter filter);

    /**
     * Notin-filter operation.
     *
     * @param dimensionRows  The unfiltered set of dimension rows
     * @param filter  The api filter to be applied to the dimension rows
     *
     * @return Tree set of DimensionRows
     */
    TreeSet<DimensionRow> notinFilterOperation(TreeSet<DimensionRow> dimensionRows, ApiFilter filter);

    /**
     * Startswith-filter operation.
     *
     * @param dimensionRows  The unfiltered set of dimension rows
     * @param filter  The api filter to be applied to the dimension rows
     *
     * @return Tree set of DimensionRows
     */
    TreeSet<DimensionRow> startswithFilterOperation(TreeSet<DimensionRow> dimensionRows, ApiFilter filter);

    /**
     * Contains filter operation.
     *
     * @param dimensionRows  The unfiltered set of dimension rows
     * @param filter  The api filter to be applied to the dimension rows
     *
     * @return Tree set of DimensionRows
     */
    TreeSet<DimensionRow> containsFilterOperation(TreeSet<DimensionRow> dimensionRows, ApiFilter filter);
}
