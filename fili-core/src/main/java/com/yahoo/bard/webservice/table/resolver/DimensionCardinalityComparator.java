// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

import com.yahoo.bard.webservice.table.PhysicalTable;

import java.util.Comparator;

/**
 * Comparator to prefer fewer dimensions in physical tables (for the purpose of preferring fewer rows).
 */
public class DimensionCardinalityComparator implements Comparator<PhysicalTable> {
    /**
     * Compare two Physical Tables based on their number of dimensions.
     *
     * @param table1 The first table
     * @param table2 The second table
     *
     * @return negative if table1 has fewer dimensions than table2
     */
    @Override
    public int compare(PhysicalTable table1, PhysicalTable table2) {
        return table1.getDimensions().size() - table2.getDimensions().size();
    }
}
