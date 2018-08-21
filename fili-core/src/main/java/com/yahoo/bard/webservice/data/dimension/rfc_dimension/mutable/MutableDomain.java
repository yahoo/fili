// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.rfc_dimension.mutable;

import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.dimension.rfc_dimension.Domain;

import org.joda.time.DateTime;

import java.util.Set;

public interface MutableDomain extends Domain {

    /**
     * Add a dimension row to the dimension's set of rows.
     *
     * @param dimensionRow  DimensionRow to add
     */
    void addDimensionRow(DimensionRow dimensionRow);

    /**
     * Add all dimension rows to the dimension's set of rows.
     *
     * @param dimensionRows  Set of DimensionRows to add
     */
    void addAllDimensionRows(Set<DimensionRow> dimensionRows);

    /**
     * Setter for lastUpdated.
     *
     * @param lastUpdated  The date and time at which this Dimension was last updated
     */
    void setLastUpdated(DateTime lastUpdated);

}
