// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider;

import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.config.table.PhysicalTableDefinition;

import java.util.Set;

/**
 * A PhysicalTableConfiguration can create its PhysicalTableDefinition, given a dimension configuration.
 */
public interface PhysicalTableConfiguration {

    /**
     * Build a physical table, given dimensions.
     *
     * @param dimensionMap the dimension configuration
     * @return a PhysicalTableDefinition
     */
    PhysicalTableDefinition buildPhysicalTable(ConfigurationDictionary<DimensionConfig> dimensionMap);

    /**
     * Return the metrics of the physical table.
     *
     * @return the set of FieldNames
     */
    Set<FieldName> getMetrics();

}
