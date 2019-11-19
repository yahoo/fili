// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config;

import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;

/**
 * This class acts as the topmost factory for all the configuration objects in the system.
 *
 * Once {@link #load()} is called, all the dictionaries in this object should be populated and ready to use.
 */
public interface ConfigurationLoader {

    /**
     * Load the Dimensions, Metrics, and Tables.
     */
    void load();

    /**
     * Getter.
     *
     * @return A dictionary of dimensions.
     */
    DimensionDictionary getDimensionDictionary();

    /**
     * Getter.
     *
     * @return A dictionary of metrics.
     */
    MetricDictionary getMetricDictionary();

    /**
     * Getter.
     *
     * @return A dictionary of logical tables.
     */
    LogicalTableDictionary getLogicalTableDictionary();

    /**
     * Getter.
     *
     * @return A dictionary of physical tables.
     */
    PhysicalTableDictionary getPhysicalTableDictionary();

    /**
     * Getter.
     *
     * @return A collection of system dictionaries.
     */
    ResourceDictionaries getDictionaries();
}
