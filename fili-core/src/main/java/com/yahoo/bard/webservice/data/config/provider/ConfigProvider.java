// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider;

import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;

/**
 * A ConfigProvider defines a programmatic source for configuring Fili.
 *
 * An instance of ConfigProvider provides access to configuration of:
 *  - Physical tables
 *  - Logical tables
 *  - Dimensions
 *  - Metrics, both base and derived
 *  - Custom metric makers
 */
public interface ConfigProvider {

    /**
     * Return the actual physical tables in Druid.
     *
     * @return a mapping of physical table names to their configurations
     */
    ConfigurationDictionary<PhysicalTableConfiguration> getPhysicalTableConfig();

    /**
     * Return the logical tables you'd like to expose.
     *
     * @return a mapping of logical table names to their configurations
     */
    ConfigurationDictionary<LogicalTableConfiguration> getLogicalTableConfig();

    /**
     * Return any custom metric makers used in metrics.
     *
     * @return a mapping of custom metric maker names to their definitions
     */
    ConfigurationDictionary<MakerConfiguration> getCustomMakerConfig();

    /**
     * Return the dimension config.
     *
     * @return a mapping of dimension names to their definition
     */
    ConfigurationDictionary<DimensionConfig> getDimensionConfig();

    /**
     * Get the base metrics.
     *
     * @return a mapping of base metric names to their definitions
     */
    ConfigurationDictionary<MetricConfiguration> getBaseMetrics();

    /**
     * Get the derived / computed metrics.
     *
     * @return a mapping of derived metric names to their definitions
     */
    ConfigurationDictionary<MetricConfiguration> getDerivedMetrics();
}
