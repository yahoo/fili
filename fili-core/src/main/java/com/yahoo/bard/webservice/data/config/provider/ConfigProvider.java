// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider;

import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;

import java.util.List;

/**
 * A ConfigProvider defines a programmatic source for configuring Fili.
 *
 * An instance of ConfigProvider provides access to configuration of:
 *  - Physical tables
 *  - Logical tables
 *  - Dimensions
 *  - Metrics
 *  - Custom metric makers
 */
public interface ConfigProvider {

    /**
     * Return the actual physical tables in Druid.
     *
     * @return a list of physical table configurations
     */
    List<PhysicalTableConfiguration> getPhysicalTableConfig();

    /**
     * Return the logical tables you'd like to expose.
     *
     * @return a list of logical table configurations
     */
    List<LogicalTableConfiguration> getLogicalTableConfig();

    /**
     * Return any custom metric makers used in metrics.
     *
     * @return a list of metric maker configurations
     */
    List<MakerConfiguration> getCustomMakerConfig();

    /**
     * Return the dimension config.
     *
     * @return a list of dimension configurations
     */
    List<DimensionConfig> getDimensionConfig();

    /**
     * Get the base metrics.
     *
     * @return a list of metric configurations
     */
    List<MetricConfiguration> getMetricConfig();
}
