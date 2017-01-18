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
     * Return the configurations describing physical tables in Druid.
     *
     * @return a list of physical table configurations
     */
    List<PhysicalTableConfiguration> getPhysicalTableConfig();

    /**
     * Return the configurations describing logical tables.
     *
     * @return a list of logical table configurations
     */
    List<LogicalTableConfiguration> getLogicalTableConfig();

    /**
     * Return the configurations describing custom-registered metric makers.
     *
     * @return a list of metric maker configurations
     */
    List<MakerConfiguration> getCustomMakerConfig();

    /**
     * Return the dimension configurations.
     *
     * @return a list of dimension configurations
     */
    List<DimensionConfig> getDimensionConfig();

    /**
     * Return the metric configurations.
     *
     * @return a list of metric configurations
     */
    List<MetricConfiguration> getMetricConfig();
}
