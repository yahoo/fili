// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider;

import com.yahoo.bard.webservice.data.config.provider.descriptor.DimensionDescriptor;
import com.yahoo.bard.webservice.data.config.provider.descriptor.DimensionFieldDescriptor;
import com.yahoo.bard.webservice.data.config.provider.descriptor.LogicalTableDescriptor;
import com.yahoo.bard.webservice.data.config.provider.descriptor.MakerDescriptor;
import com.yahoo.bard.webservice.data.config.provider.descriptor.MetricDescriptor;
import com.yahoo.bard.webservice.data.config.provider.descriptor.PhysicalTableDescriptor;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;

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
    List<PhysicalTableDescriptor> getPhysicalTableConfig();

    /**
     * Return the configurations describing logical tables.
     *
     * @return a list of logical table configurations
     */
    List<LogicalTableDescriptor> getLogicalTableConfig();

    /**
     * Return the configurations describing custom-registered metric makers.
     *
     * @return a list of metric maker configurations
     */
    List<MakerDescriptor> getCustomMakerConfig();

    /**
     * Return the dimension configurations.
     *
     * @return a list of dimension configurations
     */
    List<DimensionDescriptor> getDimensionConfig();

    /**
     * Return the dimension field configurations.
     *
     * @return a list of dimension field configurations
     */
    List<DimensionFieldDescriptor> getDimensionFieldConfig();

    /**
     * Return the metric configurations.
     *
     * @return a list of metric configurations
     */
    List<MetricDescriptor> getMetricConfig();

    /**
     * Return an object that can turn metric configurations into actual metrics.
     *
     * @param metricDictionary  The metric dictionary
     * @param makerBuilder  The metric maker builder
     * @param dimensionDictionary  The dimension dictionary
     *
     * @return The metric builder
     */
    LogicalMetricBuilder getLogicalMetricBuilder(
            MetricDictionary metricDictionary,
            MakerBuilder makerBuilder,
            DimensionDictionary dimensionDictionary
    );
}
