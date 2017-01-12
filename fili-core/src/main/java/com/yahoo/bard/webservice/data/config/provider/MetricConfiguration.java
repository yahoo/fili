// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider;

import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;

/**
 * A metric with a definition, description, and flag to exclude from external API
 *
 * FIXME: The isExcluded() flag is currently unused.
 */
public interface MetricConfiguration {

    /**
     * Get the metric's name.
     *
     * @return the name
     */
    String getName();

    /**
     * Get the metric's definition.
     *
     * @return the definition
     */
    String getDefinition();

    /**
     * Get the metric's description.
     *
     * @return the description
     */
    String getDescription();

    /**
     * Return true if the metric shouldn't be exposed in the api.
     *
     * @return true if the metric should be excluded
     */
    Boolean isExcluded();

    /**
     * Build the LogicalMetric for this metric.
     *
     * Yes, the signature is a little nasty and some of these things should be changed around a bit.
     *
     * @param dict the local metric dictionary
     * @param makerBuilder the metric maker builder
     * @param dimensionDictionary the dimension dictionary
     * @return a logical metric
     */
    LogicalMetric build(
            MetricDictionary dict,
            MakerBuilder makerBuilder,
            DimensionDictionary dimensionDictionary
    );
}
