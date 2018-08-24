// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config.metric;

import java.util.Map;

/**
 * Metric Maker Template.
 */
public interface MetricMakerTemplate {

    /**
     * Get maker's name.
     *
     * @return maker's name
     */
    String getName();

    /**
     * Get maker's class.
     *
     * @return maker's class
     */
    String getFullyQualifiedClassName();

    /**
     * Get maker's parameters.
     *
     * @return maker's parameters
     */
    Map<String, Object> getParams();
}
