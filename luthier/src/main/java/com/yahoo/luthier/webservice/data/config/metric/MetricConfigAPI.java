// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config.metric;

import com.yahoo.bard.webservice.data.config.names.ApiMetricName;

import java.util.List;

/**
 * Wiki metric config API.
 */
public interface MetricConfigAPI extends ApiMetricName {

    /**
     * Get metrics' long name.
     *
     * @return metrics' long name
     */
    String getLongName();

    /**
     * Get metrics' maker name.
     *
     * @return metrics' maker name
     */
    String getMakerName();

    /**
     * Get metrics' description.
     *
     * @return metrics' description
     */
    String getDescription();

    /**
     * Get metrics' dependency metric names.
     *
     * @return metrics' dependency metric names
     */
    List<String> getDependencyMetricNames();
}
