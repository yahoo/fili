// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config.metric;

import java.util.List;

/**
 * Metric Template.
 */
public interface MetricTemplate {

    /**
     * Get metrics' api name.
     *
     * @return metrics' api name
     */
    String getApiName();

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
