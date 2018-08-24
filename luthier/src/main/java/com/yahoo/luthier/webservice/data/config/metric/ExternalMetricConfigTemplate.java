// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config.metric;

import java.util.Set;

/**
 * External Metric Config Template.
 */
public interface ExternalMetricConfigTemplate {

    /**
     * Get metrics makers configuration info.
     *
     * @return a list of metrics makers
     */
    Set<MetricMakerTemplate> getMakers();

    /**
     * Get metrics configuration info.
     *
     * @return a list of metrics
     */
    Set<MetricTemplate> getMetrics();
}
