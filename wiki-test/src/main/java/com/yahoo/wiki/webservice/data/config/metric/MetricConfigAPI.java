// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.metric;

import com.yahoo.bard.webservice.data.config.names.ApiMetricName;

import java.util.List;

/**
 * Wiki metric config API.
 */
public interface MetricConfigAPI extends ApiMetricName {

    /**
     * Set metrics' api name.
     *
     * @param apiName metric's apiName
     */
    void setApiName(String apiName);

    /**
     * Set metrics' long name.
     *
     * @param longName metric's longName
     */
    void setLongName(String longName);

    /**
     * Set metrics' maker Name.
     *
     * @param makerName metric's maker Name
     */
    void setMakerName(String makerName);

    /**
     * Set metrics' Description.
     *
     * @param description metric's description
     */
    void setDescription(String description);

    /**
     * Set metrics' dependency Metrics.
     *
     * @param dependencyMetricNames metric's dependency metric names
     */
    void setDependencyMetricNames(List<String> dependencyMetricNames);

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
