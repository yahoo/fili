// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.slurper.webservice.data.config.metric;

import com.yahoo.slurper.webservice.data.config.JsonObject;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * Metric object for parsing to json.
 */
public class MetricObject extends JsonObject {

    private final String apiName;
    private final String longName;
    private final String maker;
    private final List<String> dependencyMetricNames;

    /**
     * Construct a metric instance from metric name and
     * only using default maker.
     *
     * @param apiName The API Name is the external, end-user-facing name for the metric.
     * @param longName The Long Name is the external, end-user-facing long  name for the metric.
     * @param maker The maker this metric depends on.
     * @param dependencyMetricNames A list of metric names that this metric depends on.
     */
    public MetricObject(
            @NotNull String apiName,
            String longName,
            @NotNull String maker,
            List<String> dependencyMetricNames
    )  {
        this.apiName = apiName;
        this.longName = longName;
        this.maker = maker;
        this.dependencyMetricNames = dependencyMetricNames;
    }

    /**
     * The API Name is the external, end-user-facing name for the metric. This is the name of this metric in API
     * requests.
     *
     * @return User facing name for this metric
     */
    public String getApiName() {
        return apiName;
    }

    /**
     * The Long Name is the external, end-user-facing long  name for the metric.
     *
     * @return User facing long name for this metric
     */
    public String getLongName() {
        return longName;
    }

    /**
     * Get the maker this metric depends on.
     *
     * @return maker this metric depends on
     */
    public String getMaker() {
        return maker;
    }

    /**
     * Get the dependency metric names.
     *
     * @return dependency metric names
     */
    public List<String> getDependencyMetricNames() {
        return dependencyMetricNames;
    }
}
