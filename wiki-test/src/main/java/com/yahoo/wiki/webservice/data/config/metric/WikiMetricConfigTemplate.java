// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.metric;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.LinkedHashSet;
import java.util.Objects;

/**
 * Metric Config template.
 * <p>
 * An example:
 *
 *       {
 *          "metrics" :
 *              [
 *                  -> a list of metrics deserialize by WikiMetricTemplate
 *              ]
 *       }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class WikiMetricConfigTemplate {

    @JsonProperty("metrics")
    private LinkedHashSet<WikiMetricTemplate> metrics;

    /**
     * Constructor used by json parser.
     *
     * @param metrics json property metrics
     */
    public WikiMetricConfigTemplate(@JsonProperty("metrics") LinkedHashSet<WikiMetricTemplate> metrics) {
        this.metrics = (Objects.isNull(metrics) ? new LinkedHashSet<>() : new LinkedHashSet<>(metrics));
    }

    /**
     * Get metrics configuration info.
     *
     * @return a list of metrics
     */
    public LinkedHashSet<WikiMetricTemplate> getMetrics() {
        return this.metrics;
    }
}
