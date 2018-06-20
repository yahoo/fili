// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.metric;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.yahoo.wiki.webservice.data.config.Template;

import java.util.LinkedHashSet;

/**
 * Metric Config template.
 *
 * An example:
 *
 * {
 *   "metrics" :
 *     [
 *       -> a list of metrics deserialize by WikiMetricTemplate
 *     ]
 * }
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class WikiMetricConfigTemplate extends Template {

    @JsonProperty("metrics")
    private LinkedHashSet<WikiMetricTemplate> metrics;

    /**
     * Constructor used by json parser.
     *
     * @param metrics  json property metrics
     */
    public WikiMetricConfigTemplate(@JsonProperty("metrics") LinkedHashSet<WikiMetricTemplate> metrics) {
        setMetrics(metrics);
    }

    /**
     * Set metrics configuration info.
     *
     * @param metrics a list of metrics
     */
    public void setMetrics(LinkedHashSet<WikiMetricTemplate> metrics) {
        this.metrics = metrics;
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
