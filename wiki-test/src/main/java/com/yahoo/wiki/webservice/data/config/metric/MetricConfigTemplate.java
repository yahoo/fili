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
 *          "makers" :
 *              [
 *                  a list of metrics makers deserialize by MetricMakerTemplate
 *              ]
 *          "metrics" :
 *              [
 *                  a list of metrics deserialize by MetricTemplate
 *              ]
 *       }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MetricConfigTemplate {

    private final LinkedHashSet<MetricMakerTemplate> makers;
    private final LinkedHashSet<MetricTemplate> metrics;

    /**
     * Constructor used by json parser.
     *
     * @param makers  json property makers
     * @param metrics json property metrics
     */
    public MetricConfigTemplate(
            @JsonProperty("makers") LinkedHashSet<MetricMakerTemplate> makers,
            @JsonProperty("metrics") LinkedHashSet<MetricTemplate> metrics
    ) {
        this.makers = (Objects.isNull(makers) ? new LinkedHashSet<>() : new LinkedHashSet<>(makers));
        this.metrics = (Objects.isNull(metrics) ? new LinkedHashSet<>() : new LinkedHashSet<>(metrics));
    }

    /**
     * Get metrics makers configuration info.
     *
     * @return a list of metrics makers
     */
    public LinkedHashSet<MetricMakerTemplate> getMakers() {
        return this.makers;
    }

    /**
     * Get metrics configuration info.
     *
     * @return a list of metrics
     */
    public LinkedHashSet<MetricTemplate> getMetrics() {
        return this.metrics;
    }
}
