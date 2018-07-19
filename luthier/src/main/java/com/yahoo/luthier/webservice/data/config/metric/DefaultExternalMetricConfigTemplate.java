// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config.metric;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;

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
public class DefaultExternalMetricConfigTemplate implements ExternalMetricConfigTemplate {

    private final Set<MetricMakerTemplate> makers;
    private final Set<MetricTemplate> metrics;

    /**
     * Constructor used by json parser.
     *
     * @param makers  json property makers
     * @param metrics json property metrics
     */
    public DefaultExternalMetricConfigTemplate(
            @JsonProperty("makers") Set<MetricMakerTemplate> makers,
            @JsonProperty("metrics") Set<MetricTemplate> metrics
    ) {
        this.makers = makers;
        this.metrics = metrics;
    }

    @Override
    public Set<MetricMakerTemplate> getMakers() {
        return this.makers;
    }

    @Override
    public Set<MetricTemplate> getMetrics() {
        return this.metrics;
    }
}
