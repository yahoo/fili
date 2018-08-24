// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config.metric;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import javax.validation.constraints.NotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;


/**
 * Wiki metric maker config template.
 * <p>
 * An example:
 * <p>
 *      {
 *          "name": "bigThetaSketch",
 *          "class": "com.yahoo.bard.webservice.data.config.metric.makers.ThetaSketchMaker",
 *          "params": {
 *              "sketchSize": "4096"
 *          }
 *      }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DefaultMetricMakerTemplate implements MetricMakerTemplate {

    private final String name;
    private final String fullyQualifiedClassName;
    private final Map<String, Object> parameters;

    /**
     * Constructor used by json parser.
     *
     * @param name       json property name
     * @param fullyQualifiedClassName  json property class
     * @param parameters json property parameters
     * Todo: change lua code to rename "classPath"
     */
    public DefaultMetricMakerTemplate(
            @NotNull @JsonProperty("name") String name,
            @NotNull @JsonProperty("classPath") String fullyQualifiedClassName,
            @JsonProperty("params") Map<String, Object> parameters
    ) {
        this.name = name;
        this.fullyQualifiedClassName = fullyQualifiedClassName;
        this.parameters = (Objects.isNull(parameters) ? new HashMap<>() : ImmutableMap.copyOf(parameters));
    }

    /**
     * Get maker's name.
     *
     * @return maker's name
     */
    @Override
    public String getName() {
        return this.name;
    }

    /**
     * Get maker's class.
     *
     * @return maker's class
     */
    @Override
    public String getFullyQualifiedClassName() {
        return this.fullyQualifiedClassName;
    }

    /**
     * Get maker's parameters.
     *
     * @return maker's parameters
     */
    @Override
    public Map<String, Object> getParams() {
        return this.parameters;
    }
}
