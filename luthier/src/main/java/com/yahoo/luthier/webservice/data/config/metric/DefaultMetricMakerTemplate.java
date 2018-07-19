// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config.metric;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import javax.validation.constraints.NotNull;

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
 *          "sketchSize": "4096"
 *          }
 *      }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DefaultMetricMakerTemplate implements MetricMakerTemplate {

    private final String name;
    private final String classPath;
    private final Map<String, String> parameters;

    /**
     * Constructor used by json parser.
     *
     * @param name       json property name
     * @param classPath  json property classPath
     * @param parameters json property parameters
     */
    public DefaultMetricMakerTemplate(
            @NotNull @JsonProperty("name") String name,
            @NotNull @JsonProperty("class") String classPath,
            @JsonProperty("params") Map<String, String> parameters
    ) {
        this.name = name;
        this.classPath = classPath;
        this.parameters = (Objects.isNull(parameters) ? null : ImmutableMap.copyOf(parameters));;
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
    public String getClassPath() {
        return this.classPath;
    }

    /**
     * Get maker's parameters.
     *
     * @return maker's parameters
     */
    @Override
    public Map<String, String> getParams() {
        return this.parameters;
    }
}
