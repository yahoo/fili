// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider.yaml;

import com.yahoo.bard.webservice.data.config.metric.parser.MetricParser;
import com.yahoo.bard.webservice.data.config.provider.MakerDictionary;
import com.yahoo.bard.webservice.data.config.provider.MetricConfiguration;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.util.Objects;

/**
 * Yaml-configured metric.
 *
 * Definition is parsed using the built-in metrics parser.
 *
 * @see com.yahoo.bard.webservice.data.config.metric.parser.MetricParser
 */
public class YamlMetricConfiguration implements MetricConfiguration {

    protected String definition;
    protected String description;
    protected Boolean exclude;

    /**
     * Construct the YAML metric configuration.
     *
     * @param description metric description
     * @param definition metric definition
     * @param exclude true if metric is excluded from API (not yet implemented)
     */
    @JsonCreator
    public YamlMetricConfiguration(
            @JsonProperty("description") String description,
            @JsonProperty("def") String definition,
            @JsonProperty("exclude") Boolean exclude
    ) {
        this.description = description;
        this.definition = definition;
        this.exclude = exclude;
    }

    @Override
    public String getDefinition() {
        return definition;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public Boolean isExcluded() {
        return exclude;
    }

    @Override
    public LogicalMetric build(
            String metricName,
            MetricDictionary dict,
            MetricDictionary tempDict,
            MakerDictionary makerDict,
            DimensionDictionary dimensionDictionary
    ) throws IOException {
        MetricParser p = new MetricParser(metricName, definition, dict, tempDict, makerDict, dimensionDictionary);
        return p.parse();
    }

    @Override
    public boolean equals(Object object) {
        if (object == null || !(object instanceof YamlMetricConfiguration)) {
            return false;
        }

        YamlMetricConfiguration other = (YamlMetricConfiguration) object;
        return Objects.equals(definition, other.definition) &&
                Objects.equals(description, other.definition) &&
                Objects.equals(exclude, other.exclude);
    }

    @Override
    public int hashCode() {
        return Objects.hash(definition, description, exclude);
    }
}
