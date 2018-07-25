// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config.metric;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.constraints.NotNull;
import com.yahoo.bard.webservice.util.EnumUtils;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Wiki metric template.
 * <p>
 * An example:
 * <p>
 *      {
 *          "apiName" : "ADDED",
 *          "longName" : "ADDED",
 *          "description" : "Description for added",
 *          "maker" : "DoubleSum",
 *          "dependencyMetricNames" : ["ADDED"]
 *      }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DefaultMetricTemplate implements MetricTemplate {

    private final String apiName;
    private final String longName;
    private final String makerName;
    private final String description;
    private final List<String> dependencyMetricNames;

    /**
     * Constructor used by json parser.
     *
     * @param apiName               json property apiName
     * @param longName              json property longName
     * @param makerName             json property makerName
     * @param description           json property description
     * @param dependencyMetricNames json property dependencyMetricNames
     */
    DefaultMetricTemplate(
            @NotNull @JsonProperty("apiName") String apiName,
            @JsonProperty("longName") String longName,
            @JsonProperty("maker") String makerName,
            @JsonProperty("description") String description,
            @JsonProperty("dependencyMetricNames") List<String> dependencyMetricNames
    ) {
        this.apiName = apiName;
        this.longName = (Objects.isNull(longName) ? apiName : longName);
        this.makerName = EnumUtils.camelCase(makerName);
        this.description = (Objects.isNull(description) ? "" : description);
        this.dependencyMetricNames = Objects.isNull(dependencyMetricNames) ?
                Collections.emptyList() : dependencyMetricNames;
    }

    @Override
    public String getApiName() {
        return this.apiName;
    }

    @Override
    public String getLongName() {
        return this.longName;
    }

    @Override
    public String getMakerName() {
        return this.makerName;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public List<String> getDependencyMetricNames() {
        return this.dependencyMetricNames;
    }

    @Override
    public String toString() {
        return getApiName();
    }
}
