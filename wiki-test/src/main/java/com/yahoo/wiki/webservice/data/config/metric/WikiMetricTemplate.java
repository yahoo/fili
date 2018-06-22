// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.metric;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.util.EnumUtils;

import javax.validation.constraints.NotNull;
import java.util.*;
import java.util.stream.Collectors;

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
public class WikiMetricTemplate implements MetricConfigAPI {

    @JsonProperty("apiName")
    private final String apiName;

    @JsonProperty("longName")
    private final String longName;

    @JsonProperty("maker")
    private final String makerName;

    @JsonProperty("description")
    private final String description;

    @JsonProperty("dependencyMetricNames")
    private final List<String> dependencyMetricNames;

    @JsonProperty("satisfyingGrains")
    private final List<TimeGrain> satisfyingGrains;

    /**
     * Constructor used by json parser.
     *
     * @param apiName               json property apiName
     * @param longName              json property longName
     * @param makerName             json property makerName
     * @param description           json property description
     * @param dependencyMetricNames json property dependencyMetricNames
     * @param satisfyingGrains      json property satisfyingGrains
     */
    WikiMetricTemplate(
            @NotNull @JsonProperty("apiName") String apiName,
            @JsonProperty("longName") String longName,
            @JsonProperty("maker") String makerName,
            @JsonProperty("description") String description,
            @JsonProperty("dependencyMetricNames") List<String> dependencyMetricNames,
            @JsonProperty("satisfyingGrains") List<TimeGrain> satisfyingGrains
    ) {
        this.apiName = apiName.toLowerCase(Locale.ENGLISH);
        this.longName = (Objects.isNull(longName) ? EnumUtils.camelCase(apiName) : EnumUtils.camelCase(longName));
        this.makerName = makerName.toLowerCase(Locale.ENGLISH);
        this.description = (Objects.isNull(description) ? "" : description);
        this.dependencyMetricNames = (Objects.isNull(dependencyMetricNames) ?
                Collections.emptyList() : dependencyMetricNames.stream()
                        .map(name -> EnumUtils.camelCase(name))
                        .collect(Collectors.toList()));
        this.satisfyingGrains = (Objects.isNull(satisfyingGrains) ?
                Collections.emptyList() :
                Arrays.asList(satisfyingGrains.toArray(new TimeGrain[satisfyingGrains.size()])));
    }

    /**
     * Get metrics info.
     */
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
        return this.getApiName().toLowerCase(Locale.ENGLISH);
    }

    @Override
    public String asName() {
        return this.getApiName();
    }

    @Override
    public boolean isValidFor(TimeGrain grain) {
        // As long as the satisfying grains of this metric satisfy the requested grain
        return satisfyingGrains.stream().anyMatch(grain::satisfiedBy);
    }
}
