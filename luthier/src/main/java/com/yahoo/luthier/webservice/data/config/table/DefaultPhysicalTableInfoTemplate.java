// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.google.common.collect.ImmutableSet;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.config.table.ConcretePhysicalTableDefinition;
import com.yahoo.bard.webservice.data.config.table.PhysicalTableDefinition;
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain;
import org.joda.time.DateTimeZone;

import javax.validation.constraints.NotNull;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Wiki physical table information template.
 * <p>
 * An example:
 * <p>
 *      {
 *          "name": "WIKITICKER",
 *          "description": "",
 *          "metrics": [
 *              "count",
 *              "added",
 *          ],
 *          "dimensions": [
 *              "COMMENT",
 *              "COUNTRY_ISO_CODE",
 *              "REGION_ISO_CODE",
 *          ],
 *          "granularity": "HOUR"
 *      }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DefaultPhysicalTableInfoTemplate implements PhysicalTableInfoTemplate {

    private final String name;
    private final String description;
    private final Set<String> metrics;
    private final Set<String> dimensions;
    private final DefaultTimeGrain granularity;

    /**
     * Constructor used by json parser.
     *
     * @param name        json property name
     * @param description json property description
     * @param metrics     json property metrics
     * @param dimensions  json property dimensions
     * @param granularity json property granularity
     */
    @JsonCreator
    public DefaultPhysicalTableInfoTemplate(
            @NotNull @JsonProperty("name") String name,
            @JsonProperty("description") String description,
            @JsonProperty("metrics") Set<String> metrics,
            @JsonProperty("dimensions") Set<String> dimensions,
            @JsonProperty("granularity") String granularity
    ) {
        this.name = name;
        this.description = (Objects.isNull(description) ? "" : description);
        this.metrics = Objects.isNull(metrics) ?
                Collections.emptySet() : ImmutableSet.copyOf(metrics);
        this.dimensions = Objects.isNull(dimensions) ? Collections.emptySet() : ImmutableSet.copyOf(dimensions);
        this.granularity = (Objects.isNull(granularity) ?
                DefaultTimeGrain.HOUR : DefaultTimeGrain.valueOf(granularity));
    }

    public String getName() {
        return this.name;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public Set<String> getMetrics() {
        return this.metrics;
    }

    @Override
    public Set<String> getDimensions() {
        return this.dimensions;
    }

    @Override
    public DefaultTimeGrain getGranularity() {
        return this.granularity;
    }

    @Override
    public PhysicalTableDefinition build(Map<String, DimensionConfig> dimensionsMap) {

        // Metrics for this physical table
        Set<FieldName> metricsSet = metrics.stream()
                .map(ApiMetricName::of)
                .collect(Collectors.toSet());

        // Dimensions for this physical table
        Set<DimensionConfig> dimensionsSet = dimensions.stream()
                .map(dimensionsMap::get)
                .collect(Collectors.toSet());

        return new ConcretePhysicalTableDefinition(
                TableName.of(name),
                granularity.buildZonedTimeGrain(DateTimeZone.UTC),
                metricsSet,
                dimensionsSet
        );
    }
}
