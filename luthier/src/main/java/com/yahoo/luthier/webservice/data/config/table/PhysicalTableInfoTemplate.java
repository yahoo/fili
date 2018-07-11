// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain;
import com.yahoo.bard.webservice.util.EnumUtils;

import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
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
public class PhysicalTableInfoTemplate implements TableName {

    private final String name;
    private final String description;
    private final List<String> metrics;
    private final List<String> dimensions;
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
    public PhysicalTableInfoTemplate(
            @NotNull @JsonProperty("name") String name,
            @JsonProperty("description") String description,
            @JsonProperty("metrics") List<String> metrics,
            @JsonProperty("dimensions") List<String> dimensions,
            @JsonProperty("granularity") String granularity
    ) {
        this.name = EnumUtils.camelCase(name);
        this.description = (Objects.isNull(description) ? "" : description);
        this.metrics = (Objects.isNull(metrics) ?
                Collections.emptyList() : metrics.stream()
                .map(metric -> EnumUtils.camelCase(metric))
                .collect(Collectors.toList()));
        this.dimensions = (Objects.isNull(dimensions) ?
                Collections.emptyList() : dimensions.stream()
                .map(dimension -> EnumUtils.camelCase(dimension))
                .collect(Collectors.toList()));
        this.granularity = (Objects.isNull(granularity) ?
                DefaultTimeGrain.HOUR : DefaultTimeGrain.valueOf(granularity));
    }

    /**
     * Get physical table name.
     *
     * @return physical table name
     */
    public String getName() {
        return this.name;
    }

    /**
     * Get physical table description.
     *
     * @return physical table description
     */
    public String getDescription() {
        return this.description;
    }

    /**
     * Get physical table's metrics name.
     *
     * @return physical table's metrics
     */
    public List<String> getMetrics() {
        return this.metrics;
    }

    /**
     * Get physical table's dimensions.
     *
     * @return physical table's dimensions
     */
    public List<String> getDimensions() {
        return this.dimensions;
    }

    /**
     * Get physical table's granularity.
     *
     * @return physical table's granularity
     */
    public DefaultTimeGrain getGranularity() {
        return this.granularity;
    }

    @Override
    public String asName() {
        return this.name;
    }
}
