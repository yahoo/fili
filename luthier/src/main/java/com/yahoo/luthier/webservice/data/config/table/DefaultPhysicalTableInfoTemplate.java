// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.yahoo.bard.webservice.data.time.DefaultTimeGrain;
import com.yahoo.bard.webservice.util.EnumUtils;

import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

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
    public DefaultPhysicalTableInfoTemplate(
            @NotNull @JsonProperty("name") String name,
            @JsonProperty("description") String description,
            @JsonProperty("metrics") List<String> metrics,
            @JsonProperty("dimensions") List<String> dimensions,
            @JsonProperty("granularity") String granularity
    ) {
        this.name = EnumUtils.camelCase(name);
        this.description = (Objects.isNull(description) ? "" : description);
        this.metrics = !Objects.isNull(metrics) ?
                metrics : Collections.emptyList();
        this.dimensions = !Objects.isNull(dimensions) ? dimensions : Collections.emptyList();
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
    public List<String> getMetrics() {
        return this.metrics;
    }

    @Override
    public List<String> getDimensions() {
        return this.dimensions;
    }

    @Override
    public DefaultTimeGrain getGranularity() {
        return this.granularity;
    }
}
