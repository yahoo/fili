// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider.yaml;

import com.yahoo.bard.webservice.data.config.provider.LogicalTableConfiguration;
import com.yahoo.bard.webservice.data.config.provider.yaml.serde.GranularityDeserializer;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.druid.model.query.Granularity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import javax.validation.constraints.NotNull;

/**
 * YAML configuration for a single LogicalTable.
 *
 * FIXME: Handle non-default time grains; should maybe be Set of Granularity instead?
 *
 */
public class YamlLogicalTableConfig implements LogicalTableConfiguration {

    protected Set<TimeGrain> timeGrains;
    protected Set<String> physicalTables;
    protected Set<String> metrics;

    /**
     * Construct the logical table configuration object.
     *
     * @param timeGrains the logical table time grains
     * @param physicalTables the physical tables backing the logical table
     * @param metrics the logical table metrics
     */
    @JsonCreator
    public YamlLogicalTableConfig(
            @NotNull @JsonProperty("granularities") @JsonDeserialize(contentUsing = GranularityDeserializer.class)
                    Granularity[] timeGrains,
            @NotNull @JsonProperty("physical_tables") String[] physicalTables,
            @NotNull @JsonProperty("metrics") String[] metrics
    ) {
        this.timeGrains = new HashSet<>();
        for (Granularity granularity : timeGrains) {
            if (!(granularity instanceof TimeGrain)) {
                throw new RuntimeException("Must construct logical table with only TimeGrains; found: " + granularity);
            }
            this.timeGrains.add((TimeGrain) granularity);
        }
        this.physicalTables = new HashSet<>(Arrays.asList(physicalTables));
        this.metrics = new HashSet<>(Arrays.asList(metrics));
    }

    @Override
    public Set<TimeGrain> getTimeGrains() {
        return timeGrains;
    }

    @Override
    public Set<String> getPhysicalTables() {
        return physicalTables;
    }

    @Override
    public Set<String> getMetrics() {
        return metrics;
    }

    // FIXME: I wanted to implement .equals and .hashCode on these objects but
    // couldn't convince the ClassScannerSpec to pass because it can't construct
    // valid objects out of thin air.

    @Override
    public boolean equals(Object other) {
        if (other == null || !(other instanceof YamlLogicalTableConfig)) {
            return false;
        }

        YamlLogicalTableConfig conf = (YamlLogicalTableConfig) other;
        return Objects.equals(timeGrains, conf.timeGrains) &&
                Objects.equals(physicalTables, conf.physicalTables) &&
                Objects.equals(metrics, conf.metrics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeGrains, physicalTables, metrics);
    }
}
