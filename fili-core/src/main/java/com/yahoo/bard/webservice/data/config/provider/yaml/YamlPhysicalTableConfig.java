// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider.yaml;

import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.config.provider.ConfigurationDictionary;
import com.yahoo.bard.webservice.data.config.provider.ConfigurationError;
import com.yahoo.bard.webservice.data.config.provider.PhysicalTableConfiguration;
import com.yahoo.bard.webservice.data.config.provider.yaml.serde.GranularityDeserializer;
import com.yahoo.bard.webservice.data.config.table.PhysicalTableDefinition;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.data.time.ZonelessTimeGrain;
import com.yahoo.bard.webservice.druid.model.query.Granularity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.joda.time.DateTimeZone;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * YAML configuration for physical table.
 *
 * FIXME: Timegrain has changed since I wrote originally; probably needs configurable time zone
 */
public class YamlPhysicalTableConfig implements PhysicalTableConfiguration {

    protected TableName name;
    protected ZonedTimeGrain grain;
    protected List<String> dimensions;
    protected List<String> metrics;

    /**
     * Create the physical table configuration.
     * @param grain the table time grain
     * @param dimensions the table dimensions
     * @param metrics the table metrics
     */
    @JsonCreator
    public YamlPhysicalTableConfig(
            @JsonProperty("granularity") @JsonDeserialize(using = GranularityDeserializer.class) Granularity grain,
            @JsonProperty("dimensions") String[] dimensions,
            @JsonProperty("metrics") String[] metrics
    ) {

        if (!(grain instanceof ZonelessTimeGrain)) {
            throw new ConfigurationError("ZonelessTimeGrain required; found " + grain);
        }
        if (dimensions == null || dimensions.length == 0) {
            throw new ConfigurationError("Physical table must be configured with dimensions; found null.");
        }

        if (metrics == null || metrics.length == 0) {
            throw new ConfigurationError("Physical table must be configured with metrics; found null.");
        }

        this.grain = new ZonedTimeGrain((ZonelessTimeGrain) grain, DateTimeZone.UTC);
        this.dimensions = Arrays.asList(dimensions);
        this.metrics = Arrays.asList(metrics);
    }

    /**
     * Set the table name.
     *
     * Not intended for public use; called from Jackson deserialization code on construction
     *
     * @param tableName the table name
     */
    public void setTableName(String tableName) {
        this.name = new YamlTableName(tableName);
    }

    @Override
    public PhysicalTableDefinition buildPhysicalTable(ConfigurationDictionary<DimensionConfig> dimensionMap) {

        Set<DimensionConfig> dimSet = dimensions
                .stream()
                .filter(dimensionMap::containsKey)
                .map(dimensionMap::get)
                .collect(Collectors.toSet());

        return new PhysicalTableDefinition(name, grain, dimSet);
    }

    @Override
    public Set<FieldName> getMetrics() {
        return metrics.stream().map(YamlFieldName::new).collect(Collectors.toSet());
    }

    /**
     * A FieldName.
     *
     * Was this, but problems with creating a set of them (.equals doesn't work):
     *
     *   return stream.map(f -&gt; (FieldName) () -&gt; f).collect(Collectors.toSet());
     */
    public static class YamlFieldName implements FieldName {
        protected String name;

        /**
         * Construct a new name.
         *
         * @param name the field name
         */
        public YamlFieldName(String name) {
            this.name = name;
        }

        @Override
        public String asName() {
            return name;
        }

        @Override
        public boolean equals(Object other) {
            if (other == null || !(other instanceof YamlFieldName)) {
                return false;
            } else {
                return Objects.equals(this.name, ((YamlFieldName) other).name);
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.name);
        }
    }
}
