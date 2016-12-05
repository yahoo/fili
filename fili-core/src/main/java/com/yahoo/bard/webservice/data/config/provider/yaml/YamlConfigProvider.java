// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider.yaml;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.provider.ConfigProvider;
import com.yahoo.bard.webservice.data.config.provider.ConfigurationDictionary;
import com.yahoo.bard.webservice.data.config.provider.ConfigurationError;
import com.yahoo.bard.webservice.data.config.provider.LogicalTableConfiguration;
import com.yahoo.bard.webservice.data.config.provider.MakerConfiguration;
import com.yahoo.bard.webservice.data.config.provider.MetricConfiguration;
import com.yahoo.bard.webservice.data.config.provider.PhysicalTableConfiguration;
import com.yahoo.bard.webservice.data.config.provider.yaml.serde.YamlDimensionConfigDeserializer;
import com.yahoo.bard.webservice.data.config.provider.yaml.serde.YamlDimensionFieldConfigDeserializer;
import com.yahoo.bard.webservice.data.config.provider.yaml.serde.YamlPhysicalTableConfigDeserializer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import javax.validation.constraints.NotNull;

/**
 * YAML-based configuration.
 */
public class YamlConfigProvider implements ConfigProvider {
    private static final Logger LOG = LoggerFactory.getLogger(YamlConfigProvider.class);
    public static final String CONF_YAML_PATH = "config_binder_yaml_path";
    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

    protected ConfigurationDictionary<PhysicalTableConfiguration> physicalTables;
    protected ConfigurationDictionary<MakerConfiguration> makers;
    protected ConfigurationDictionary<LogicalTableConfiguration> logicalTables;
    protected ConfigurationDictionary<DimensionConfig> dimensions;
    protected ConfigurationDictionary<MetricConfiguration> baseMetrics;
    protected ConfigurationDictionary<MetricConfiguration> derivedMetrics;

    /**
     * Instantiate the yaml-based configuration.
     *
     * @param physicalTables the physical table configuration
     * @param logicalTables the logical table configuration
     * @param dimensions the dimension configuration
     * @param dimensionFields the dimension field configuration
     * @param baseMetrics the base (Druid) metric configuration
     * @param derivedMetrics the derived metric configuration
     * @param makers the metric makers
     */
    @JsonCreator
    public YamlConfigProvider(
            @NotNull
            @JsonProperty("physical_tables")
            @JsonDeserialize(using = YamlPhysicalTableConfigDeserializer.class)
                    ConfigurationDictionary<PhysicalTableConfiguration> physicalTables,

            @NotNull
            @JsonProperty("logical_tables")
            @JsonDeserialize(contentAs = YamlLogicalTableConfig.class)
                    ConfigurationDictionary<LogicalTableConfiguration> logicalTables,

            @NotNull
            @JsonProperty("dimensions")
            @JsonDeserialize(using = YamlDimensionConfigDeserializer.class, contentAs = YamlDimensionConfig.class)
                    ConfigurationDictionary<DimensionConfig> dimensions,

            // Don't need a 'nice' type here, since not exposed
            @NotNull
            @JsonProperty("dimension_fields")
            @JsonDeserialize(using = YamlDimensionFieldConfigDeserializer.class)
                    ConfigurationDictionary<YamlDimensionFieldConfig> dimensionFields,

            @NotNull
            @JsonProperty("base_metrics")
            @JsonDeserialize(contentAs = YamlMetricConfiguration.class)
                    ConfigurationDictionary<MetricConfiguration> baseMetrics,

            @JsonProperty("derived_metrics")
            @JsonDeserialize(contentAs = YamlMetricConfiguration.class)
                    ConfigurationDictionary<MetricConfiguration> derivedMetrics,

            // Any custom metric makers
            @JsonProperty("makers")
            @JsonDeserialize(contentAs = YamlMakerConfiguration.class)
                    ConfigurationDictionary<MakerConfiguration> makers

    ) {
        this.physicalTables = physicalTables;
        this.logicalTables = logicalTables;
        this.dimensions = dimensions;
        this.baseMetrics = baseMetrics;
        this.derivedMetrics = derivedMetrics;
        this.makers = makers;

        // Derived metrics are not required
        if (derivedMetrics == null) {
            LOG.info("No derived metrics found.");
            this.derivedMetrics = new ConfigurationDictionary<>();
        }

        // Custom makers are not required
        if (makers == null) {
            LOG.info("No custom metric makers found.");
            this.makers = new ConfigurationDictionary<>();
        }

        // Dimensions must know their fields
        dimensions.values().forEach(v -> ((YamlDimensionConfig) v).setAvailableDimensionFields(dimensionFields));
    }

    @Override
    public ConfigurationDictionary<PhysicalTableConfiguration> getPhysicalTableConfig() {
        return physicalTables;
    }

    @Override
    public ConfigurationDictionary<LogicalTableConfiguration> getLogicalTableConfig() {
        return logicalTables;
    }

    @Override
    public ConfigurationDictionary<DimensionConfig> getDimensionConfig() {
        return dimensions;
    }

    @Override
    public ConfigurationDictionary<MetricConfiguration> getBaseMetrics() {
        return baseMetrics;
    }

    @Override
    public ConfigurationDictionary<MetricConfiguration> getDerivedMetrics() {
        return derivedMetrics;
    }

    @Override
    public ConfigurationDictionary<MakerConfiguration> getCustomMakerConfig() {
        return makers;
    }

    /**
     * Build self from yaml file.
     *
     * @param systemConfig the system configuration
     * @return a ConfigProvider
     *
     * @see com.yahoo.bard.webservice.data.config.provider.ConfigBinderFactory
     */
    public static ConfigProvider build(SystemConfig systemConfig) {
        String path = systemConfig.getStringProperty(
                systemConfig.getPackageVariableName(CONF_YAML_PATH)
        );

        if (path == null) {
            throw new ConfigurationError("Could not read path variable: " + CONF_YAML_PATH);
        }

        File f = new File(path);
        if (!f.exists() || !f.canRead()) {
            throw new ConfigurationError("Could not read path: " + path + ". Please ensure it exists and is readable.");
        }

        try {
            LOG.info("Loading YAML configuration from path: {}", path);
            return build(f);
        } catch (Exception e) {
            throw new ConfigurationError("Could not parse path: " + path, e);
        }
    }

    /**
     * Build self from yaml file.
     *
     * @param file the YAML file to read
     * @return a ConfigProvider
     * @throws IOException when the file cannot be parsed
     * @see com.yahoo.bard.webservice.data.config.provider.ConfigBinderFactory
     */
    public static ConfigProvider build(File file) throws IOException {
        return MAPPER.readValue(file, YamlConfigProvider.class);
    }
}
