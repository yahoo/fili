// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.provider;

import com.yahoo.bard.webservice.application.AbstractBinderFactory;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.table.TableLoader;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Config-driven AbstractBinderFactory.
 */
public class ConfigBinderFactory extends AbstractBinderFactory {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigBinderFactory.class);

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    private static final String CONF_TYPE = SYSTEM_CONFIG.getPackageVariableName("config_binder_type");

    private final ConfigProvider provider;

    protected final MetricDictionary localDictionary = new MetricDictionary();

    protected final MakerBuilder makerBuilder;

    protected final DimensionDictionary dimensionDictionary = new DimensionDictionary();
    protected final Set<DimensionConfig> dimensionConfigs = new HashSet<>();

    /**
     * Construct a ConfigBinderFactory that instantiates a config provider via reflection.
     */
    public ConfigBinderFactory() {
        String className = SYSTEM_CONFIG.getStringProperty(CONF_TYPE);

        if (className == null) {
            throw new ConfigurationError("Unable to read class name property from config: " + CONF_TYPE);
        }

        try {
            // oof
            LOG.info("Loading ConfigBinderFactory for type: {}", className);
            Class<? extends ConfigProvider> providerClass = (Class<? extends ConfigProvider>) Class.forName(className);
            Method build = providerClass.getDeclaredMethod("build", SystemConfig.class);
            provider = (ConfigProvider) build.invoke(null, SYSTEM_CONFIG);
        } catch (Exception e) {
            throw new ConfigurationError("Unable to construct config provider", e);
        }

        makerBuilder = new MakerBuilder(provider.getCustomMakerConfig());

        // FIXME
        // Everything from here below is annoying.

        List<DimensionConfiguration> dimensionConfig = provider.getDimensionConfig();
        List<DimensionFieldConfiguration> dimensionFieldConfig = provider.getDimensionFieldConfig();

        // Build up our dimension dictionary.
        // This just munges the dimension and field configs together, but it's a little nasty.
        Map<String, DimensionFieldConfiguration> availableFields = new HashMap<>();

        for (DimensionFieldConfiguration conf : dimensionFieldConfig) {
            availableFields.put(conf.getName(), conf);
        }

        for (DimensionConfiguration conf : dimensionConfig) {
            LinkedHashSet<DimensionField> allFields;
            LinkedHashSet<DimensionField> defaultFields;

            // If we specified fields, use them.
            if (conf.getFields() != null) {
                if (!availableFields.keySet().containsAll(conf.getFields())) {
                    throw new ConfigurationError("Requested undefined dimension fields on dimension " + conf.getApiName() + ". Requested fields: " + conf.getFields() + "; defined fields:" + availableFields.keySet());
                }
                allFields = conf.getFields().stream()
                        .map(availableFields::get)
                        .collect(Collectors.toCollection(LinkedHashSet<DimensionField>::new));
            } else {
                allFields = availableFields.values().stream().filter(DimensionFieldConfiguration
                        ::isDimensionIncludedByDefault).collect(
                        Collectors.toCollection(LinkedHashSet<DimensionField>::new));
                if (allFields.size() == 0) {
                    throw new RuntimeException("Dimension " + conf.getApiName() + " has no fields!");
                }
            }

            // If we've got default fields, use them.
            // Otherwise, pull in defaults.
            if (conf.getDefaultFields() != null) {
                if (!availableFields.keySet().containsAll(conf.getDefaultFields())) {
                    throw new ConfigurationError("Requested undefined dimension fields on dimension " + conf.getApiName() + ". Requested default fields: " + conf.getDefaultFields() + "; defined fields:" + availableFields.keySet());
                }

                defaultFields = conf.getDefaultFields().stream()
                        .map(availableFields::get)
                        .collect(Collectors.toCollection(LinkedHashSet<DimensionField>::new));
            } else {
                defaultFields = availableFields.values().stream().filter(DimensionFieldConfiguration
                        ::isQueryIncludedByDefault).collect(
                        Collectors.toCollection(LinkedHashSet<DimensionField>::new));
                if (defaultFields.size() == 0) {
                    throw new RuntimeException("Dimension " + conf.getApiName() + " has no fields!");
                }
            }

            DimensionConfigImpl dimConf = new DimensionConfigImpl(
                    conf.getApiName(),
                    conf.getLongName(),
                    conf.getCategory(),
                    conf.getPhysicalName(),
                    conf.getDescription(),
                    allFields,
                    defaultFields,
                    conf.getKeyValueStore(),
                    conf.getSearchProvider(),
                    conf.isAggregatable(),
                    KeyValueStoreDimension.class
            );

            dimensionConfigs.add(dimConf);

            // FIXME: type here should eventually be configurable
            dimensionDictionary.add(new KeyValueStoreDimension(dimConf));
        }
    }

    @Override
    protected MetricLoader getMetricLoader() {
        return new ConfiguredMetricLoader(
                provider.getMetricConfig(),
                provider.getLogicalMetricBuilder()
        );
    }

    @Override
    protected Set<DimensionConfig> getDimensionConfigurations() {
        return dimensionConfigs;
    }

    @Override
    protected TableLoader getTableLoader() {
        return new ConfiguredTableLoader(
                provider.getLogicalTableConfig(),
                provider.getPhysicalTableConfig(),
                dimensionConfigs
        );
    }
}
