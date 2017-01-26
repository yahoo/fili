// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.provider;

import com.yahoo.bard.webservice.application.AbstractBinderFactory;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.config.provider.descriptor.DimensionDescriptor;
import com.yahoo.bard.webservice.data.config.provider.descriptor.DimensionFieldDescriptor;
import com.yahoo.bard.webservice.data.config.provider.descriptor.MetricDescriptor;
import com.yahoo.bard.webservice.data.config.provider.impl.ApiMetricNameImpl;
import com.yahoo.bard.webservice.data.config.provider.impl.DimensionConfigImpl;
import com.yahoo.bard.webservice.data.config.table.TableLoader;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.druid.model.query.Granularity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
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
    protected final DimensionDictionary dimensionDictionary = new DimensionDictionary();
    protected final MakerBuilder makerBuilder;
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
            // Probably don't need to pass in the SystemConfig here.
            LOG.info("Loading ConfigBinderFactory for type: {}", className);
            Class<? extends ConfigProvider> providerClass = (Class<? extends ConfigProvider>) Class.forName(className);
            Method build = providerClass.getDeclaredMethod("build", SystemConfig.class);
            provider = (ConfigProvider) build.invoke(null, SYSTEM_CONFIG);
        } catch (Exception e) {
            throw new ConfigurationError("Unable to construct config provider", e);
        }

        makerBuilder = new MakerBuilder(provider.getCustomMakerConfig());

        initializeDimensionConfiguration();
    }

    @Override
    protected MetricLoader getMetricLoader() {
        return new ConfiguredMetricLoader(
                provider.getMetricConfig(),
                provider.getLogicalMetricBuilder(localDictionary, makerBuilder, dimensionDictionary)
        );
    }

    @Override
    protected Set<DimensionConfig> getDimensionConfigurations() {
        return dimensionConfigs;
    }

    @Override
    protected TableLoader getTableLoader() {
        HashMap<String, ApiMetricName> apiMetrics = new HashMap<>();
        for (MetricDescriptor metric : provider.getMetricConfig()) {
            Predicate<Granularity> predicate;

            // If no granularity is specified, valid for all
            if (metric.getValidGrains() == null) {
                predicate = p -> true;
            } else {
                predicate = grain -> metric.getValidGrains().stream().anyMatch(grain::satisfiedBy);
            }

            ApiMetricNameImpl apiMetric = new ApiMetricNameImpl(metric.getName(), predicate);
            apiMetrics.put(metric.getName(), apiMetric);
        }

        return new ConfiguredTableLoader(
                provider.getLogicalTableConfig(),
                provider.getPhysicalTableConfig(),
                dimensionConfigs,
                apiMetrics
        );
    }

    /**
     * Initialize dimension configuration objects and dimension dictionary.
     *
     * This munges together dimensions and dimension field configurations, and
     * creates our temporary dimension dictionary (for use in metric loading).
     */
    protected void initializeDimensionConfiguration() {

        List<DimensionDescriptor> dimensionDescriptors = provider.getDimensionConfig();
        List<DimensionFieldDescriptor> fieldDescriptors = provider.getDimensionFieldConfig();

        Map<String, DimensionFieldDescriptor> fields = fieldDescriptors.stream()
                .collect(Collectors.toMap(DimensionFieldDescriptor::getName, v -> v));

        for (DimensionDescriptor d : dimensionDescriptors) {
            String name = d.getApiName();

            // Should KeyValueStoreDimension be configurable here?
            DimensionConfigImpl dimConf = new DimensionConfigImpl(
                    name,
                    d.getLongName(),
                    d.getCategory(),
                    d.getPhysicalName(),
                    d.getDescription(),
                    collectFields(d.getFields(), name, fields, df -> df.isDimensionIncludedByDefault()),
                    collectFields(d.getDefaultFields(), name, fields, df -> df.isQueryIncludedByDefault()),
                    d.getKeyValueStore(),
                    d.getSearchProvider(),
                    d.isAggregatable(),
                    KeyValueStoreDimension.class
            );

            dimensionConfigs.add(dimConf);

            // Annoyingly, we need to build our own dimension dictionary temporarily. Could go away if API changed.
            dimensionDictionary.add(new KeyValueStoreDimension(dimConf));
        }
    }

    /**
     * Collect the DimensionFields for a given dimension.
     *
     * @param fieldNames  the field names requested by the dimension
     * @param dimensionName  the dimension name, used only for debug messages
     * @param dimFields  Map of dimension fields by name
     * @param defaultInclude  Predicate used to choose fields from dimFields when fieldNames is unconfigured
     * @return Set of dimension fields for the dimension
     */
    protected LinkedHashSet<DimensionField> collectFields(
            Set<String> fieldNames,
            String dimensionName,
            Map<String, DimensionFieldDescriptor> dimFields,
            Predicate<DimensionFieldDescriptor> defaultInclude) {

        if (fieldNames == null) {
            fieldNames = dimFields.values().stream()
                    .filter(defaultInclude)
                    .map(DimensionFieldDescriptor::getName)
                    .collect(Collectors.toSet());
        }

        // Verify that we didn't ask for any fields that don't exist.
        if (!dimFields.keySet().containsAll(fieldNames)) {
            StringBuilder message = new StringBuilder();
            message.append("Requested undefined dimension fields on dimension ");
            message.append(dimensionName);
            message.append(". Requested fields: ");
            message.append(fieldNames);
            message.append("; defined fields: ");
            message.append(dimFields.keySet());
            throw new ConfigurationError(message.toString());
        }

        LinkedHashSet<DimensionField> fields = fieldNames.stream()
                .map(dimFields::get)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        if (fields.size() == 0) {
            LOG.debug("Dimension {} configured with zero fields.", dimensionName);
        }

        return fields;
    }
}
