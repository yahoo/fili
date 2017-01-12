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
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Config-driven AbstractBinderFactory.
 */
public class ConfigBinderFactory extends AbstractBinderFactory {
    private static final Logger LOG = LoggerFactory.getLogger(SystemConfigProvider.class);

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    private static final String CONF_TYPE = SYSTEM_CONFIG.getPackageVariableName("config_binder_type");

    private final ConfigProvider provider;

    // FIXME: Can't remember why I did this this way. Probably needs to be cleaned up.
    protected final MetricDictionary localDictionary = new MetricDictionary();

    protected final MakerBuilder makerBuilder;

    protected final DimensionDictionary dimensionDictionary = new DimensionDictionary();

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

        // This is annoying
        provider.getDimensionConfig().values().forEach(
                v -> dimensionDictionary.add(new KeyValueStoreDimension(v))
        );
    }

    @Override
    protected MetricLoader getMetricLoader() {
        return new ConfiguredMetricLoader(
                localDictionary,
                provider.getBaseMetrics(),
                provider.getDerivedMetrics(),
                makerBuilder,
                dimensionDictionary
        );
    }

    @Override
    protected Set<DimensionConfig> getDimensionConfigurations() {
        return new LinkedHashSet<>(
                provider.getDimensionConfig().values()
        );
    }

    @Override
    protected TableLoader getTableLoader() {
        return new ConfiguredTableLoader(
                provider.getLogicalTableConfig(),
                provider.getPhysicalTableConfig(),
                provider.getDimensionConfig()
        );
    }
}
