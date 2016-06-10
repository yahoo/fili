// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.application;

import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.COLOR;
import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.SHAPE;
import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.SIZE;

import com.yahoo.bard.webservice.config.BardFeatureFlag;
import com.yahoo.bard.webservice.data.cache.DataCache;
import com.yahoo.bard.webservice.data.cache.HashDataCache;
import com.yahoo.bard.webservice.data.cache.StubDataCache;
import com.yahoo.bard.webservice.data.cache.TestDataCache;
import com.yahoo.bard.webservice.data.cache.TestTupleDataCache;
import com.yahoo.bard.webservice.data.config.ConfigurationLoader;
import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.dimension.DimensionLoader;
import com.yahoo.bard.webservice.data.config.dimension.TestDimensions;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.table.TableLoader;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.metadata.QuerySigningService;
import com.yahoo.bard.webservice.metadata.SegmentIntervalsHashIdGenerator;
import com.yahoo.bard.webservice.models.druid.client.impl.TestDruidWebService;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.glassfish.hk2.utilities.binding.AbstractBinder;

import groovy.util.logging.Slf4j;

import java.util.LinkedHashSet;

/**
 * Bard test app configuration binder
 */
@Slf4j
public class TestBinderFactory extends AbstractBinderFactory {

    public LinkedHashSet<DimensionConfig> dimensionConfig;
    public MetricLoader metricLoader;
    public TableLoader tableLoader;
    public ApplicationState state;

    public boolean afterBindingHookWasCalled = false;
    public boolean afterRegistrationHookWasCalled = false;

    public ConfigurationLoader configurationLoader;

    public TestBinderFactory() {
        dimensionConfig = new TestDimensions().getDimensionConfigurationsByApiName(SIZE, COLOR, SHAPE);
        metricLoader = new MetricLoader() {
            @Override
            public void loadMetricDictionary(MetricDictionary dictionary) {
                // Empty
            }
        };
        tableLoader = new TableLoader() {
            @Override
            public void loadTableDictionary(ResourceDictionaries dictionaries) {
                // Empty
            }
        };
        state = new ApplicationState();
        state.uiWebService = new TestDruidWebService("default ui");
        state.nonUiWebService = new TestDruidWebService("default non ui");
        state.metadataWebService = new TestDruidWebService("default metadata service");
    }

    public TestBinderFactory(
            LinkedHashSet<DimensionConfig> dimensionConfig,
            MetricLoader metricLoader,
            TableLoader tableLoader,
            ApplicationState state
    ) {
        this.dimensionConfig = dimensionConfig;
        this.state = state;
        this.metricLoader = metricLoader;
        this.tableLoader = tableLoader;
    }

    public ConfigurationLoader getConfigurationLoader() {
        return configurationLoader;
    }

    public DruidWebService getNonUiDruidWebService() {
        return state.nonUiWebService;
    }

    public DruidWebService getUiDruidWebService() {
        return state.uiWebService;
    }

    public DruidWebService getMetadataDruidWebService() {
        return state.metadataWebService;
    }

    public QuerySigningService<?> getQuerySigningService() {
        return buildQuerySigningService(
                configurationLoader.getPhysicalTableDictionary(),
                getDataSourceMetaDataService()
        );
    }

    public DataSourceMetadataService getDataSourceMetaDataService() {
        return buildDataSourceMetadataService();
    }

    public DataCache<?> getDataCache() {
        return state.cache;
    }

    @Override
    protected ConfigurationLoader buildConfigurationLoader(
            DimensionLoader dimensionLoader,
            MetricLoader metricLoader,
            TableLoader tableLoader
    ) {
        // Store the config loader so that we can get access to it, and then return it
        this.configurationLoader = super.buildConfigurationLoader(dimensionLoader, metricLoader, tableLoader);
        return configurationLoader;
    }

    @Override
    protected MetricLoader getMetricLoader() {
        return metricLoader;
    }

    @Override
    protected LinkedHashSet<DimensionConfig> getDimensionConfigurations() {
        return dimensionConfig;
    }

    @Override
    protected TableLoader getTableLoader() {
        return tableLoader;
    }

    @Override
    protected DruidWebService buildUiDruidWebService(ObjectMapper mapper) {
        return state.uiWebService;
    }

    @Override
    protected DruidWebService buildNonUiDruidWebService(ObjectMapper mapper) {
        return state.nonUiWebService;
    }

    @Override
    protected DruidWebService buildMetadataDruidWebService(ObjectMapper mapper) {
        return state.metadataWebService;
    }

    @Override
    protected QuerySigningService<?> buildQuerySigningService(
            PhysicalTableDictionary physicalTableDictionary,
            DataSourceMetadataService dataSourceMetadataService
    ) {
        return state.querySigningService == null ?
                super.buildQuerySigningService(physicalTableDictionary, dataSourceMetadataService) :
                (SegmentIntervalsHashIdGenerator) state.querySigningService;
    }

    @Override
    protected DataCache<?> buildCache() {
        if (BardFeatureFlag.DRUID_CACHE.isOn()) {
            // test cache stored in memory
            if (BardFeatureFlag.DRUID_CACHE_V2.isOn()) {
                state.cache = new TestTupleDataCache();
            } else {
                state.cache = new HashDataCache<>(new TestDataCache());
            }
        } else {
            state.cache = new StubDataCache<>();
        }

        return state.cache;
    }

    protected DataCache<?> getCache() {
        return state.cache == null ? buildCache() : state.cache;
    }

    @Override
    public void afterRegistration(ResourceConfig resourceConfig) {
        afterRegistrationHookWasCalled = true;
    }

    @Override
    protected void afterBinding(AbstractBinder abstractBinder) {
        afterBindingHookWasCalled = true;
    }
}
