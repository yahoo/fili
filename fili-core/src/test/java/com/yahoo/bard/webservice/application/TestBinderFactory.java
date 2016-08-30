// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.application;

import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.COLOR;
import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.SHAPE;
import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.SIZE;
import static com.yahoo.bard.webservice.data.config.names.TestDruidTableName.HOURLY;
import static com.yahoo.bard.webservice.data.config.names.TestDruidTableName.MONTHLY;

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
import com.yahoo.bard.webservice.data.volatility.DefaultingVolatileIntervalsService;
import com.yahoo.bard.webservice.data.volatility.NoVolatileIntervalsFunction;
import com.yahoo.bard.webservice.data.volatility.VolatileIntervalsFunction;
import com.yahoo.bard.webservice.data.volatility.VolatileIntervalsService;
import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.async.jobs.stores.ApiJobStore;
import com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField;
import com.yahoo.bard.webservice.async.preresponses.stores.PreResponseStore;
import com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobRowBuilder;
import com.yahoo.bard.webservice.async.jobs.jobrows.JobRowBuilder;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.metadata.QuerySigningService;
import com.yahoo.bard.webservice.metadata.SegmentIntervalsHashIdGenerator;
import com.yahoo.bard.webservice.models.druid.client.impl.TestDruidWebService;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;
import com.yahoo.bard.webservice.web.endpoints.JobsEndpointResources;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.time.Clock;
import java.util.Collections;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.UUID;

/**
 * Bard test app configuration binder.
 */
public class TestBinderFactory extends AbstractBinderFactory {

    public LinkedHashSet<DimensionConfig> dimensionConfig;
    public MetricLoader metricLoader;
    public TableLoader tableLoader;
    public ApplicationState state;

    public boolean afterBindingHookWasCalled = false;
    public boolean afterRegistrationHookWasCalled = false;

    public ConfigurationLoader configurationLoader;

    /**
     * Constructor.
     */
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

    /**
     * Constructor.
     *
     * @param dimensionConfig  Set of dimension configs to load
     * @param metricLoader  Loader for the test metrics
     * @param tableLoader  Loader for the test tables
     * @param state  Testing application state
     */
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

    /*
     * Returns a `VolatileIntervalsService` that provides some test volatility intervals.
     * <p>
     *  The service provides volatile intervals for only two tables:
     *  {@link com.yahoo.bard.webservice.data.config.names.TestDruidTableName#MONTHLY}, and
     *  {@link com.yahoo.bard.webservice.data.config.names.TestDruidTableName#HOURLY}. The HOURLY table is volatile
     *  from August 15 2016 to August 16 2016, while the MONTHLY table is volatile from August 1 2016 to
     *  September 1 2016.
     *
     * @return A VolatileIntervalsService that provides some test volatility for the HOURLY and MONTHLY tables
     */
    @Override
    protected VolatileIntervalsService getVolatileIntervalsService() {
        Map<PhysicalTable, VolatileIntervalsFunction> hourlyMonthlyVolatileIntervals = new LinkedHashMap<>();
        hourlyMonthlyVolatileIntervals.put(
                configurationLoader.getPhysicalTableDictionary().get(HOURLY.asName()),
                () -> new SimplifiedIntervalList(
                        Collections.singleton(
                                new Interval(new DateTime(2016, 8, 15, 0, 0), new DateTime(2016, 8, 16, 0, 0))
                        )
                )
        );
        hourlyMonthlyVolatileIntervals.put(
                configurationLoader.getPhysicalTableDictionary().get(MONTHLY.asName()),
                () -> new SimplifiedIntervalList(
                        Collections.singleton(
                                new Interval(new DateTime(2016, 8, 1, 0, 0), new DateTime(2016, 9, 1, 0, 0))
                        )
                )
        );
        return new DefaultingVolatileIntervalsService(
                NoVolatileIntervalsFunction.INSTANCE,
                hourlyMonthlyVolatileIntervals
        );
    }

    public ConfigurationLoader getConfigurationLoader() {
        return configurationLoader;
    }

    /**
     * Get the query signing service for the test.
     *
     * @return the query signing service
     */
    public QuerySigningService<?> getQuerySigningService() {
        return buildQuerySigningService(
                configurationLoader.getPhysicalTableDictionary(),
                getDataSourceMetaDataService()
        );
    }

    /**
     * Get the datasource metadata for the test.
     *
     * @return the datasource metadata service
     */
    public DataSourceMetadataService getDataSourceMetaDataService() {
        return buildDataSourceMetadataService();
    }

    @Override
    public ApiJobStore buildApiJobStore() {
        return JobsEndpointResources.getApiJobStore();
    }

    @Override
    public PreResponseStore buildPreResponseStore(ResourceDictionaries resourceDictionaries) {
        return JobsEndpointResources.getPreResponseStore();
    }

    @Override
    protected JobRowBuilder buildJobRowBuilder() {
        return new DefaultJobRowBuilder(
                jobMetadata -> jobMetadata.get(DefaultJobField.USER_ID) + UUID.randomUUID().toString(),
                ignored -> "greg",
                Clock.systemDefaultZone()
        );
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

    /**
     * Get the data cache that has been loaded, or build one if none has been loaded yet.
     *
     * @return the data cache being used or that will be used if needed
     */
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
