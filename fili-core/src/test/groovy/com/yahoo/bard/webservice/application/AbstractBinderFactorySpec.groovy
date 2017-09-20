// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application

import static com.yahoo.bard.webservice.application.AbstractBinderFactory.HEALTH_CHECK_NAME_DATASOURCE_METADATA
import static com.yahoo.bard.webservice.application.AbstractBinderFactory.HEALTH_CHECK_NAME_DIMENSION
import static com.yahoo.bard.webservice.application.AbstractBinderFactory.HEALTH_CHECK_NAME_DRUID_DIM_LOADER
import static com.yahoo.bard.webservice.application.AbstractBinderFactory.HEALTH_CHECK_VERSION
import static com.yahoo.bard.webservice.config.BardFeatureFlag.DRUID_CACHE
import static com.yahoo.bard.webservice.config.BardFeatureFlag.DRUID_CACHE_V2
import static com.yahoo.bard.webservice.config.BardFeatureFlag.DRUID_COORDINATOR_METADATA
import static com.yahoo.bard.webservice.config.BardFeatureFlag.PARTIAL_DATA
import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.COLOR
import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.SHAPE
import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.SIZE
import static com.yahoo.bard.webservice.druid.client.DruidClientConfigHelper.DRUID_COORD_URL_KEY
import static com.yahoo.bard.webservice.druid.client.DruidClientConfigHelper.NON_UI_DRUID_BROKER_URL_KEY
import static com.yahoo.bard.webservice.druid.client.DruidClientConfigHelper.UI_DRUID_BROKER_URL_KEY

import com.yahoo.bard.webservice.application.healthchecks.AllDimensionsLoadedHealthCheck
import com.yahoo.bard.webservice.application.healthchecks.DataSourceMetadataLoaderHealthCheck
import com.yahoo.bard.webservice.application.healthchecks.DruidDimensionsLoaderHealthCheck
import com.yahoo.bard.webservice.application.healthchecks.VersionHealthCheck
import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigException
import com.yahoo.bard.webservice.config.SystemConfigProvider
import com.yahoo.bard.webservice.data.cache.DataCache
import com.yahoo.bard.webservice.data.cache.HashDataCache
import com.yahoo.bard.webservice.data.cache.StubDataCache
import com.yahoo.bard.webservice.data.cache.TupleDataCache
import com.yahoo.bard.webservice.data.config.ConfigurationLoader
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig
import com.yahoo.bard.webservice.data.config.dimension.TestDimensions
import com.yahoo.bard.webservice.data.config.dimension.TypeAwareDimensionLoader
import com.yahoo.bard.webservice.data.config.metric.MetricLoader
import com.yahoo.bard.webservice.data.config.table.TableLoader
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.druid.client.DruidWebService
import com.yahoo.bard.webservice.metadata.DataSourceMetadataLoadTask
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService
import com.yahoo.bard.webservice.table.LogicalTableDictionary
import com.yahoo.bard.webservice.table.PhysicalTableDictionary

import com.codahale.metrics.health.HealthCheck
import com.codahale.metrics.health.HealthCheckRegistry
import com.fasterxml.jackson.databind.ObjectMapper

import org.glassfish.hk2.api.DynamicConfiguration
import org.glassfish.hk2.utilities.Binder

import spock.lang.IgnoreIf
import spock.lang.Shared
import spock.lang.Specification

public class AbstractBinderFactorySpec extends Specification {

    private static final ObjectMapper MAPPER = new ObjectMappersSuite().getMapper()

    final SystemConfig systemConfig = SystemConfigProvider.getInstance()

    @Shared
    HealthCheckRegistry registry

    @Shared
    TestBinderFactory binderFactory

    protected final String DIMENSION_BACKEND_KEY = systemConfig.getPackageVariableName("dimension_backend");

    @Shared boolean partialDataStatus = PARTIAL_DATA.isOn()
    @Shared boolean cacheStatus = DRUID_CACHE.isOn()
    @Shared boolean cacheV2Status = DRUID_CACHE_V2.isOn()
    @Shared boolean coordinatorStatus = DRUID_COORDINATOR_METADATA.isOn()

    String oldUiURL
    String oldNonUiURL
    String oldCoordURL

    def setupSpec() {
        registry = HealthCheckRegistryFactory.registry
    }

    /**
     * Turn-off the cache, save the URLs of druid nodes, set their values for this test and create a binder factory.
     */
    def setup() {
        DRUID_CACHE.setOn(false)
        DRUID_COORDINATOR_METADATA.setOn(true)
        try {
            oldUiURL = systemConfig.getStringProperty(UI_DRUID_BROKER_URL_KEY)
        } catch (SystemConfigException e) {
            oldUiURL = null
        }
        systemConfig.setProperty(UI_DRUID_BROKER_URL_KEY, "http://localhost:9998/uidruid")

        try {
            oldNonUiURL = systemConfig.getStringProperty(NON_UI_DRUID_BROKER_URL_KEY)
        } catch (SystemConfigException e) {
            oldNonUiURL = null
        }
        systemConfig.setProperty(NON_UI_DRUID_BROKER_URL_KEY, "http://localhost:9998/nonuidruid")

        try {
            oldCoordURL = systemConfig.getStringProperty(DRUID_COORD_URL_KEY)
        } catch (SystemConfigException e) {
            oldCoordURL = null
        }
        systemConfig.setProperty(DRUID_COORD_URL_KEY, "http://localhost:9998/coordinator")

        binderFactory = new TestBinderFactory()
    }

    def propertyRestore(propertyName, propertyValue) {
        if (propertyValue) {
            systemConfig.setProperty(propertyName, propertyValue)
        } else {
            systemConfig.clearProperty(propertyName)
        }
    }

    def cleanup() {
        systemConfig.clearProperty(DIMENSION_BACKEND_KEY)
        PARTIAL_DATA.setOn(partialDataStatus)
        DRUID_COORDINATOR_METADATA.setOn(coordinatorStatus)
        DRUID_CACHE.setOn(cacheStatus)
        DRUID_CACHE_V2.setOn(cacheV2Status)
        propertyRestore(UI_DRUID_BROKER_URL_KEY, oldUiURL)
        propertyRestore(NON_UI_DRUID_BROKER_URL_KEY, oldNonUiURL)
        propertyRestore(DRUID_COORD_URL_KEY, oldCoordURL)
        binderFactory.shutdownLoaderScheduler()
        HealthCheckRegistryFactory.registry = registry
    }

    def "test setupHealthChecks"() {
        setup:
        HealthCheckRegistry reg = Mock(HealthCheckRegistry)
        HealthCheckRegistryFactory.registry = reg
        1 * reg.register(HEALTH_CHECK_NAME_DIMENSION, _ as HealthCheck)
        1 * reg.register(HEALTH_CHECK_VERSION, _ as VersionHealthCheck)
        Binder binder = binderFactory.buildBinder()
        DynamicConfiguration dc = Mock(DynamicConfiguration)

        when:
        binder.bind(dc)

        then:
        true
    }

    @IgnoreIf({
        SystemConfigProvider.getInstance().
                getStringProperty(
                        SystemConfigProvider.getInstance().getPackageVariableName("dimension_backend"),
                        "memory"
                ) == "memory"
    })
    def "test config builder"() {
        when:
        systemConfig.setProperty(DIMENSION_BACKEND_KEY, "redis")
        ConfigurationLoader cl = binderFactory.getConfigurationLoader()

        then:
        cl.dictionaries != null
        cl.tableLoader != null
        cl.metricLoader != null
        cl.dimensionLoader instanceof TypeAwareDimensionLoader
    }

    def "test configure bindings"() {
        setup:
        DynamicConfiguration dc = Mock(DynamicConfiguration)

        when:
        systemConfig.setProperty(DIMENSION_BACKEND_KEY, "memory")
        Binder binder = binderFactory.buildBinder()
        binder.bind(dc)

        then:
        binder != null
        2 * dc.bind({ it.advertisedContracts.contains("com.yahoo.bard.webservice.druid.client.DruidWebService") }, _)
        1 * dc.bind({ it.implementation.contains(PhysicalTableDictionary.canonicalName) }, _)
        1 * dc.bind({ it.implementation.contains(LogicalTableDictionary.canonicalName) }, _)
        1 * dc.bind({ it.implementation.contains(MetricDictionary.canonicalName) }, _)
        1 * dc.bind({ it.implementation.contains(DimensionDictionary.canonicalName) }, _)
    }

    def "BindAtEnd is called when binding"() {
        given: "An AbstractBinder"
        DynamicConfiguration dc = Mock(DynamicConfiguration)

        when: "Bind is called"
        binderFactory.buildBinder().bind(dc)

        then: "The afterBinding hook was called"
        binderFactory.afterBindingHookWasCalled
    }

    def "test health checks config with partial data on"() {
        setup:
        PARTIAL_DATA.setOn(true)

        // Clear up the health check registry
        HealthCheckRegistry reg = HealthCheckRegistryFactory.getRegistry()
        def names = reg.getNames()
        names.each {
            reg.unregister(it)
        }

        when:
        DynamicConfiguration dc = Mock(DynamicConfiguration)
        Binder binder = binderFactory.buildBinder()
        binder.bind(dc)

        then:
        reg.getNames().contains(HEALTH_CHECK_VERSION)
        reg.getNames().contains(HEALTH_CHECK_NAME_DIMENSION)
    }

    def "test keyValueStore health check"() {
        setup:

        // Clear up the health check registry
        HealthCheckRegistry reg = HealthCheckRegistryFactory.getRegistry()
        def names = reg.getNames()
        names.each {
            reg.unregister(it)
        }

        when:
        DynamicConfiguration dc = Mock(DynamicConfiguration)
        Binder binder = binderFactory.buildBinder()
        binder.bind(dc)

        LinkedHashSet<DimensionConfig> dimensionConfigs = binderFactory.getDimensionConfigurations()

        then:
        for (DimensionConfig dimensionConfig : dimensionConfigs) {
            reg.getNames().contains(dimensionConfig.getApiName() + " keyValueStore check")
        }
    }

    def "test searchProvider health check"() {
        setup:

        // Clear up the health check registry
        HealthCheckRegistry reg = HealthCheckRegistryFactory.getRegistry()
        def names = reg.getNames()
        names.each {
            reg.unregister(it)
        }

        when:
        DynamicConfiguration dc = Mock(DynamicConfiguration)
        Binder binder = binderFactory.buildBinder()
        binder.bind(dc)

        LinkedHashSet<DimensionConfig> dimensionConfigs = binderFactory.getDimensionConfigurations()

        then:
        for (DimensionConfig dimensionConfig : dimensionConfigs) {
            reg.getNames().contains(dimensionConfig.getApiName() + " searchProvider check")
        }
    }

    def "Test setup health checks"() {
        setup:
        AllDimensionsLoadedHealthCheck capturedHealthCheck = null
        DimensionDictionary dimensionDictionary = new DimensionDictionary()
        HealthCheckRegistry registry = Mock(HealthCheckRegistry)

        when:
        binderFactory.setupHealthChecks(registry, dimensionDictionary)

        then:
        1 * registry.register(HEALTH_CHECK_NAME_DIMENSION, _) >> {
            string, healthCheck -> capturedHealthCheck = healthCheck
        }
        1 * registry.register(HEALTH_CHECK_VERSION, _ as VersionHealthCheck)
        capturedHealthCheck.dimensionDictionary.is(dimensionDictionary)
    }

    def "Test Setup Druid Dimensions Loader"() {
        setup:
        DruidDimensionsLoaderHealthCheck capturedHealthCheck = null
        PhysicalTableDictionary physicalTableDictionary = new PhysicalTableDictionary()
        DimensionDictionary dimensionDictionary = new DimensionDictionary()

        DruidWebService webService = Mock(DruidWebService)
        HealthCheckRegistry registry = Mock(HealthCheckRegistry)

        when:
        DimensionValueLoadTask druidDimensionsLoader = binderFactory.buildDruidDimensionsLoader(
                webService,
                physicalTableDictionary,
                dimensionDictionary
        )
        binderFactory.setupDruidDimensionsLoader(registry, druidDimensionsLoader)
        binderFactory.shutdownLoaderScheduler()

        then:
        DruidDimensionValueLoader druidDimensionRowProvider = druidDimensionsLoader.dimensionRowProviders.getAt(0)
        druidDimensionRowProvider.druidWebService.is(webService)
        1 * registry.register(HEALTH_CHECK_NAME_DRUID_DIM_LOADER, _ as DruidDimensionsLoaderHealthCheck) >> {
            string, healthCheck -> capturedHealthCheck = healthCheck
        }
        capturedHealthCheck?.loader?.is(druidDimensionsLoader)
    }

    def "Test Setup Datasource Metadata"() {
        setup:
        DataSourceMetadataLoaderHealthCheck capturedHealthCheck = null
        PhysicalTableDictionary physicalTableDictionary = new PhysicalTableDictionary()
        DataSourceMetadataService metadataService = new DataSourceMetadataService()

        DruidWebService webService = Mock(DruidWebService)
        HealthCheckRegistry registry = Mock(HealthCheckRegistry)

        when:
        DataSourceMetadataLoadTask dataSourceMetadataLoader = binderFactory.buildDataSourceMetadataLoader(
                webService,
                physicalTableDictionary,
                metadataService,
                MAPPER
        )
        binderFactory.setupDataSourceMetaData(registry, dataSourceMetadataLoader)
        binderFactory.shutdownLoaderScheduler()

        then:
        dataSourceMetadataLoader.druidWebService.is(webService)
        dataSourceMetadataLoader.physicalTableDictionary.is(physicalTableDictionary)
        1 * registry.register(HEALTH_CHECK_NAME_DATASOURCE_METADATA, _ as DataSourceMetadataLoaderHealthCheck) >> {
            string, healthCheck -> capturedHealthCheck = healthCheck
        }
        capturedHealthCheck.loader.is(dataSourceMetadataLoader)
    }

    def "Test build cache"() {
        when:
        DRUID_CACHE.setOn(true)
        DRUID_CACHE_V2.setOn(false)
        DataCache test = binderFactory.buildCache()

        then:
        test instanceof HashDataCache

        when:
        // Since this test re-instantiates the global binderFactory, it needs to shutdown the previous scheduler
        binderFactory.shutdownLoaderScheduler()
        binderFactory = new TestBinderFactory()
        DRUID_CACHE.setOn(true)
        DRUID_CACHE_V2.setOn(true)
        test = binderFactory.buildCache()

        then:
        test instanceof TupleDataCache

        when:
        binderFactory.shutdownLoaderScheduler()
        binderFactory = new TestBinderFactory()
        DRUID_CACHE.setOn(false)
        DRUID_CACHE_V2.setOn(false)
        test = binderFactory.buildCache()

        then:
        test instanceof StubDataCache
        // for coverage
        test.clear()
        test.get("key") == null
        !test.set("key", "value")
    }

    def "Build configuration loader"() {
        given: "Mocks for the load classes"

        LinkedHashSet<DimensionConfig> dimensions = new TestDimensions().getDimensionConfigurationsByApiName(
                SIZE,
                COLOR,
                SHAPE
        )

        MetricLoader metricLoader = Mock(MetricLoader)
        TableLoader tableLoader = Mock(TableLoader)

        def localBinderFactory = new AbstractBinderFactory() {
            LinkedHashSet<DimensionConfig> getDimensionConfigurations() {
                return dimensions
            }

            MetricLoader getMetricLoader() {
                return metricLoader
            }

            TableLoader getTableLoader() {
                return tableLoader
            }
        }

        when:
        systemConfig.setProperty(DIMENSION_BACKEND_KEY, "memory")
        ConfigurationLoader cl = localBinderFactory.getConfigurationLoader()

        then:
        cl.dimensionLoader instanceof TypeAwareDimensionLoader
        cl.metricLoader == metricLoader
        cl.tableLoader == tableLoader
    }
}
