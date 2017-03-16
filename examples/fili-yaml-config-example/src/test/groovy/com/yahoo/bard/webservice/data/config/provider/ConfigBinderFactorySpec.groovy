// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.provider

import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigProvider
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig
import com.yahoo.bard.webservice.data.config.provider.descriptor.DimensionFieldDescriptor
import com.yahoo.bard.webservice.data.config.provider.descriptor.LogicalTableDescriptor
import com.yahoo.bard.webservice.data.config.provider.descriptor.MakerDescriptor
import com.yahoo.bard.webservice.data.config.provider.descriptor.MetricDescriptor
import com.yahoo.bard.webservice.data.config.provider.descriptor.PhysicalTableDescriptor
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.MetricDictionary

import spock.lang.Specification

public class ConfigBinderFactorySpec extends Specification {
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    private static final String CONF_TYPE = SYSTEM_CONFIG.getPackageVariableName("config_binder_type");

    public static class StubProvider implements ConfigProvider {
        @Override
        List<PhysicalTableDescriptor> getPhysicalTableConfig() {
            return new LinkedList<>()
        }

        @Override
        List<LogicalTableDescriptor> getLogicalTableConfig() {
            return new LinkedList<>()
        }

        @Override
        List<MakerDescriptor> getCustomMakerConfig() {
            return new LinkedList<>()
        }

        @Override
        List<DimensionConfig> getDimensionConfig() {
            return new LinkedList<>()
        }

        @Override
        List<DimensionFieldDescriptor> getDimensionFieldConfig() {
            return new LinkedList<>()
        }

        @Override
        List<MetricDescriptor> getMetricConfig() {
            return new LinkedList<>()
        }

        @Override
        LogicalMetricBuilder getLogicalMetricBuilder(
                final MetricDictionary metricDictionary,
                final MakerBuilder makerBuilder,
                final DimensionDictionary dimensionDictionary
        ) {
            return new LogicalMetricBuilder(dimensionDictionary, makerBuilder, metricDictionary) {
                @Override
                LogicalMetric buildMetric(final MetricDescriptor metric) {
                    return new LogicalMetric(null,  null, "example metric")
                }
            }
        }

        /**
         *
         * @return
         */
        public static ConfigProvider build(SystemConfig systemConfig) {
            return new StubProvider();
        }
    }

    def "test that class can be instantiated"() {

        setup:
        SYSTEM_CONFIG.setProperty(CONF_TYPE, StubProvider.class.getName())
        // This should instantiate the above stub class
        def factory = new ConfigBinderFactory()
        def loader = factory.buildConfigurationLoader()
        loader.load()

        expect:
        factory.provider instanceof StubProvider
    }

    def "Exception should be thrown when no class exists"() {

        setup:
        SYSTEM_CONFIG.setProperty(CONF_TYPE, "this.is.not.a.class")
        // This should instantiate the above stub class
        when:
        new ConfigBinderFactory()

        then:
        ConfigurationError ex = thrown()
        ex.message =~ /.*Unable to construct config provider.*/
        ex.cause instanceof ClassNotFoundException
    }
}
