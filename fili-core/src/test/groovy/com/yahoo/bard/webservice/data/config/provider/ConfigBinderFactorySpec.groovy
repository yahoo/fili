// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.provider

import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigProvider
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig
import spock.lang.Specification

public class ConfigBinderFactorySpec extends Specification {
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    private static final String CONF_TYPE = SYSTEM_CONFIG.getPackageVariableName("config_binder_type");

    public static class StubProvider implements ConfigProvider {
        @Override
        List<PhysicalTableConfiguration> getPhysicalTableConfig() {
            return new LinkedList<PhysicalTableConfiguration>()
        }

        @Override
        List<LogicalTableConfiguration> getLogicalTableConfig() {
            return new LinkedList<LogicalTableConfiguration>()
        }

        @Override
        List<MakerConfiguration> getCustomMakerConfig() {
            return new LinkedList<MakerConfiguration>()
        }

        @Override
        List<DimensionConfig> getDimensionConfig() {
            return new LinkedList<DimensionConfig>()
        }

        @Override
        List<MetricConfiguration> getMetricConfig() {
            return new LinkedList<MetricConfiguration>()
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
