// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.provider

import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigProvider
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig
import com.yahoo.bard.webservice.data.config.provider.yaml.YamlConfigProvider
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.ConstantPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation
import spock.lang.Specification

public class ConfigBinderFactorySpec extends Specification {
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    private static final String CONF_TYPE = SYSTEM_CONFIG.getPackageVariableName("config_binder_type");

    public static class StubProvider implements ConfigProvider {
        @Override
        ConfigurationDictionary<PhysicalTableConfiguration> getPhysicalTableConfig() {
            return new ConfigurationDictionary<PhysicalTableConfiguration>()
        }

        @Override
        ConfigurationDictionary<LogicalTableConfiguration> getLogicalTableConfig() {
            return new ConfigurationDictionary<LogicalTableConfiguration>()
        }

        @Override
        ConfigurationDictionary<MakerConfiguration> getCustomMakerConfig() {
            return new ConfigurationDictionary<MakerConfiguration>()
        }

        @Override
        ConfigurationDictionary<DimensionConfig> getDimensionConfig() {
            return new ConfigurationDictionary<DimensionConfig>()
        }

        @Override
        ConfigurationDictionary<MetricConfiguration> getBaseMetrics() {
            return new ConfigurationDictionary<MetricConfiguration>()
        }

        @Override
        ConfigurationDictionary<MetricConfiguration> getDerivedMetrics() {
            return new ConfigurationDictionary<MetricConfiguration>()
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

    def "We should be able to load a YAML configuration"() {

        setup:
        SYSTEM_CONFIG.setProperty(CONF_TYPE, YamlConfigProvider.class.getName())
        String path = ConfigBinderFactory.class.getResource("/yaml/exampleConfiguration.yaml").getPath()
        SYSTEM_CONFIG.setProperty(SYSTEM_CONFIG.getPackageVariableName(YamlConfigProvider.CONF_YAML_PATH), path)
        when:
        ConfigBinderFactory factory = new ConfigBinderFactory()
        def loader = factory.buildConfigurationLoader()
        loader.load()

        def expectedAgg = new LongSumAggregation("impressions", "impressions")

        then:

        loader.metricDictionary.containsKey("impressions")
        def q = loader.metricDictionary.get("incremented_impressions").getTemplateDruidQuery()

        FuzzyQueryMatcher.matches(
                loader.metricDictionary.get("impressions").getTemplateDruidQuery(),
                new TemplateDruidQuery([expectedAgg], [])
        );

        FuzzyQueryMatcher.matches(q,
                new TemplateDruidQuery(
                        [expectedAgg],
                        [new ArithmeticPostAggregation(
                                "incremented_impressions",
                                ArithmeticPostAggregation.ArithmeticPostAggregationFunction.PLUS,
                                [new FieldAccessorPostAggregation(expectedAgg), new ConstantPostAggregation("some_name", 1.0)]
                        )]));
    }

}
