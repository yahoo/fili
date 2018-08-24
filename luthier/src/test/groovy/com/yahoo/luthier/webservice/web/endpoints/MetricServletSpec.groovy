// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.web.endpoints

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.joda.JodaModule
import com.yahoo.luthier.webservice.data.config.ExternalConfigLoader
import com.yahoo.luthier.webservice.data.config.metric.DefaultExternalMetricConfigTemplate
import com.yahoo.luthier.webservice.data.config.metric.ExternalMetricConfigTemplate
import com.yahoo.luthier.webservice.data.config.metric.ExternalMetricsLoader
import com.yahoo.luthier.webservice.data.config.metric.MetricMakerDictionary
import com.yahoo.luthier.webservice.data.config.metric.MetricTemplate
import org.springframework.core.DefaultParameterNameDiscoverer
import spock.lang.Specification
import spock.lang.Unroll

import java.lang.reflect.Constructor
import java.util.function.BiFunction

class MetricServletSpec extends Specification{

    ExternalMetricConfigTemplate metricConfig
    ExternalMetricsLoader externalMetricsLoader
    DefaultParameterNameDiscoverer discoverer

    def setup() {

        final EXTERNAL_CONFIG_FILE_PATH  = "src/test/resources/"
        final ExternalConfigLoader metricConfigLoader = new ExternalConfigLoader()

        discoverer = new DefaultParameterNameDiscoverer()

        externalMetricsLoader = new ExternalMetricsLoader(
                metricConfigLoader,
                EXTERNAL_CONFIG_FILE_PATH
        )

        JodaModule jodaModule = externalMetricsLoader.bindTemplates()
        ObjectMapper objectMapper = new ObjectMapper().registerModule(jodaModule)

        metricConfig =
                metricConfigLoader.parseExternalFile(EXTERNAL_CONFIG_FILE_PATH + "MetricTester.json",
                        DefaultExternalMetricConfigTemplate.class, objectMapper
                )
    }

    @Unroll
    def "test sortMetrics method in ExternalMetricLoader" () {
        setup: "load metrics"
        LinkedHashSet<MetricTemplate> metrics = externalMetricsLoader
                    .sortMetrics(metricConfig.getMetrics())

        expect: "a metric's dependency metrics order before this metric"
        Set<String> metricSet = new HashSet<>()
        for (metric in metrics) {
            metricSet.add (metric.getApiName())
            for (dependency in metric.getDependencyMetricNames()) {
                assert metricSet.contains(dependency)
            }
        }
    }

    @Unroll
    def "test findConstructor method in MetricMakerDictionary" () {
        setup: "load metrics"
        MetricMakerDictionary metricMakerDictionary = new MetricMakerDictionary()

        expect: "constructor's parameters matchs maker's parameters"
        for (maker in metricConfig.getMakers()) {
            Map<Class, BiFunction<Class, Object, ?>> paramMapper = metricMakerDictionary.buildParamMappers()
            Constructor<?> constructor = metricMakerDictionary.findConstructor(maker, discoverer)
            Class<?>[] pTypes = constructor.getParameterTypes()
            String[] pNames = discoverer.getParameterNames(constructor)
            int index = 0
            for (int i = 0; i < pTypes.length; i++) {
                if ("MetricDictionary" == pTypes[i].getSimpleName()
                        || "DimensionDictionary" == pTypes[i].getSimpleName()) {
                    continue
                }
                assert maker.getParams().containsKey(pNames[i])
                index++
                if (!pTypes[i].isPrimitive()) {
                    Object param = metricMakerDictionary.parseParams(pTypes[i], pNames[i], maker, paramMapper)
                    assert pTypes[i].isInstance(param)
                }
            }
            assert index == maker.getParams().size()
        }
    }
}
