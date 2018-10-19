// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config.metric;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.yahoo.bard.webservice.data.config.metric.MetricInstance;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.util.Utils;
import com.yahoo.luthier.webservice.data.config.ExternalConfigLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Load the Wiki-specific metrics. Currently only loads primitive makers that are considered built-in to Fili,
 * such as the LongSumMaker for performing the longSum aggregation, and the divisionMaker, which performs division
 * of two other metrics.
 */
public class ExternalMetricsLoader implements MetricLoader {

    private static final Logger LOG = LoggerFactory.getLogger(MetricLoader.class);

    private static MetricMakerDictionary metricMakerDictionary;
    private ExternalConfigLoader metricConfigLoader;
    private String externalConfigFilePath;

    /**
     * Constructs a MetricLoader.
     *
     * @param metricConfigLoader The external configuration loader for loading metrics
     * @param externalConfigFilePath The external file's url containing the external config information
     */
    public ExternalMetricsLoader(ExternalConfigLoader metricConfigLoader, String externalConfigFilePath) {
        this.metricConfigLoader = metricConfigLoader;
        this.externalConfigFilePath = externalConfigFilePath;
    }

    @Override
    public void loadMetricDictionary(MetricDictionary metricDictionary, DimensionDictionary dimensionDictionary) {

        JodaModule jodaModule = bindTemplates();
        ObjectMapper objectMapper = new ObjectMapper().registerModule(jodaModule);

        ExternalMetricConfigTemplate metricConfig =
                metricConfigLoader.parseExternalFile(
                        externalConfigFilePath + "MetricConfig.json",
                        ExternalMetricConfigTemplate.class,
                        objectMapper
                );

        buildMetricMakersDictionary(metricConfig.getMakers(), metricDictionary, dimensionDictionary);

        List<MetricInstance> metrics = sortMetrics(metricConfig.getMetrics()).stream().map(
                metric -> new MetricInstance(
                        new LogicalMetricInfo(metric.getApiName(), metric.getLongName(), metric.getDescription()),
                        metricMakerDictionary.findByName(metric.getMakerName()),
                        metric.getDependencyMetricNames().toArray(new String[0])
                )
        ).collect(Collectors.toList());

        Utils.addToMetricDictionary(metricDictionary, metrics);
        LOG.debug("About to load direct aggregation metrics. Metric dictionary keys: {}", metricDictionary.keySet());
    }

    /**
     * (Re)Initialize the Metric Makers dictionary.
     *
     * @param metricMakers        The configuration of metric makers
     * @param metricDictionary    The dictionary that will be loaded with metrics
     * @param dimensionDictionary The dimension dictionary containing loaded dimensions
     */
    private void buildMetricMakersDictionary(Set<MetricMakerTemplate> metricMakers,
                                             MetricDictionary metricDictionary,
                                             DimensionDictionary dimensionDictionary) {
        metricMakerDictionary = new MetricMakerDictionary(metricMakers, metricDictionary, dimensionDictionary);
    }

    /**
     * Templates and deserializers binder.
     *
     * @return A Joda Module contains binding information.
     */
    private JodaModule bindTemplates() {
        JodaModule jodaModule = new JodaModule();
        jodaModule.addAbstractTypeMapping(
                ExternalMetricConfigTemplate.class,
                DefaultExternalMetricConfigTemplate.class
        );
        jodaModule.addAbstractTypeMapping(MetricMakerTemplate.class, DefaultMetricMakerTemplate.class);
        jodaModule.addAbstractTypeMapping(MetricTemplate.class, DefaultMetricTemplate.class);
        return jodaModule;
    }

    /**
     * Sort metrics by topological order.
     *
     * Metrics are topologically sorted iff a metric's dependencent metrics appear before it. So, suppose 
     * metric A depends on metrics B, C, and metric B depends on metric C. Then, the list [A, B, C] is NOT 
     * topologically sorted, but [C, B, A] is.
     *
     * @param metrics a list of unsorted metrics
     * @return a list of sorted metrics
     */
    private LinkedHashSet<MetricTemplate> sortMetrics(Set<MetricTemplate> metrics) {

        // map from metric's apiName to metric instance
        Map<String, MetricTemplate> map = new HashMap<>();

        // map from metric's apiName to metric's degree,
        // which means the number of other metrics this metric depends on
        Map<String, Integer> inDegree = new HashMap<>();

        // map from metric's apiName to a list of other metrics that depends on this metric
        Map<String, LinkedList<String>> dependencyMetrics = new HashMap<>();

        Queue<String> queue = new LinkedList<>();
        LinkedHashSet<MetricTemplate> result = new LinkedHashSet<>();

        metrics.forEach(metric -> {
                    map.put(metric.getApiName(), metric);
                    inDegree.put(metric.getApiName(), 0);
                });

        metrics.forEach(
                metric -> metric.getDependencyMetricNames().stream()
                        .filter(dependency ->
                                map.containsKey(dependency) &&
                                        !dependency.equals(metric.getApiName()))
                        .forEach(dependency -> {
                            dependencyMetrics.computeIfAbsent(dependency, d -> new LinkedList<>())
                                    .add(metric.getApiName());
                            inDegree.merge(metric.getApiName(), 1, Integer::sum);
                        })
        );

        inDegree.entrySet().stream().filter(m -> m.getValue() == 0)
                .forEach(m -> queue.offer(m.getKey()));

        while (!queue.isEmpty()) {
            String head = queue.poll();
            result.add(map.get(head));
            if (!dependencyMetrics.containsKey(head)) {
                continue;
            }
            dependencyMetrics.get(head).forEach(dependency -> {
                inDegree.put(dependency, inDegree.get(dependency) - 1);
                if (inDegree.get(dependency) == 0) {
                    queue.offer(dependency);
                }
            });
        }

        return result;
    }
}
