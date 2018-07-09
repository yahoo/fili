// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.metric;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;

/**
 * Metric Config template.
 * <p>
 * An example:
 *
 *       {
 *          "makers" :
 *              [
 *                  a list of metrics makers deserialize by MetricMakerTemplate
 *              ]
 *          "metrics" :
 *              [
 *                  a list of metrics deserialize by MetricTemplate
 *              ]
 *       }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MetricConfigTemplate {

    private final LinkedHashSet<MetricMakerTemplate> makers;
    private final LinkedHashSet<MetricTemplate> metrics;

    /**
     * Constructor used by json parser.
     *
     * @param makers  json property makers
     * @param metrics json property metrics
     */
    public MetricConfigTemplate(
            @JsonProperty("makers") LinkedHashSet<MetricMakerTemplate> makers,
            @JsonProperty("metrics") LinkedHashSet<MetricTemplate> metrics
    ) {
        this.makers = (Objects.isNull(makers) ? new LinkedHashSet<>() : new LinkedHashSet<>(makers));
        this.metrics = (Objects.isNull(metrics) ? new LinkedHashSet<>() : sort(metrics));
    }

    /**
     * Get metrics makers configuration info.
     *
     * @return a list of metrics makers
     */
    public LinkedHashSet<MetricMakerTemplate> getMakers() {
        return this.makers;
    }

    /**
     * Get metrics configuration info.
     *
     * @return a list of metrics
     */
    public LinkedHashSet<MetricTemplate> getMetrics() {
        return this.metrics;
    }

    /**
     * Sort metrics by topological order,
     * make sure a metric's dependency metrics order before this metric.
     *
     * @param metrics a list of unsorted metrics
     * @return a list of sorted metrics
     */
    private LinkedHashSet<MetricTemplate> sort(LinkedHashSet<MetricTemplate> metrics) {

        // map from metric's apiName to metric instance
        Map<String, MetricTemplate> map = new HashMap<>();

        // map from metric's apiName to metric's degree,
        // which means the number of other metrics this metric depends on
        Map<String, Integer> inDegree = new HashMap<>();

        // map from metric's apiName to a list of other metrics that depends on this metric
        Map<String, LinkedList<String>> dependencyMetrics = new HashMap<>();

        Queue<String> queue = new LinkedList<>();
        LinkedHashSet<MetricTemplate> result = new LinkedHashSet<>();

        metrics.stream()
                .forEach(metric -> {
                    map.put(metric.getApiName(), metric);
                    inDegree.put(metric.getApiName(), 0);
                });

        metrics.stream().forEach(
                metric -> metric.getDependencyMetricNames().stream()
                        .filter(dependency ->
                                map.containsKey(dependency.toLowerCase(Locale.ENGLISH)) &&
                                !dependency.toLowerCase(Locale.ENGLISH).equals(metric.getApiName()))
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
            for (String dependency : dependencyMetrics.get(head)) {
                inDegree.put(dependency, inDegree.get(dependency) - 1);
                if (inDegree.get(dependency) == 0) {
                    queue.offer(dependency);
                }
            }
        }

        return result;
    }
}
