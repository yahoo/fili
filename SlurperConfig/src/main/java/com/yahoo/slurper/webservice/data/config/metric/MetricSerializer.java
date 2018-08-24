// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.slurper.webservice.data.config.metric;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.slurper.webservice.ExternalConfigSerializer;
import com.yahoo.slurper.webservice.data.config.JsonObject;
import com.yahoo.slurper.webservice.data.config.auto.DataSourceConfiguration;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Serializer metric into json file.
 */
public class MetricSerializer extends ExternalConfigSerializer {

    private Map<String, Set<JsonObject>> config = new HashMap<>();
    private Map<String, Set<String>> tableToMetricNames = new HashMap<>();
    private String path;

    /**
     * Constructor.
     *
     * @param mapper object mapper for serialization
     */
    public MetricSerializer(ObjectMapper mapper) {
        super(mapper);
    }

    /**
     * Constructor.
     *
     * @param configLoader Supplies DataSourceConfigurations to build the metric from.
     *
     * @return MetricSerializer
     */
    public MetricSerializer setConfig(
            Supplier<List<? extends DataSourceConfiguration>> configLoader
    ) {

        Set<JsonObject> metrics = new HashSet<>();
        Set<JsonObject> makers = new HashSet<>();

        configLoader.get()
                .forEach(dataSourceConfiguration -> {
                    Set<MetricObject> metricObjects = dataSourceConfiguration.getMetrics().stream()
                            .map(metric -> new MetricObject(
                                    metric,
                                    metric,
                                    "doubleSum",
                                    Stream.of(metric).collect(Collectors.toList())
                            )).collect(
                            Collectors.collectingAndThen(
                                    Collectors.toSet(),
                                    Collections::unmodifiableSet
                            ));

                    metrics.addAll(metricObjects);

                    tableToMetricNames.put(
                            dataSourceConfiguration.getTableName().asName(),
                            metricObjects.stream()
                                    .map(MetricObject::getApiName)
                                    .collect(Collectors.toSet())
                    );
                });

        MakerObject doubleSum = new MakerObject(
                "doubleSum",
                "com.yahoo.bard.webservice.data.config.metric.makers.DoubleSumMaker"
        );

        makers.add(doubleSum);

        config.put("metrics", metrics);
        config.put("makers", makers);

        return this;
    }

    /**
     * Set json file path.
     *
     * @param path json file path
     *
     * @return MetricSerializer
     */
    public MetricSerializer setPath(String path) {
        this.path = path + "MetricConfig.json";
        return this;
    }

    /**
     * Returns all metric configurations of a particular data source.
     *
     * @param dataSourceName  Name of the data source
     *
     * @return all metric names of the particular data source
     */
    public Set<String> getMetrics(String dataSourceName) {
        return tableToMetricNames.getOrDefault(dataSourceName, Collections.emptySet());
    }

    /**
     * Parse config to json file.
     *
     */
    public void parseToJson() {
        super.parse(config, path);
    }
}
