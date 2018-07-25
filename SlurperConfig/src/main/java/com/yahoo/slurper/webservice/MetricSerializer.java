// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.slurper.webservice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.slurper.webservice.data.config.MetricObject;
import com.yahoo.slurper.webservice.data.config.auto.DataSourceConfiguration;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Serializer metric into json file.
 */
public class MetricSerializer extends ExternalConfigSerializer {

    private Map<String, Set<? extends Object>> config = new HashMap<>();
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
                    config.put("metrics", metricObjects);
                });

        Map<String, String> doubleSumMaker = new HashMap<>();
        doubleSumMaker.put("name", "doubleSum");
        doubleSumMaker.put("class", "com.yahoo.bard.webservice.data.config.metric.makers.DoubleSumMaker");

        Set<Map<String, String>> makers = new HashSet<>();
        makers.add(doubleSumMaker);

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
        this.path = path;
        return this;
    }

    /**
     * Parse config to json file.
     *
     */
    public void parseToJson() {
        super.parse(config, path);
    }
}
