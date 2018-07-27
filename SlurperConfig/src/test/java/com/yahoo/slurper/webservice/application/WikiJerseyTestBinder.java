// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.slurper.webservice.application;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.bard.webservice.application.JerseyTestBinder;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.table.TableLoader;
import com.yahoo.bard.webservice.metadata.TestDataSourceMetadataService;
import com.yahoo.luthier.webservice.data.config.ExternalConfigLoader;
import com.yahoo.luthier.webservice.data.config.dimension.ExternalDimensionsLoader;
import com.yahoo.luthier.webservice.data.config.metric.ExternalMetricsLoader;
import com.yahoo.luthier.webservice.data.config.table.ExternalTableLoader;
import com.yahoo.slurper.webservice.data.config.dimension.DimensionSerializer;
import com.yahoo.slurper.webservice.data.config.metric.MetricSerializer;
import com.yahoo.slurper.webservice.data.config.table.TableSerializer;
import com.yahoo.slurper.webservice.data.config.auto.DataSourceConfiguration;
import com.yahoo.slurper.webservice.data.config.auto.StaticWikiConfigLoader;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.Supplier;

/**
 * TestBinder with Wiki configuration specialization.
 */
public class WikiJerseyTestBinder extends JerseyTestBinder {

    private static Supplier<List<? extends DataSourceConfiguration>> configLoader = new StaticWikiConfigLoader();

    private static final String DRUID_CONFIG_FILE_PATH  = "src/test/resources/";

    private static ExternalConfigLoader externalConfigLoader = new ExternalConfigLoader();
    private ExternalDimensionsLoader externalDimensionsLoader;
    private DimensionSerializer dimensionSerializer;
    private MetricSerializer metricSerializer;

    /**
     * Constructor.
     *
     * @param resourceClasses  Resource classes to load into the application.
     */
    public WikiJerseyTestBinder(java.lang.Class<?>... resourceClasses) {
        this(true, resourceClasses);
    }

    /**
     * Constructor.
     *
     * @param doStart  Whether or not to start the application
     * @param resourceClasses  Resource classes to load into the application.
     */
    public WikiJerseyTestBinder(boolean doStart, java.lang.Class<?>... resourceClasses) {
        super(doStart, resourceClasses);
    }

    @Override
    public LinkedHashSet<DimensionConfig> getDimensionConfiguration() {

        dimensionSerializer = new DimensionSerializer(new ObjectMapper());

        dimensionSerializer
                .setConfig(configLoader)
                .setPath(DRUID_CONFIG_FILE_PATH)
                .parseToJson();

        externalDimensionsLoader = new ExternalDimensionsLoader(
                externalConfigLoader,
                DRUID_CONFIG_FILE_PATH
        );

        return new LinkedHashSet<>(externalDimensionsLoader.getAllDimensionConfigurations());
    }

    @Override
    public MetricLoader getMetricLoader() {

        metricSerializer = new MetricSerializer(new ObjectMapper());

        metricSerializer
                .setConfig(configLoader)
                .setPath(DRUID_CONFIG_FILE_PATH)
                .parseToJson();

        return new ExternalMetricsLoader(
                externalConfigLoader,
                DRUID_CONFIG_FILE_PATH
        );
    }

    @Override
    public TableLoader getTableLoader() {

        TableSerializer tableSerializer = new TableSerializer(new ObjectMapper());

        tableSerializer
                .setDimensions(dimensionSerializer)
                .setMetrics(metricSerializer)
                .setConfig(configLoader)
                .setPath(DRUID_CONFIG_FILE_PATH)
                .parseToJson();

        return new ExternalTableLoader(
                new TestDataSourceMetadataService(),
                externalDimensionsLoader,
                externalConfigLoader,
                DRUID_CONFIG_FILE_PATH
        );

    }
}
