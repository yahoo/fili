package com.yahoo.slurper.webservice.application;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.bard.webservice.application.JerseyTestBinder;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.table.TableLoader;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.luthier.webservice.data.config.ExternalConfigLoader;
import com.yahoo.luthier.webservice.data.config.dimension.ExternalDimensionsLoader;
import com.yahoo.luthier.webservice.data.config.table.ExternalTableLoader;
import com.yahoo.slurper.webservice.DimensionSerializer;
import com.yahoo.slurper.webservice.data.config.auto.DataSourceConfiguration;
import com.yahoo.slurper.webservice.data.config.auto.StaticWikiConfigLoader;
import com.yahoo.slurper.webservice.data.config.dimension.GenericDimensionConfigs;
import com.yahoo.slurper.webservice.data.config.metric.GenericMetricLoader;
import com.yahoo.slurper.webservice.data.config.table.GenericTableLoader;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.Supplier;

public class WikiJerseyTestBinder extends JerseyTestBinder {
    private static Supplier<List<? extends DataSourceConfiguration>> configLoader = new StaticWikiConfigLoader();
    private GenericDimensionConfigs genericDimensionConfigs;

    private static final String EXTERNAL_CONFIG_FILE_PATH  = System.getProperty("user.dir") + "/config/";
    private static final String DRUID_CONFIG_FILE_PATH  = System.getProperty("user.dir") + "/";
    private static ExternalConfigLoader externalConfigLoader = new ExternalConfigLoader();
    private ExternalDimensionsLoader externalDimensionsLoader;

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
        DimensionSerializer dimensionSerializer = new DimensionSerializer(new ObjectMapper());
        dimensionSerializer
                .setConfig(configLoader)
                .setPath("DimensionConfig.json")
                .parseToJson();

        externalDimensionsLoader = new ExternalDimensionsLoader(
                externalConfigLoader,
                DRUID_CONFIG_FILE_PATH
        );

        genericDimensionConfigs = new GenericDimensionConfigs(configLoader);
        return new LinkedHashSet<>(externalDimensionsLoader.getAllDimensionConfigurations());
    }

    @Override
    public MetricLoader getMetricLoader() {
        return new GenericMetricLoader(configLoader);
    }

    @Override
    public TableLoader getTableLoader() {
        return new GenericTableLoader(configLoader, genericDimensionConfigs, new DataSourceMetadataService());
//        return new ExternalTableLoader(
//                new DataSourceMetadataService(),
//                externalDimensionsLoader,
//                externalConfigLoader,
//                EXTERNAL_CONFIG_FILE_PATH
//        );
    }
}
