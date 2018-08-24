// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.slurper.webservice.data.config.table;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.slurper.webservice.data.config.dimension.DimensionSerializer;
import com.yahoo.slurper.webservice.ExternalConfigSerializer;
import com.yahoo.slurper.webservice.data.config.metric.MetricSerializer;
import com.yahoo.slurper.webservice.data.config.JsonObject;
import com.yahoo.slurper.webservice.data.config.auto.DataSourceConfiguration;

import java.util.*;
import java.util.function.Supplier;

/**
 * Serializer tables into json file.
 */
public class TableSerializer extends ExternalConfigSerializer {

    private Map<String, Set<JsonObject>> config = new HashMap<>();
    private String path;
    private DimensionSerializer dimensionSerializer;
    private MetricSerializer metricSerializer;

    /**
     * Constructor.
     *
     * @param mapper object mapper for serialization
     */
    public TableSerializer(ObjectMapper mapper) {
        super(mapper);
    }

    /**
     * Set dimension serializer.
     *
     * @param dimensionSerializer dimension serializer that stores map from table name to dimension
     *
     * @return TableSerializer
     */
    public TableSerializer setDimensions(DimensionSerializer dimensionSerializer) {
        this.dimensionSerializer = dimensionSerializer;
        return this;
    }

    /**
     * Set metric serializer.
     *
     * @param metricSerializer metric serializer that stores map from table name to metrics
     *
     * @return TableSerializer
     */
    public TableSerializer setMetrics(MetricSerializer metricSerializer) {
        this.metricSerializer = metricSerializer;
        return this;
    }

    /**
     * Set table configs into config for parsing to json.
     *
     * @param configLoader Supplies DataSourceConfigurations to build the tables from.
     *
     * @return TableSerializer
     */
    public TableSerializer setConfig(
            Supplier<List<? extends DataSourceConfiguration>> configLoader
    ) {

        Set<JsonObject> tableObjects = new HashSet<>();
        Set<JsonObject> logicalTableObjects = new HashSet<>();

        configLoader.get()
                .forEach(dataSourceConfiguration -> {

                    String tableName = dataSourceConfiguration.getTableName().asName();

                    tableObjects.add(new PhysicalTableObject(
                            tableName,
                            dataSourceConfiguration.getValidTimeGrain().toString(),
                            dimensionSerializer.getDimensionConfigs(tableName),
                            metricSerializer.getMetrics(tableName)
                    ));

                    logicalTableObjects.add(new LogicalTableObject(
                            tableName,
                            dataSourceConfiguration.getValidTimeGrain().toString(),
                            dimensionSerializer.getDimensionConfigs(tableName),
                            metricSerializer.getMetrics(tableName),
                            tableName
                    ));
                });

        config.put("physicalTables", tableObjects);
        config.put("logicalTables", logicalTableObjects);

        return this;
    }

    /**
     * Set json file path.
     *
     * @param path json file path
     *
     * @return  TableSerializer
     */
    public TableSerializer setPath(String path) {
        this.path = path + "TableConfig.json";
        return this;
    }

    /**
     * Parse config to json file.
     */
    public void parseToJson() {
        super.parse(config, path);
    }
}
