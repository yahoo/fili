// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.slurper.webservice.data.config.dimension;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.bard.webservice.data.config.dimension.DefaultDimensionField;
import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.metadata.DataSourceMetadata;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.util.Utils;
import com.yahoo.slurper.webservice.ExternalConfigSerializer;
import com.yahoo.slurper.webservice.data.config.auto.DataSourceConfiguration;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Serializer dimension into json file.
 */
public class DimensionSerializer extends ExternalConfigSerializer {

    private Map<String, Set<DimensionObject>> config = new HashMap<>();
    private Map<String, Set<String>> tableToDimensionNames = new HashMap<>();
    private DataSourceMetadataService metadataService;
    private String path;

    /**
     * Constructor.
     *
     * @param mapper object mapper for serialization
     */
    public DimensionSerializer(ObjectMapper mapper) {
        super(mapper);
    }

    /**
     * Set dimension configs into config for parsing to json.
     *
     * @param configLoader Supplies DataSourceConfigurations to build the dimensions from.
     *
     * @return DimensionSerializer
     */
    public DimensionSerializer setConfig(
            Supplier<List<? extends DataSourceConfiguration>> configLoader
    ) {

        config.put("dimensions", new HashSet<>());
        metadataService = new DataSourceMetadataService();

        configLoader.get()
                .forEach(dataSourceConfiguration -> {
                    Set<DimensionObject> dimensionObjects = dataSourceConfiguration.getDimensions().stream()
                            .map(dimensionName -> new DimensionObject(
                                            dimensionName,
                                            dimensionName,
                                            "",
                                            dimensionName,
                                            "General",
                                            getDefaultFields()
                                    )
                            ).collect(
                                    Collectors.collectingAndThen(
                                            Collectors.toSet(),
                                            Collections::unmodifiableSet
                                    ));

                    tableToDimensionNames.put(
                            dataSourceConfiguration.getTableName().asName(),
                            dimensionObjects.stream()
                                    .map(DimensionObject::getApiName)
                            .collect(Collectors.toSet())
                            );

                    metadataService.update(
                            DataSourceName.of(dataSourceConfiguration.getName()),
                            new DataSourceMetadata(
                                    dataSourceConfiguration.getName(),
                                    Collections.emptyMap(),
                                    dataSourceConfiguration.getDataSegments()
                            )
                    );

                    config.get("dimensions").addAll(dimensionObjects);
                });

        return this;
    }

    /**
     * Set json file path.
     *
     * @param path json file path
     *
     * @return DimensionSerializer
     */
    public DimensionSerializer setPath(String path) {
        this.path = path + "DimensionConfig.json";
        return this;
    }

    /**
     * Returns all dimension configurations of a particular data source.
     *
     * @param dataSourceName  Name of the data source
     *
     * @return all dimension names of the particular data source
     */
    public Set<String> getDimensionConfigs(String dataSourceName) {
        return tableToDimensionNames.getOrDefault(dataSourceName, Collections.emptySet());
    }

    /**
     * Parse config to json file.
     *
     */
    public void parseToJson() {
        super.parse(config, path);
    }

    /**
     * Get metaDataService.
     *
     * @return metaDataService
     */
    public DataSourceMetadataService getMetadataService() {
        return metadataService;
    }

    /**
     * get Default dimension fields.
     *
     * @return a set of default dimension fields
     */
    private LinkedHashSet<DimensionField> getDefaultFields() {
        return Utils.asLinkedHashSet(
                DefaultDimensionField.ID);
    }
}
