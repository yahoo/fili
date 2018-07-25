// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.slurper.webservice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.bard.webservice.data.config.dimension.DefaultDimensionField;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.util.Utils;
import com.yahoo.slurper.webservice.data.config.DimensionObject;
import com.yahoo.slurper.webservice.data.config.auto.DataSourceConfiguration;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Serializer dimension into json file.
 */
public class DimensionSerializer extends ExternalConfigSerializer {

    private Map<String, Set<DimensionObject>> config = new HashMap<>();
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
     * Constructor.
     *
     * @param configLoader Supplies DataSourceConfigurations to build the dimensions from.
     *
     * @return DimensionSerializer
     */
    public DimensionSerializer setConfig(
            Supplier<List<? extends DataSourceConfiguration>> configLoader
    ) {
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
                    config.put("dimensions", dimensionObjects);
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
