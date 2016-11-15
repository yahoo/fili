// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider.yaml.serde;

import com.yahoo.bard.webservice.data.config.provider.ConfigurationDictionary;
import com.yahoo.bard.webservice.data.config.provider.yaml.YamlDimensionFieldConfig;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.util.Map;

/**
 * Deserializer for dimension field configuration.
 */
public class YamlDimensionFieldConfigDeserializer
        extends JsonDeserializer<ConfigurationDictionary<YamlDimensionFieldConfig>> {
    protected static final TypeReference<ConfigurationDictionary<YamlDimensionFieldConfig>> TYPE = new
            TypeReference<ConfigurationDictionary<YamlDimensionFieldConfig>>() {
    };

    @Override
    public ConfigurationDictionary<YamlDimensionFieldConfig> deserialize(JsonParser p, DeserializationContext ctxt)
            throws IOException, JsonProcessingException {
        ConfigurationDictionary<YamlDimensionFieldConfig> dimensionFields = p.readValueAs(TYPE);

        for (Map.Entry<String, YamlDimensionFieldConfig> entry : dimensionFields.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }

        return dimensionFields;
    }
}
