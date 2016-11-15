// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider.yaml.serde;

import com.yahoo.bard.webservice.data.config.provider.ConfigurationDictionary;
import com.yahoo.bard.webservice.data.config.provider.yaml.YamlPhysicalTableConfig;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.util.Map;

/**
 * Deserializer for physical table configuration.
 */
public class YamlPhysicalTableConfigDeserializer
        extends JsonDeserializer<ConfigurationDictionary<YamlPhysicalTableConfig>> {
    protected static final TypeReference<ConfigurationDictionary<YamlPhysicalTableConfig>> TYPE = new
            TypeReference<ConfigurationDictionary<YamlPhysicalTableConfig>>() { };

    @Override
    public ConfigurationDictionary<YamlPhysicalTableConfig> deserialize(JsonParser p, DeserializationContext ctxt)
            throws IOException, JsonProcessingException {
        ConfigurationDictionary<YamlPhysicalTableConfig> result = p.readValueAs(TYPE);

        // Set the table name to the map key
        for (Map.Entry<String, YamlPhysicalTableConfig> entry : result.entrySet()) {
            entry.getValue().setTableName(entry.getKey());
        }

        return result;
    }
}
