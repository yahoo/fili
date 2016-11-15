// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider.yaml.serde;

import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.provider.ConfigurationDictionary;
import com.yahoo.bard.webservice.data.config.provider.yaml.YamlDimensionConfig;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.util.Map;

/**
 * Deserializer for dimension configuration.
 */
public class YamlDimensionConfigDeserializer
        extends JsonDeserializer<ConfigurationDictionary<? extends DimensionConfig>> {

    protected static final TypeReference<ConfigurationDictionary<YamlDimensionConfig>> TYPE = new
            TypeReference<ConfigurationDictionary<YamlDimensionConfig>>() { };

    @Override
    public ConfigurationDictionary<? extends DimensionConfig> deserialize(JsonParser p, DeserializationContext ctxt)
            throws IOException, JsonProcessingException {
        ConfigurationDictionary<YamlDimensionConfig> map = p.readValueAs(TYPE);

        for (Map.Entry<String, YamlDimensionConfig> entry : map.entrySet()) {
            entry.getValue().setApiName(entry.getKey());
        }
        return map;
    }
}
