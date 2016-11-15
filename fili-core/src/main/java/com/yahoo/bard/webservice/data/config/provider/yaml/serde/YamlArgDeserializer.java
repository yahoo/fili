// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider.yaml.serde;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;

/**
 * Deserialize a list of arguments into Java objects
 *
 * If any of them look like they're annotated with YAML types, try to interpret them.
 */
public class YamlArgDeserializer extends JsonDeserializer<Object[]> {
    @Override
    public Object[] deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        Object[] args = p.readValueAs(Object[].class);

        int i;
        for (i = 0; i < args.length; i++) {
            if (args[i] != null && args[i] instanceof String && ((String) args[i]).startsWith("!!")) {
                Yaml yaml = new Yaml();
                args[i] = yaml.load((String) args[i]);
            }
        }

        return args;
    }
}
