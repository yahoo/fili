// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.provider.yaml.serde;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.IOException;
import java.util.HashMap;

/**
 * Utility functions to test deserializers.
 */
public class DeserializationHelper {

    /**
     * Get an object mapper that deserializes using the specified deserializer.
     *
     * @param asClass target class
     * @param deserializer deserializer
     * @return object mapper that uses deserializer to turn json into the target class
     */
    public static ObjectMapper getMapper(Class asClass, JsonDeserializer deserializer) {
        ObjectMapper mapper = new ObjectMapper();
        HashMap<Class<?>, JsonDeserializer<?>> map = new HashMap<>();
        map.put(asClass, deserializer);
        mapper.registerModule(new SimpleModule("module", new Version(1, 1, 1, ""), map));
        return mapper;
    }

    /**
     * Deserialize json using given deserializer.
     *
     * @param json the json string to deserialize
     * @param deserializer deserializer to use
     * @param <T> class to return
     * @return deserialized object
     * @throws IOException when object could not be deserialized
     */
    public static <T> T deserialize(String json, JsonDeserializer<T> deserializer) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonParser parser = mapper.getFactory().createParser(json);
        return deserializer.deserialize(parser, null);
    }
}
