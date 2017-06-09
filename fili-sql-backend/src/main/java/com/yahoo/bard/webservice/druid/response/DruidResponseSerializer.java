// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.response;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

/**
 * Serializes {@link DruidResponse} into a json equivalent to Druid's actual responses.
 */
public class DruidResponseSerializer extends JsonSerializer {

    @Override
    public void serialize(Object o, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException {
        DruidResponse druidResponse = (DruidResponse) o;
        jsonGenerator.writeStartArray();
        for (Object result : druidResponse.getResults()) {
            jsonGenerator.writeObject(result);
        }
        jsonGenerator.writeEndArray();
    }
}
