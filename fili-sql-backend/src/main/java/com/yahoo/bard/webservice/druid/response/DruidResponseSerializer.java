// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.response;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

/**
 * Created by hinterlong on 5/31/17.
 */
public class DruidResponseSerializer extends JsonSerializer {

    @Override
    public void serialize(Object o, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException, JsonProcessingException {
        DruidResponse druidResponse = (DruidResponse) o;
        // todo test cases?
        jsonGenerator.writeStartArray();
        for (Object result : druidResponse.results) {
            jsonGenerator.writeObject(result);
        }
        jsonGenerator.writeEndArray();
    }
}
