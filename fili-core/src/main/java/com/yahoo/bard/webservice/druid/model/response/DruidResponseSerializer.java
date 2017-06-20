// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.response;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

/**
 * Serializes a {@link com.yahoo.bard.webservice.druid.model.response.DruidResponse} into the
 * same format as Druid's responses.
 */
public class DruidResponseSerializer extends JsonSerializer<DruidResponse> {

    @Override
    public void serialize(
            DruidResponse druidResponse,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider
    ) throws IOException {
        jsonGenerator.writeStartArray();
        for (DruidResultRow resultRow : druidResponse.getResults()) {
            jsonGenerator.writeObject(resultRow);
        }
        jsonGenerator.writeEndArray();
    }
}
