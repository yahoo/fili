// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Serialization strategy for DimensionRow.
 */
public class DimensionRowSerializer extends JsonSerializer<DimensionRow> {

    @Override
    public void serialize(DimensionRow value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeObject(value.entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey().getName(), Map.Entry::getValue)));
    }
}
