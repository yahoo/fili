// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.serializers;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Serializer to map Dimensions to Physical names for Abstract Druid Fact Queries.
 */
public class DimensionToNameSerializer extends JsonSerializer<Dimension> {
    private static final Logger LOG = LoggerFactory.getLogger(DimensionToNameSerializer.class);

    @Override
    public void serialize(Dimension value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        gen.writeString(
                SerializerUtil.findPhysicalName(value, gen).orElseThrow(() -> {
                            LOG.error(ErrorMessageFormat.PHYSICAL_NAME_NOT_FOUND.logFormat(value.getApiName()));
                            return new IllegalStateException(ErrorMessageFormat.PHYSICAL_NAME_NOT_FOUND.format());
                        }
                )
        );
    }
}
