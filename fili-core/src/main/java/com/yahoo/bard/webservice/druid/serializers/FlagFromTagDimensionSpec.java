// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.serializers;

import com.yahoo.bard.webservice.data.dimension.impl.FlagFromTagDimension;
import com.yahoo.bard.webservice.data.dimension.impl.RegisteredLookupDimension;
import com.yahoo.bard.webservice.druid.model.query.DruidQuery;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.sun.corba.se.impl.io.TypeMismatchException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Serializer to map LookupDimension to either DimensionSpec base on namespaces.
 */
public class FlagFromTagDimensionSpec extends JsonSerializer<FlagFromTagDimension> {

    private static final Logger LOG = LoggerFactory.getLogger(FlagFromTagDimensionSpec.class);

    @Override
    public void serialize(FlagFromTagDimension value, JsonGenerator gen, SerializerProvider provider)
            throws IOException {

        if (!FlagFromTagDimension.class.isInstance(value)) {
            throw new TypeMismatchException("Tag dimension serializer was given a non tag dimension.");
        }


        JsonStreamContext context = gen.getOutputContext();
        if (context.getCurrentValue() instanceof DruidQuery && context.getCurrentName().equals("dimensions")) {
            JsonSerializer<Object> dimensionSerializer = provider.findValueSerializer(RegisteredLookupDimension.class);
            dimensionSerializer.serialize(value, gen, provider);
            return;
        }


        JsonSerializer<Object> dimensionSerializer = provider.findValueSerializer(value.getFilteringDimension()
                .getClass());
        dimensionSerializer.serialize(value.getCardinality(), gen, provider);
        return;
    }
}
