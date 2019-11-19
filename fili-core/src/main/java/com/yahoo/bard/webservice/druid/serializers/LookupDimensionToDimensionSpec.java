// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.serializers;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.impl.ExtractionFunctionDimension;
import com.yahoo.bard.webservice.data.dimension.impl.LookupDimension;
import com.yahoo.bard.webservice.data.dimension.impl.RegisteredLookupDimension;
import com.yahoo.bard.webservice.druid.model.dimension.ExtractionDimensionSpec;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.ExtractionFunction;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.sun.corba.se.impl.io.TypeMismatchException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

/**
 * Serializer to map LookupDimension to either DimensionSpec base on namespaces.
 */
public class LookupDimensionToDimensionSpec extends JsonSerializer<ExtractionFunctionDimension> {
    private static final Logger LOG = LoggerFactory.getLogger(LookupDimensionToDimensionSpec.class);

    @Override
    public void serialize(ExtractionFunctionDimension value, JsonGenerator gen, SerializerProvider provider)
            throws IOException {

        if (!LookupDimension.class.isInstance(value) && !RegisteredLookupDimension.class.isInstance(value)) {
            throw new TypeMismatchException("Lookup dimension serializer was given a non-lookup dimension.");
        }

        Optional<ExtractionFunction> extractionFunction = value.getExtractionFunction();

        // Use DimensionToDefaultDimensionSpec serializer if LookupDimension does not contain any namespace or lookups
        // or is not the inner most query
        if (!extractionFunction.isPresent() || SerializerUtil.hasInnerQuery(gen)) {
            JsonSerializer<Object> dimensionSerializer = provider.findValueSerializer(Dimension.class);
            dimensionSerializer.serialize(value, gen, provider);
            return;
        }

        String apiName = value.getApiName();
        String physicalName = SerializerUtil.findPhysicalName(value, gen).orElseThrow(() -> {
                    LOG.error(ErrorMessageFormat.PHYSICAL_NAME_NOT_FOUND.logFormat(value.getApiName()));
                    return new IllegalStateException(ErrorMessageFormat.PHYSICAL_NAME_NOT_FOUND.format());
                }
        );

        gen.writeObject(new ExtractionDimensionSpec(physicalName, apiName, extractionFunction.get(), value));
    }
}
