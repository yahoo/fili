// Copyright 2019 Verizon Media Group.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.serializers;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.impl.FlagFromTagDimension;
import com.yahoo.bard.webservice.druid.model.dimension.ExtractionDimensionSpec;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.ExtractionFunction;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonStreamContext;
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
public class FlagFromTagDimensionSpec extends JsonSerializer<FlagFromTagDimension> {

    private static final Logger LOG = LoggerFactory.getLogger(FlagFromTagDimensionSpec.class);

    @Override
    public void serialize(FlagFromTagDimension value, JsonGenerator gen, SerializerProvider provider)
            throws IOException {

        if (!FlagFromTagDimension.class.isInstance(value)) {
            throw new TypeMismatchException("Tag dimension serializer was given a non tag dimension.");
        }


        JsonStreamContext context = gen.getOutputContext();
        // Dimensions block indicated group by expression. Use grouping dimension
        if (context.getParent().getCurrentName().equals("dimensions")) {

            Optional<ExtractionFunction> extractionFunction = value.getGroupingDimension().getExtractionFunction();

            // Use DimensionToDefaultDimensionSpec serializer if there are no lookups to serialize or we are not in the
            // inner most query
            if (!extractionFunction.isPresent() || SerializerUtil.hasInnerQuery(gen)) {
                JsonSerializer<Object> dimensionSerializer = provider.findValueSerializer(Dimension.class);
                dimensionSerializer.serialize(value, gen, provider);
                return;
            }

            String apiName = value.getApiName();
            String physicalName = SerializerUtil.findPhysicalName(value.getGroupingDimension(), gen).orElseThrow(() -> {
                        LOG.error(ErrorMessageFormat.PHYSICAL_NAME_NOT_FOUND.logFormat(value.getApiName()));
                        return new IllegalStateException(ErrorMessageFormat.PHYSICAL_NAME_NOT_FOUND.format());
                    }
            );

            gen.writeObject(
                    new ExtractionDimensionSpec(
                            physicalName,
                            apiName,
                            extractionFunction.get(),
                            value.getGroupingDimension()
                    )
            );
            return;
        }

        // If we are not grouping use the multivalued filtering dimension instead.
        JsonSerializer<Object> dimensionSerializer = provider.findValueSerializer(value.getFilteringDimension()
                .getClass());
        dimensionSerializer.serialize(value.getFilteringDimension(), gen, provider);
    }
}
