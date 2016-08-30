// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.serializers;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.dimension.DefaultDimensionSpec;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Serializer to map Dimensions to DimensionSpec or api name for Abstract Druid Fact Queries.
 */
public class DimensionToDefaultDimensionSpec extends JsonSerializer<Dimension> {
    private static final Logger LOG = LoggerFactory.getLogger(DimensionToDefaultDimensionSpec.class);

    @Override
    public void serialize(Dimension value, JsonGenerator gen, SerializerProvider provider) throws IOException {

        String apiName = value.getApiName();
        String physicalName = Util.findPhysicalName(value, gen).orElseThrow(() -> {
                    LOG.error(ErrorMessageFormat.PHYSICAL_NAME_NOT_FOUND.logFormat(value.getApiName()));
                    return new IllegalStateException(ErrorMessageFormat.PHYSICAL_NAME_NOT_FOUND.format());
                }
        );

        // serialize to only apiName if api and physical name is same or there are nested queries
        if (physicalName.equals(apiName) || Util.hasInnerQuery(gen)) {
            gen.writeString(apiName);
        } else {
            gen.writeObject(new DefaultDimensionSpec(physicalName, apiName));
        }
    }
}
