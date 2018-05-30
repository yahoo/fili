// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.rfc_dimension.impl;

import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension;
import com.yahoo.bard.webservice.data.dimension.rfc_dimension.DomainSchema;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class StandardJsonDimensionRowFactory implements DimensionRowFactory {

    private static final Logger LOG = LoggerFactory.getLogger(KeyValueStoreDimension.class);

    private static final String MISSING_ROW_KEY_FORMAT = "Dimension row '%s' doesn't contain expected key '%s'";

    ObjectMapper objectMapper;

    public StandardJsonDimensionRowFactory() {
        this(new ObjectMapper());
    }

    public StandardJsonDimensionRowFactory(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public DimensionRow parseDimensionRow(Map<String, String> fieldNameValueMap, DomainSchema dimensionSchema) {
        LinkedHashMap<DimensionField, String> dimensionRowFieldValues = new LinkedHashMap<>(fieldNameValueMap.size());

        // Load every field we expect and only fields we expect
        for (DimensionField field : dimensionSchema.getDimensionFields()) {
            String fieldName = field.getName();
            String value = fieldNameValueMap.get(fieldName);
            if (value == null) {
                // A missing key value is unacceptable
                if (field == dimensionSchema.getKey()) {
                    String error = String.format(MISSING_ROW_KEY_FORMAT, fieldNameValueMap.toString(), fieldName);
                    LOG.info(error);
                    throw new IllegalArgumentException(error);
                }
                // A missing value for another field is turned into the empty string
                value = "";
            }
            dimensionRowFieldValues.put(field, value);
        }
        return new DimensionRow(dimensionSchema.getKey(), dimensionRowFieldValues);
    }

    @Override
    public DimensionRow apply(final byte[] bytes, DomainSchema domainSchema) {
        if (bytes != null) {
            try {
                LinkedHashMap<String, String> fieldNameValueMap = objectMapper.readValue(
                        bytes,
                        new TypeReference<LinkedHashMap<String, String>>() { }
                );
                return parseDimensionRow(fieldNameValueMap, domainSchema);

            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        return null;
    }
}
