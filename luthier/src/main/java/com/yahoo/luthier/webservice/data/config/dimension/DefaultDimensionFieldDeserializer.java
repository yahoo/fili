// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config.dimension;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.LinkedHashSet;

/**
 * Deserialize dimension fields.
 * <p>
 * If a dimension has "field" keyword, it may be either a string like:
 *
 *     fields: "A"
 *
 * or a field list:
 *
 *     fields: [
 *          {
 *              "name" : "fieldA"
 *          },
 *          {
 *              "name" : "fieldB"
 *          }
 *     ]
 *
 * This deserializer can distinguish these two cases and set field info into WikiDimensionFieldConfigTemplate
 *
 */
public class DefaultDimensionFieldDeserializer extends JsonDeserializer<DefaultDimensionFieldListTemplate> {

    /**
     * Deserialize dimension field configuration.
     *
     * @param jp Json parser to parse json
     * @param ctxt Deserialization context
     * @return dimension filed info (an instance of WikiDimensionFieldConfigTemplate)
     * @throws IOException when json file not found or read exception occurs
     */
    @Override
    public DefaultDimensionFieldListTemplate deserialize(
            JsonParser jp,
            DeserializationContext ctxt
    ) throws IOException {

        DefaultDimensionFieldListTemplate dimensionField = new DefaultDimensionFieldListTemplate();

        if (jp.getCurrentToken() == JsonToken.VALUE_STRING) {
            // if field is a string, set field name to this string
            dimensionField.setFieldName(jp.getText());
        } else {
            ObjectCodec oc = jp.getCodec();
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode node = oc.readTree(jp);
            // if field is a list, set field list to this list
            LinkedHashSet<DimensionFieldInfoTemplate> list = objectMapper.convertValue(node,
                    new TypeReference<LinkedHashSet<DefaultDimensionFieldInfoTemplate>>() { });
            dimensionField.setFieldList(list);
        }

        return dimensionField;
    }
}
