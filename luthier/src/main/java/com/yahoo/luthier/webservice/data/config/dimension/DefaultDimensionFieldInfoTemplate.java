// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config.dimension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.yahoo.bard.webservice.data.dimension.Tag;
import com.yahoo.bard.webservice.data.dimension.TaggedDimensionField;
import com.yahoo.bard.webservice.util.EnumUtils;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Dimension field set template.
 * <p>
 * An example:
 * <p>
 *      {
 *          "name": "ID",
 *          "description": "Dimension ID",
 *          "tags": [
 *              "primaryKey"
 *          ]
 *      }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DefaultDimensionFieldInfoTemplate implements DimensionFieldInfoTemplate, TaggedDimensionField {

    private final String name;
    private final String description;
    private final List<String> tags;

    /**
     * Constructor used by json parser.
     *
     * @param name json property name
     * @param description json property description
     * @param tags json property tags
     */
    @JsonCreator
    public DefaultDimensionFieldInfoTemplate(
            @NotNull @JsonProperty("name") String name,
            @JsonProperty("description") String description,
            @JsonProperty("tags") List<String> tags
    ) {
        this.name = EnumUtils.camelCase(name);
        this.description = (Objects.isNull(description) ? "" : description);
        this.tags = (Objects.isNull(tags) ? Collections.emptyList() : ImmutableList.copyOf(tags));
    }

    @Override
    public String getFieldName() {
        return this.name;
    }

    @Override
    public String getFieldDescription() {
        return this.description;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    /**
     * Get dimensions tags.
     *
     * @return a set of dimension tags
     */
    @Override
    public Set<? extends Tag> getTags() {
        return this.tags.stream().map(tag -> (Tag) () -> (tag)).collect(Collectors.toSet());
    }

    @Override
    public TaggedDimensionField build() {
        return this;
    }

    /**
     * dimension field deserializer builder.
     *
     * @return a dimension field deserializer instance
     **/
    public static JsonDeserializer<DefaultDimensionFieldListTemplate> deserializer() {
        return new DimensionFieldDeserializer();
    }

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
     * This deserializer can distinguish these two cases and set field info into DimensionFieldListTemplate
     *
     */
    private static class DimensionFieldDeserializer extends JsonDeserializer<DefaultDimensionFieldListTemplate> {

        /**
         * Deserialize dimension field configuration.
         *
         * @param jp Json parser to parse json
         * @param ctxt Deserialization context
         *
         * @return dimension field info (an instance of DimensionFieldInfoTemplate)
         *
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
                List<DimensionFieldInfoTemplate> list = objectMapper.convertValue(node,
                        new TypeReference<List<DefaultDimensionFieldInfoTemplate>>() { });
                dimensionField.setFieldList(list);
            }

            return dimensionField;
        }
    }

    @Override
    public String toString() {
        return this.getName();
    }
}
