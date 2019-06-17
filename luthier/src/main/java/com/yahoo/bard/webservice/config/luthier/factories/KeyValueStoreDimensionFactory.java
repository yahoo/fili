// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config.luthier.factories;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yahoo.bard.webservice.config.luthier.Factory;
import com.yahoo.bard.webservice.config.luthier.LuthierFactoryException;
import com.yahoo.bard.webservice.config.luthier.LuthierIndustrialPark;
import com.yahoo.bard.webservice.data.config.LuthierDimensionField;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension;
import com.yahoo.bard.webservice.util.EnumUtils;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.ArrayList;
import java.util.List;


/**
 * A factory that is used by default to support KeyValueStore Dimensions.
 */
public class KeyValueStoreDimensionFactory implements Factory<Dimension> {
    public static final String DEFAULT_FIELD_NAME_ERROR = "Dimension: '%s' defaultField name '%s' not in its fields";

    /**
     * Helper function to build both fields and defaultFields.
     *
     * @param fieldsNode  the JsonNode object that points to the content of "fields" key
     * @param defaultFieldsNode  the JsonNode object that contains a list of field nam
     * @param dimensionName the name of the dimension passed by the caller, needed for error message
     * @param dimensionFields  an empty LinkedHashSet of DimensionField, will be populated by this method
     * @param defaultDimensionFields  an empty LinkedHashSet of DimensionField, will be populated by this method
     */
    private void fieldsBuilder(
            JsonNode fieldsNode,
            JsonNode defaultFieldsNode,
            String dimensionName,
            LinkedHashSet<DimensionField> dimensionFields,
            LinkedHashSet<DimensionField> defaultDimensionFields
    ) {
        LinkedHashMap<String, DimensionField> fieldsMap = new LinkedHashMap<>();
        // build the fields map
        for (JsonNode node : fieldsNode) {
            List<String> tags = new ArrayList<>();
            for (final JsonNode strNode : node.get("tags")) {
                tags.add(strNode.textValue());
            }
            String fieldName = node.get("name").textValue();
            fieldsMap.put(
                    fieldName,
                    new LuthierDimensionField(
                            EnumUtils.camelCase(fieldName),
                            fieldName,
                            tags
                    )
            );
        }
        dimensionFields.addAll(fieldsMap.values());
        // build the defaultFields map
        for (JsonNode node: defaultFieldsNode) {
            String fieldName = node.textValue();
            if (fieldsMap.get(fieldName) == null) {
                String message = String.format(DEFAULT_FIELD_NAME_ERROR, dimensionName, fieldName);
                throw new LuthierFactoryException(message);
            }
            defaultDimensionFields.add(
                    fieldsMap.get(fieldName)
            );
        }
    }

    /**
     * Build a dimension instance.
     *
     * @param name  the config dictionary name (normally the apiName)
     * @param configTable  the json tree describing this config entity
     * @param resourceFactories  the source for locating dependent objects
     *
     * @return  A newly constructed config instance for the name and config provided
     */
    @Override
    public Dimension build(String name, ObjectNode configTable, LuthierIndustrialPark resourceFactories) {
        String longName = configTable.get("longName").textValue();
        String category = configTable.get("category").textValue();
        String description = configTable.get("description").textValue();

        KeyValueStore keyValueStore = resourceFactories.getKeyValueStore(
                configTable.get("description").textValue()
        );
        SearchProvider searchProvider = resourceFactories.getSearchProvider(
                configTable.get("searchProvider").textValue()
        );
        LinkedHashSet<DimensionField> dimensionFields = new LinkedHashSet<>();
        LinkedHashSet<DimensionField> defaultDimensionFields = new LinkedHashSet<>();
        fieldsBuilder(
                configTable.get("fields"),
                configTable.get("defaultFields"),
                name,
                dimensionFields,
                defaultDimensionFields
        );
        boolean isAggregatable = configTable.get("isAggregatable").booleanValue();

        return new KeyValueStoreDimension(
                name,
                longName,
                category,
                description,
                dimensionFields,
                keyValueStore,
                searchProvider,
                defaultDimensionFields,
                isAggregatable
        );
    }
}
