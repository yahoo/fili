// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config.luthier.factories;

import com.yahoo.bard.webservice.config.luthier.Factory;
import com.yahoo.bard.webservice.config.luthier.LuthierFactoryException;
import com.yahoo.bard.webservice.config.luthier.LuthierIndustrialPark;
import com.yahoo.bard.webservice.config.luthier.LuthierValidationUtils;
import com.yahoo.bard.webservice.data.config.LuthierDimensionField;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension;
import com.yahoo.bard.webservice.util.EnumUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;


/**
 * A factory that is used by default to support KeyValueStore Dimensions.
 */
public class KeyValueStoreDimensionFactory implements Factory<Dimension> {
    public static final String DEFAULT_FIELD_NAME_ERROR =
            "Dimension '%s': defaultField name '%s' not found in fields '%s'";

    public static final String DIMENSION = "Dimension";

    /**
     * Helper function to build both fields and defaultFields.
     *
     * @param fieldsNode  the JsonNode object that points to the content of "fields" key
     * @param defaultFieldsNode  the JsonNode object that contains the list of field names to be shown by default
     * @param dimensionName  the name of the dimension passed by the caller, needed for error messages
     * @param dimensionFields  an empty collection that will be populated with the dimension's fields
     * @param defaultDimensionFields an empty collection that will be populated by the dimension's default fields
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
            for (JsonNode strNode : node.get("tags")) {
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
            String defaultFieldName = node.textValue();
            if (fieldsMap.get(defaultFieldName) == null) {
                String fieldNames = dimensionFields.stream()
                        .map(DimensionField::getName)
                        .collect(Collectors.joining(", "));
                String message = String.format(DEFAULT_FIELD_NAME_ERROR, dimensionName, defaultFieldName, fieldNames);
                throw new LuthierFactoryException(message);
            }
            defaultDimensionFields.add(fieldsMap.get(defaultFieldName));
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
        LuthierValidationUtils.validateField(configTable.get("longName"), DIMENSION, name, "longName");
        String longName = configTable.get("longName").textValue();

        LuthierValidationUtils.validateField(configTable.get("category"), DIMENSION, name, "category");
        String category = configTable.get("category").textValue();

        LuthierValidationUtils.validateField(configTable.get("description"), DIMENSION, name, "description");
        String description = configTable.get("description").textValue();

        LuthierValidationUtils.validateField(configTable.get("keyValueStore"), DIMENSION, name, "keyValueStore");
        KeyValueStore keyValueStore = resourceFactories.getKeyValueStore(configTable.get("keyValueStore").textValue());

        LuthierValidationUtils.validateField(configTable.get("searchProvider"), DIMENSION, name, "searchProvider");
        SearchProvider searchProvider = resourceFactories.getSearchProvider(
                configTable.get("domain").textValue()
        );

        LinkedHashSet<DimensionField> dimensionFields = new LinkedHashSet<>();
        LinkedHashSet<DimensionField> defaultDimensionFields = new LinkedHashSet<>();

        LuthierValidationUtils.validateField(configTable.get("fields"), DIMENSION, name, "fields");
        LuthierValidationUtils.validateField(configTable.get("defaultFields"), DIMENSION, name, "defaultFields");
        fieldsBuilder(
                configTable.get("fields"),
                configTable.get("defaultFields"),
                name,
                dimensionFields,
                defaultDimensionFields
        );

        LuthierValidationUtils.validateField(configTable.get("isAggregatable"), DIMENSION, name, "isAggregatable");
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
