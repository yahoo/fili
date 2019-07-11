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

    public static final String ENTITY_TYPE = "Dimension";

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
     * @param configTable  ObjectNode that points to the value of corresponding table entry in config file
     * @param resourceFactories  the source for locating dependent objects
     *
     * @return  A newly constructed config instance for the name and config provided
     */
    @Override
    public Dimension build(String name, ObjectNode configTable, LuthierIndustrialPark resourceFactories) {
        validateFields(name, configTable);
        String longName = configTable.get("longName").textValue();
        String category = configTable.get("category").textValue();
        String description = configTable.get("description").textValue();
        KeyValueStore keyValueStore = resourceFactories.getKeyValueStore(configTable.get("keyValueStore").textValue());
        SearchProvider searchProvider = resourceFactories.getSearchProvider(
                configTable.get("domain").textValue()
        );
        boolean isAggregatable = configTable.get("isAggregatable").booleanValue();
        LinkedHashSet<DimensionField> dimensionFields = new LinkedHashSet<>();
        LinkedHashSet<DimensionField> defaultDimensionFields = new LinkedHashSet<>();

        fieldsBuilder(
                configTable.get("fields"),
                configTable.get("defaultFields"),
                name,
                dimensionFields,
                defaultDimensionFields
        );

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

    /**
     * Helper function to validate only the fields needed in the parameter build.
     *
     * @param name  the config dictionary name (normally the apiName)
     * @param configTable  ObjectNode that points to the value of corresponding table entry in config file
     */
    private void validateFields(String name, ObjectNode configTable) {
        LuthierValidationUtils.validateField(configTable.get("longName"), ENTITY_TYPE, name, "longName");
        LuthierValidationUtils.validateField(configTable.get("category"), ENTITY_TYPE, name, "category");
        LuthierValidationUtils.validateField(configTable.get("description"), ENTITY_TYPE, name, "description");
        LuthierValidationUtils.validateField(configTable.get("keyValueStore"), ENTITY_TYPE, name, "keyValueStore");
        LuthierValidationUtils.validateField(configTable.get("searchProvider"), ENTITY_TYPE, name, "searchProvider");
        LuthierValidationUtils.validateField(configTable.get("fields"), ENTITY_TYPE, name, "fields");
        LuthierValidationUtils.validateField(configTable.get("defaultFields"), ENTITY_TYPE, name, "defaultFields");
        LuthierValidationUtils.validateField(configTable.get("isAggregatable"), ENTITY_TYPE, name, "isAggregatable");
    }
}
