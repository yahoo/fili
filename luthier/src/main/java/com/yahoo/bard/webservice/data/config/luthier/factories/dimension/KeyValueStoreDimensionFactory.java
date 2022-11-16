// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.luthier.factories.dimension;

import com.yahoo.bard.webservice.application.luthier.LuthierConfigNode;
import com.yahoo.bard.webservice.data.config.luthier.Factory;
import com.yahoo.bard.webservice.data.config.luthier.LuthierIndustrialPark;
import com.yahoo.bard.webservice.data.config.luthier.LuthierValidationUtils;
import com.yahoo.bard.webservice.data.config.luthier.dimension.LuthierDimensionField;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension;
import com.yahoo.bard.webservice.data.dimension.metadata.StorageStrategy;
import com.yahoo.bard.webservice.exceptions.LuthierFactoryException;

import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A factory that is used by default to support KeyValueStore Dimensions.
 *
 * A KeyValueStoreDimensionFactory expects the following fields in its configuration:
 *
 * Required Fields:
 * <ol>
 *  <li> {@code longName} - Text. Longer, human friendly name for the dimension.
 *  <li> {@code category} - Text. Category of the dimension for human/external tool consumption.
 *  <li> {@code description} - Text. Long-winded description of the dimension for human consumption.
 *  <li> {@code keyValueStore} - Text. Name of the KeyValueStore instance used by this dimension.
 *  <li> {@code searchProvider} - Text. Name of the SearchProvider instance used by this dimension.
 *  <li> {@code fields} - List&lt;LuthierConfigNode&gt;. DimensionField configuration for dimension's fields.
 *      Required Fields:
 *      <ol>
 *       <li> {@code name} - Text. Name of the field. Should be unique for this Dimension.
 *       <li> {@code tags} - List&lt;Text&gt;. Tags attached to this field for external tool consumption.
 *      </ol>
 *  <li> {@code defaultFields} - List&lt;Text&gt;.
 *          Field names shown in results by default. Must be defined in {@code fields}.
 *  <li> {@code isAggregatable} - Boolean. Whether or not this dimension can be aggregated.
 P* </ol>
 */
public class KeyValueStoreDimensionFactory implements Factory<Dimension> {
    private static final String DEFAULT_FIELD_NAME_ERROR =
            "Dimension '%s': defaultField name '%s' not found in fields '%s'";

    private static final String ENTITY_TYPE = "Dimension";
    private static final String LONG_NAME = "longName";
    private static final String CATEGORY = "category";
    private static final String DESCRIPTION = "description";
    private static final String KEY_VALUE_STORE = "keyValueStore";
    private static final String SEARCH_PROVIDER = "searchProvider";
    private static final String FIELDS = "fields";
    private static final String DEFAULT_FIELDS = "defaultFields";
    private static final String IS_AGGREGATABLE = "isAggregatable";
    private static final String DIMENSION_DOMAIN = "dimensionDomain";
    private static final String SKIP_LOADING = "skipLoading";
    private static final String STORAGE_STRATEGY = "storageStrategy";
    private static final DateTime LOAD_TIME = new DateTime();

    /**
     * Helper function to build both fields and defaultFields.
     *
     * @param fieldsNode  the LuthierConfigNode object that points to the content of "fields" key
     * @param defaultFieldsNode  the LuthierConfigNode object that contains the list of field names to be shown by
     * default
     * @param dimensionName  the name of the dimension passed by the caller, needed for error messages
     * @param dimensionFields  an empty collection that will be populated with the dimension's fields
     * @param defaultDimensionFields an empty collection that will be populated by the dimension's default fields
     */
    private void fieldsBuilder(
            LuthierConfigNode fieldsNode,
            LuthierConfigNode defaultFieldsNode,
            String dimensionName,
            LinkedHashSet<DimensionField> dimensionFields,
            LinkedHashSet<DimensionField> defaultDimensionFields
    ) {
        LinkedHashMap<String, DimensionField> fieldsMap = new LinkedHashMap<>();
        // build the fields map
        for (LuthierConfigNode node : fieldsNode) {
            List<String> tags = new ArrayList<>();
            for (LuthierConfigNode strNode : node.get("tags")) {
                tags.add(strNode.textValue());
            }
            String fieldName = node.get("name").textValue();
            fieldsMap.put(
                    fieldName,
                    new LuthierDimensionField(
                            fieldName,
                            fieldName,
                            tags
                    )
            );
        }
        dimensionFields.addAll(fieldsMap.values());
        // build the defaultFields map
        for (LuthierConfigNode node: defaultFieldsNode) {
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
     * @param configTable  LuthierConfigNode that points to the value of corresponding table entry in config file
     * @param resourceFactories  the source for locating dependent objects
     *
     * @return  A newly constructed config instance for the name and config provided
     */
    @Override
    public Dimension build(String name, LuthierConfigNode configTable, LuthierIndustrialPark resourceFactories) {
        LuthierValidationUtils.validateFields(
                configTable,
                ENTITY_TYPE,
                name,
                LONG_NAME,
                CATEGORY,
                DESCRIPTION,
                KEY_VALUE_STORE,
                SEARCH_PROVIDER,
                FIELDS,
                DEFAULT_FIELDS,
                IS_AGGREGATABLE,
                DIMENSION_DOMAIN,
                SKIP_LOADING,
                STORAGE_STRATEGY
        );

        String longName = configTable.get(LONG_NAME).textValue();
        String category = configTable.get(CATEGORY).textValue();
        String description = configTable.get(DESCRIPTION).textValue();
        KeyValueStore keyValueStore = resourceFactories.getKeyValueStore(configTable.get(DIMENSION_DOMAIN).textValue());
        boolean isAggregatable = configTable.get(IS_AGGREGATABLE).booleanValue();
        boolean skipLoading = configTable.get(SKIP_LOADING).booleanValue();
        SearchProvider searchProvider = resourceFactories.getSearchProvider(
                configTable.get(DIMENSION_DOMAIN).textValue()
        );
        StorageStrategy storageStrategy = StorageStrategy.valueOf(
                configTable.get(STORAGE_STRATEGY).textValue()
        );
        LinkedHashSet<DimensionField> dimensionFields = new LinkedHashSet<>();
        LinkedHashSet<DimensionField> defaultDimensionFields = new LinkedHashSet<>();

        fieldsBuilder(
                configTable.get(FIELDS),
                configTable.get(DEFAULT_FIELDS),
                name,
                dimensionFields,
                defaultDimensionFields
        );

        KeyValueStoreDimension result = new KeyValueStoreDimension(
                name,
                longName,
                category,
                description,
                dimensionFields,
                keyValueStore,
                searchProvider,
                defaultDimensionFields,
                isAggregatable,
                storageStrategy
        );
        if (skipLoading) {
            result.setLastUpdated(LOAD_TIME);
        }

        return result;
    }
}
