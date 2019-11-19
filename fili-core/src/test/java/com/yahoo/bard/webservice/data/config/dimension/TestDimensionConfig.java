// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.dimension;

import com.yahoo.bard.webservice.data.config.names.TestApiDimensionName;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.metadata.StorageStrategy;

import java.util.LinkedHashSet;

/**
 * Hold all of the Configuration information for the dimensions.
 */
public class TestDimensionConfig implements DimensionConfig {
    private final TestApiDimensionName apiName;
    private final String physicalName;
    private final String description;
    private final StorageStrategy storageStrategy;

    private LinkedHashSet<DimensionField> fields;
    private LinkedHashSet<DimensionField> defaultFields;
    private KeyValueStore keyValueStore;
    private SearchProvider searchProvider;

    /**
     * Constructor.
     *
     * @param apiName  API Name of the dimension
     * @param physicalName Physical name of the dimension
     * @param keyValueStore  KeyValueStore for the dimension
     * @param searchProvider  SearchProvider for the dimension
     * @param fields  Fields of the dimension
     * @param defaultFields  Default fields of the dimension
     */
    public TestDimensionConfig(
            TestApiDimensionName apiName,
            String physicalName,
            KeyValueStore keyValueStore,
            SearchProvider searchProvider,
            LinkedHashSet<DimensionField> fields,
            LinkedHashSet<DimensionField> defaultFields
    ) {
        this.apiName = apiName;
        this.physicalName = physicalName;
        this.description = apiName.asName();
        this.keyValueStore = keyValueStore;
        this.searchProvider = searchProvider;
        this.fields = fields;
        this.defaultFields = defaultFields;
        this.storageStrategy = StorageStrategy.LOADED;
    }

    /**
     * Constructor.
     *
     * @param apiName  API Name of the dimension
     * @param physicalName Physical name of the dimension
     * @param keyValueStore  KeyValueStore for the dimension
     * @param searchProvider  SearchProvider for the dimension
     * @param fields  Fields of the dimension
     * @param defaultFields  Default fields of the dimension
     * @param storageStrategy  Storage Strategy of the dimension
     */
    public TestDimensionConfig(
            TestApiDimensionName apiName,
            String physicalName,
            KeyValueStore keyValueStore,
            SearchProvider searchProvider,
            LinkedHashSet<DimensionField> fields,
            LinkedHashSet<DimensionField> defaultFields,
            StorageStrategy storageStrategy
    ) {
        this.apiName = apiName;
        this.physicalName = physicalName;
        this.description = apiName.asName();
        this.keyValueStore = keyValueStore;
        this.searchProvider = searchProvider;
        this.fields = fields;
        this.defaultFields = defaultFields;
        this.storageStrategy = storageStrategy;
    }

    //CHECKSTYLE:OFF
    public TestDimensionConfig withApiName(TestApiDimensionName apiName) {
        return new TestDimensionConfig(apiName, physicalName, keyValueStore, searchProvider, fields, defaultFields);
    }

    public TestDimensionConfig withPhysicalName(String physicalName) {
        return new TestDimensionConfig(apiName, physicalName, keyValueStore, searchProvider, fields, defaultFields);
    }

    public TestDimensionConfig withKeyValueStore(KeyValueStore keyValueStore) {
        return new TestDimensionConfig(apiName, physicalName, keyValueStore, searchProvider, fields, defaultFields);
    }

    public TestDimensionConfig withSearchProvider(SearchProvider searchProvider) {
        return new TestDimensionConfig(apiName, physicalName, keyValueStore, searchProvider, fields, defaultFields);
    }

    public TestDimensionConfig withFields(LinkedHashSet<DimensionField> fields) {
        return new TestDimensionConfig(apiName, physicalName, keyValueStore, searchProvider, fields, defaultFields);
    }

    public TestDimensionConfig withDefaultFields(LinkedHashSet<DimensionField> defaultFields) {
        return new TestDimensionConfig(apiName, physicalName, keyValueStore, searchProvider, fields, defaultFields);
    }
    //CHECKSTYLE:ON

    public TestApiDimensionName getApiNameEnum() {
        return apiName;
    }

    @Override
    public String getApiName() {
        return apiName.asName();
    }

    @Override
    public String getLongName() {
        return apiName.asName();
    }

    @Override
    public String getCategory() {
        return Dimension.DEFAULT_CATEGORY;
    }

    @Override
    public String getPhysicalName() {
        return physicalName;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public LinkedHashSet<DimensionField> getFields() {
        return fields;
    }

    @Override
    public LinkedHashSet<DimensionField> getDefaultDimensionFields() {
        return defaultFields;
    }

    @Override
    public KeyValueStore getKeyValueStore() {
        return keyValueStore;
    }

    @Override
    public SearchProvider getSearchProvider() {
        return searchProvider;
    }

    @Override
    public String toString() {
        return "Dimension Config: apiName: " + apiName + " physicalName: " + physicalName + " fields: " + fields;
    }

    @Override
    public StorageStrategy getStorageStrategy() {
        return storageStrategy;
    }
}
