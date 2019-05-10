// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.dimension;

import com.yahoo.bard.webservice.data.config.names.DimensionName;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension;
import com.yahoo.bard.webservice.data.dimension.metadata.StorageStrategy;

import java.util.LinkedHashSet;

import javax.validation.constraints.NotNull;

/**
 * A Default Key Value Store Dimension holds all of the information needed to construct a Dimension.
 */
public class DefaultKeyValueStoreDimensionConfig implements DimensionConfig {

    private final DimensionName apiName;
    private final String physicalName;
    private final String description;
    private final String longName;
    private final String category;
    private final LinkedHashSet<DimensionField> fields;
    private final LinkedHashSet<DimensionField> defaultDimensionFields;
    private final KeyValueStore keyValueStore;
    private final SearchProvider searchProvider;
    private final StorageStrategy storageStrategy;

    /**
     * Construct a DefaultKeyValueStoreDimensionConfig instance from dimension name, dimension fields and
     * default dimension fields.
     *
     * @param apiName  The API Name is the external, end-user-facing name for the dimension.
     * @param physicalName  The internal, physical name for the dimension.
     * @param description  A description of the dimension and its meaning.
     * @param longName  The Long Name is the external, end-user-facing long  name for the dimension.
     * @param category  The Category is the external, end-user-facing category for the dimension.
     * @param fields  The set of fields for this dimension.
     * @param defaultDimensionFields  The default set of fields for this dimension to be shown in the response.
     * @param keyValueStore  The key value store holding dimension row data.
     * @param searchProvider  The search provider for field value lookups on this dimension.
     */
    public DefaultKeyValueStoreDimensionConfig(
            @NotNull DimensionName apiName,
            String physicalName,
            String description,
            String longName,
            String category,
            @NotNull LinkedHashSet<DimensionField> fields,
            @NotNull LinkedHashSet<DimensionField> defaultDimensionFields,
            @NotNull KeyValueStore keyValueStore,
            @NotNull SearchProvider searchProvider
    ) {
        this.apiName = apiName;
        this.physicalName = physicalName;
        this.description = description;
        this.longName = longName;
        this.category = category;
        this.fields = fields;
        this.defaultDimensionFields = defaultDimensionFields;
        this.keyValueStore = keyValueStore;
        this.searchProvider = searchProvider;
        this.storageStrategy = StorageStrategy.LOADED;
    }

    /**
     * Construct a DefaultKeyValueStoreDimensionConfig instance from dimension name and
     * only using default dimension fields.
     *
     * @param apiName  The API Name is the external, end-user-facing name for the dimension.
     * @param physicalName  The internal, physical name for the dimension.
     * @param description  A description of the dimension and its meaning.
     * @param longName  The Long Name is the external, end-user-facing long  name for the dimension.
     * @param category  The Category is the external, end-user-facing category for the dimension.
     * @param fields  The set of fields for this dimension, this set of field will also be used for the default fields.
     * @param keyValueStore  The key value store holding dimension row data.
     * @param searchProvider  The search provider for field value lookups on this dimension.
     */
    public DefaultKeyValueStoreDimensionConfig(
            @NotNull DimensionName apiName,
            String physicalName,
            String description,
            String longName,
            String category,
            @NotNull LinkedHashSet<DimensionField> fields,
            @NotNull KeyValueStore keyValueStore,
            @NotNull SearchProvider searchProvider
    ) {
        this(
                apiName,
                physicalName,
                description,
                longName,
                category,
                fields,
                fields,
                keyValueStore,
                searchProvider
        );
    }

    /**
     * Construct a DefaultKeyValueStoreDimensionConfig instance from a dimension and physical column name.
     *
     *
     * @param dimension  The dimension whose config should be copied.
     * @param physicalName  The internal, physical name for the dimension.
     */
    public DefaultKeyValueStoreDimensionConfig(
            KeyValueStoreDimension dimension,
            String physicalName
    ) {
        this(
                (DimensionName) () -> dimension.getApiName(),
                physicalName,
                dimension.getDescription(),
                dimension.getLongName(),
                dimension.getCategory(),
                dimension.getDimensionFields(),
                dimension.getDefaultDimensionFields(),
                dimension.getKeyValueStore(),
                dimension.getSearchProvider()
        );
    }

    @Override
    public String getApiName() {
        return apiName.asName();
    }

    @Override
    public String getLongName() {
        return longName;
    }

    @Override
    public String getCategory() {
        return category;
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
        return defaultDimensionFields;
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
    public StorageStrategy getStorageStrategy() {
        return storageStrategy;
    }
}
