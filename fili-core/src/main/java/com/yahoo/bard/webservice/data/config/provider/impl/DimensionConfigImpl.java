// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.provider.impl;

import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;

import java.util.LinkedHashSet;
import java.util.Objects;

/**
 * Dimension configuration.
 */
public class DimensionConfigImpl implements DimensionConfig {

    protected final String apiName;
    protected final String longName;
    protected final String category;
    protected final String physicalName;
    protected final String description;
    protected final LinkedHashSet<DimensionField> fields;
    protected final LinkedHashSet<DimensionField> defaultDimensionFields;
    protected final KeyValueStore keyValueStore;
    protected final SearchProvider searchProvider;
    protected final boolean aggregatable;
    protected final Class type;

    /**
     * Dimension configuration.
     *
     * @param apiName the api name
     * @param longName the long name
     * @param category the category
     * @param physicalName the physical name
     * @param description the description
     * @param fields the dimension fields
     * @param defaultDimensionFields the default dimension fields
     * @param keyValueStore the key value store
     * @param searchProvider the search provider
     * @param aggregatable true if aggregatable
     * @param type the class
     */
    public DimensionConfigImpl(
            final String apiName,
            final String longName,
            final String category,
            final String physicalName,
            final String description,
            final LinkedHashSet<DimensionField> fields,
            final LinkedHashSet<DimensionField> defaultDimensionFields,
            final KeyValueStore keyValueStore,
            final SearchProvider searchProvider,
            final boolean aggregatable,
            final Class type
    ) {
        this.apiName = apiName;
        this.longName = longName;
        this.category = category;
        this.physicalName = physicalName;
        this.description = description;
        this.fields = fields;
        this.defaultDimensionFields = defaultDimensionFields;
        this.keyValueStore = keyValueStore;
        this.searchProvider = searchProvider;
        this.aggregatable = aggregatable;
        this.type = type;
    }

    @Override
    public String getApiName() {
        return apiName;
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
    public boolean isAggregatable() {
        return aggregatable;
    }

    @Override
    public Class getType() {
        return type;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        final DimensionConfigImpl that = (DimensionConfigImpl) o;
        return aggregatable == that.aggregatable &&
                Objects.equals(apiName, that.apiName) &&
                Objects.equals(longName, that.longName) &&
                Objects.equals(category, that.category) &&
                Objects.equals(physicalName, that.physicalName) &&
                Objects.equals(description, that.description) &&
                Objects.equals(fields, that.fields) &&
                Objects.equals(defaultDimensionFields, that.defaultDimensionFields) &&
                Objects.equals(keyValueStore, that.keyValueStore) &&
                Objects.equals(searchProvider, that.searchProvider) &&
                Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                apiName,
                longName,
                category,
                physicalName,
                description,
                fields,
                defaultDimensionFields,
                keyValueStore,
                searchProvider,
                aggregatable,
                type
        );
    }
}
