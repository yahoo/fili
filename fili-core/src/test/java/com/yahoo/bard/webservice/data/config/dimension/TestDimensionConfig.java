// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.dimension;

import com.yahoo.bard.webservice.data.config.names.TestApiDimensionName;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;

import java.util.Arrays;
import java.util.LinkedHashSet;

/**
 * Hold all of the Configuration information for the dimensions.
 */
public class TestDimensionConfig implements DimensionConfig {
    private final TestApiDimensionName apiName;
    private final String druidName;
    private final String description;

    private LinkedHashSet<DimensionField> fields;
    private LinkedHashSet<DimensionField> defaultFields;
    private KeyValueStore keyValueStore;
    private SearchProvider searchProvider;

    public TestDimensionConfig(
            TestApiDimensionName apiName,
            String druidName,
            KeyValueStore keyValueStore,
            SearchProvider searchProvider,
            LinkedHashSet<DimensionField> fields,
            LinkedHashSet<DimensionField> defaultFields
    ) {
        this.apiName = apiName;
        this.druidName = druidName;
        this.description = apiName.asName();
        this.keyValueStore = keyValueStore;
        this.searchProvider = searchProvider;
        this.fields = fields;
        this.defaultFields = defaultFields;
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
    public String getDruidName() {
        return druidName;
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

    public void addFields(DimensionField... moreFields) {
        fields.addAll(Arrays.asList(moreFields));
    }

    @Override
    public String toString() {
        return "Dimension Config: apiName: " + apiName + " druidName: " + druidName + " fields: " + fields;
    }
}
