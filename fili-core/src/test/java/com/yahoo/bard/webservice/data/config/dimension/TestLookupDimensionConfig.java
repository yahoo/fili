// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.dimension;

import com.yahoo.bard.webservice.data.config.names.TestApiDimensionName;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;

import javax.validation.constraints.NotNull;

/**
 * Hold all of the Configuration information for the look up dimensions.
 */
public class TestLookupDimensionConfig extends TestDimensionConfig implements LookupDimensionConfig {
    private List<String> namespaces;

    /**
     * Constructor.
     *
     * @param dimensionConfig Configuration properties for dimensions
     * @param namespaces List of namespaces used for Lookup
     */
    public TestLookupDimensionConfig(@NotNull DimensionConfig dimensionConfig, List<String> namespaces) {
        this(
                TestApiDimensionName.valueOf(dimensionConfig.getApiName().toUpperCase(Locale.ENGLISH)),
                dimensionConfig.getPhysicalName(),
                dimensionConfig.getKeyValueStore(),
                dimensionConfig.getSearchProvider(),
                dimensionConfig.getFields(),
                dimensionConfig.getDefaultDimensionFields(),
                namespaces
        );
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
     * @param namespaces List of namespaces used for Lookup
     */
    public TestLookupDimensionConfig(
            TestApiDimensionName apiName,
            String physicalName,
            KeyValueStore keyValueStore,
            SearchProvider searchProvider,
            LinkedHashSet<DimensionField> fields,
            LinkedHashSet<DimensionField> defaultFields,
            List<String> namespaces
    ) {
        super(apiName, physicalName, keyValueStore, searchProvider, fields, defaultFields);
        this.namespaces = namespaces;
    }

    @Override
    public List<String> getNamespaces() {
        return this.namespaces;
    }

    // CHECKSTYLE:OFF
    @Override
    public TestLookupDimensionConfig withApiName(TestApiDimensionName apiName) {
        return new TestLookupDimensionConfig(apiName, getPhysicalName(), getKeyValueStore(), getSearchProvider(), getFields(), getDefaultDimensionFields(), namespaces);
    }

    @Override
    public TestLookupDimensionConfig withPhysicalName(String physicalName) {
        return new TestLookupDimensionConfig(getApiNameEnum(), physicalName, getKeyValueStore(), getSearchProvider(), getFields(), getDefaultDimensionFields(), namespaces);
    }

    @Override
    public TestLookupDimensionConfig withKeyValueStore(KeyValueStore keyValueStore) {
        return new TestLookupDimensionConfig(getApiNameEnum(), getPhysicalName(), keyValueStore, getSearchProvider(), getFields(), getDefaultDimensionFields(), namespaces);
    }

    @Override
    public TestLookupDimensionConfig withSearchProvider(SearchProvider searchProvider) {
        return new TestLookupDimensionConfig(getApiNameEnum(), getPhysicalName(), getKeyValueStore(), searchProvider, getFields(), getDefaultDimensionFields(), namespaces);
    }

    @Override
    public TestLookupDimensionConfig withFields(LinkedHashSet<DimensionField> fields) {
        return new TestLookupDimensionConfig(getApiNameEnum(), getPhysicalName(), getKeyValueStore(), getSearchProvider(), fields, getDefaultDimensionFields(), namespaces);
    }

    @Override
    public TestLookupDimensionConfig withDefaultFields(LinkedHashSet<DimensionField> defaultFields) {
        return new TestLookupDimensionConfig(getApiNameEnum(), getPhysicalName(), getKeyValueStore(), getSearchProvider(), getFields(), defaultFields, namespaces);
    }

    public TestLookupDimensionConfig withNamespaces(List<String> namespaces) {
        return new TestLookupDimensionConfig(getApiNameEnum(), getPhysicalName(), getKeyValueStore(), getSearchProvider(), getFields(), getDefaultDimensionFields(), namespaces);
    }
    // CHECKSTYLE:ON
}
