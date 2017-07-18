// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.data.config.dimension;

import com.yahoo.fili.webservice.data.config.names.TestApiDimensionName;
import com.yahoo.fili.webservice.data.dimension.DimensionField;
import com.yahoo.fili.webservice.data.dimension.KeyValueStore;
import com.yahoo.fili.webservice.data.dimension.SearchProvider;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;

import javax.validation.constraints.NotNull;

/**
 * Hold all of the Configuration information for the look up dimensions.
 */
public class TestRegisteredLookupDimensionConfig extends TestDimensionConfig
        implements RegisteredLookupDimensionConfig {
    private List<String> lookups;

    /**
     * Constructor.
     *
     * @param dimensionConfig Configuration properties for dimensions
     * @param lookups List of lookups used for Lookup
     */
    public TestRegisteredLookupDimensionConfig(@NotNull DimensionConfig dimensionConfig, List<String> lookups) {
        this(
                TestApiDimensionName.valueOf(dimensionConfig.getApiName().toUpperCase(Locale.ENGLISH)),
                dimensionConfig.getPhysicalName(),
                dimensionConfig.getKeyValueStore(),
                dimensionConfig.getSearchProvider(),
                dimensionConfig.getFields(),
                dimensionConfig.getDefaultDimensionFields(),
                lookups
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
     * @param lookups List of lookups
     */
    public TestRegisteredLookupDimensionConfig(
            TestApiDimensionName apiName,
            String physicalName,
            KeyValueStore keyValueStore,
            SearchProvider searchProvider,
            LinkedHashSet<DimensionField> fields,
            LinkedHashSet<DimensionField> defaultFields,
            List<String> lookups
    ) {
        super(apiName, physicalName, keyValueStore, searchProvider, fields, defaultFields);
        this.lookups = lookups;
    }

    @Override
    public List<String> getLookups() {
        return this.lookups;
    }

    //CHECKSTYLE:OFF
    @Override
    public TestRegisteredLookupDimensionConfig withApiName(TestApiDimensionName apiName) {
        return new TestRegisteredLookupDimensionConfig(apiName, getPhysicalName(), getKeyValueStore(), getSearchProvider(), getFields(), getDefaultDimensionFields(),
                lookups
        );
    }

    @Override
    public TestRegisteredLookupDimensionConfig withPhysicalName(String physicalName) {
        return new TestRegisteredLookupDimensionConfig(getApiNameEnum(), physicalName, getKeyValueStore(), getSearchProvider(), getFields(), getDefaultDimensionFields(),
                lookups
        );
    }

    @Override
    public TestRegisteredLookupDimensionConfig withKeyValueStore(KeyValueStore keyValueStore) {
        return new TestRegisteredLookupDimensionConfig(getApiNameEnum(), getPhysicalName(), keyValueStore, getSearchProvider(), getFields(), getDefaultDimensionFields(),
                lookups
        );
    }

    @Override
    public TestRegisteredLookupDimensionConfig withSearchProvider(SearchProvider searchProvider) {
        return new TestRegisteredLookupDimensionConfig(getApiNameEnum(), getPhysicalName(), getKeyValueStore(), searchProvider, getFields(), getDefaultDimensionFields(),
                lookups
        );
    }

    @Override
    public TestRegisteredLookupDimensionConfig withFields(LinkedHashSet<DimensionField> fields) {
        return new TestRegisteredLookupDimensionConfig(getApiNameEnum(), getPhysicalName(), getKeyValueStore(), getSearchProvider(), fields, getDefaultDimensionFields(),
                lookups
        );
    }

    @Override
    public TestRegisteredLookupDimensionConfig withDefaultFields(LinkedHashSet<DimensionField> defaultFields) {
        return new TestRegisteredLookupDimensionConfig(getApiNameEnum(), getPhysicalName(), getKeyValueStore(), getSearchProvider(), getFields(), defaultFields,
                lookups
        );
    }

    public TestRegisteredLookupDimensionConfig withNamespaces(List<String> namespaces) {
        return new TestRegisteredLookupDimensionConfig(getApiNameEnum(), getPhysicalName(), getKeyValueStore(), getSearchProvider(), getFields(), getDefaultDimensionFields(), namespaces);
    }
    //CHECKSTYLE:ON
}
