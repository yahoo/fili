// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.dimension;

import com.yahoo.bard.webservice.data.config.names.TestApiDimensionName;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.ExtractionFunction;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;

import javax.validation.constraints.NotNull;

/**
 * Hold all of the Configuration information for the look up dimensions.
 */
public class TestRegisteredLookupDimensionConfig extends TestDimensionConfig
        implements RegisteredLookupDimensionConfig {

    private final List<ExtractionFunction> registeredLookupExtractionFns;

    /**
     * Constructor.
     *
     * @param dimensionConfig Configuration properties for dimensions
     * @param registeredLookupExtractionFns  A list of registered lookup extraction functions used to perform lookups.
     */
    public TestRegisteredLookupDimensionConfig(
            @NotNull DimensionConfig dimensionConfig,
            List<ExtractionFunction> registeredLookupExtractionFns
    ) {
        this(
                TestApiDimensionName.valueOf(dimensionConfig.getApiName().toUpperCase(Locale.ENGLISH)),
                dimensionConfig.getPhysicalName(),
                dimensionConfig.getKeyValueStore(),
                dimensionConfig.getSearchProvider(),
                dimensionConfig.getFields(),
                dimensionConfig.getDefaultDimensionFields(),
                registeredLookupExtractionFns
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
     * @param registeredLookupExtractionFns  A list of registered lookup extraction functions used to perform lookups.
     */
    public TestRegisteredLookupDimensionConfig(
            TestApiDimensionName apiName,
            String physicalName,
            KeyValueStore keyValueStore,
            SearchProvider searchProvider,
            LinkedHashSet<DimensionField> fields,
            LinkedHashSet<DimensionField> defaultFields,
            List<ExtractionFunction> registeredLookupExtractionFns
    ) {
        super(apiName, physicalName, keyValueStore, searchProvider, fields, defaultFields);
        this.registeredLookupExtractionFns = Collections.unmodifiableList(registeredLookupExtractionFns);
    }

    @Override
    public List<ExtractionFunction> getRegisteredLookupExtractionFns() {
        return this.registeredLookupExtractionFns;
    }

    //CHECKSTYLE:OFF
    @Override
    public TestRegisteredLookupDimensionConfig withApiName(TestApiDimensionName apiName) {
        return new TestRegisteredLookupDimensionConfig(
                apiName,
                getPhysicalName(),
                getKeyValueStore(),
                getSearchProvider(),
                getFields(),
                getDefaultDimensionFields(),
                getRegisteredLookupExtractionFns()
        );
    }

    @Override
    public TestRegisteredLookupDimensionConfig withPhysicalName(String physicalName) {
        return new TestRegisteredLookupDimensionConfig(
                getApiNameEnum(),
                physicalName,
                getKeyValueStore(),
                getSearchProvider(),
                getFields(),
                getDefaultDimensionFields(),
                getRegisteredLookupExtractionFns()
        );
    }

    @Override
    public TestRegisteredLookupDimensionConfig withKeyValueStore(KeyValueStore keyValueStore) {
        return new TestRegisteredLookupDimensionConfig(
                getApiNameEnum(),
                getPhysicalName(),
                keyValueStore,
                getSearchProvider(),
                getFields(),
                getDefaultDimensionFields(),
                getRegisteredLookupExtractionFns()
        );
    }

    @Override
    public TestRegisteredLookupDimensionConfig withSearchProvider(SearchProvider searchProvider) {
        return new TestRegisteredLookupDimensionConfig(
                getApiNameEnum(),
                getPhysicalName(),
                getKeyValueStore(),
                searchProvider,
                getFields(),
                getDefaultDimensionFields(),
                getRegisteredLookupExtractionFns()
        );
    }

    @Override
    public TestRegisteredLookupDimensionConfig withFields(LinkedHashSet<DimensionField> fields) {
        return new TestRegisteredLookupDimensionConfig(
                getApiNameEnum(),
                getPhysicalName(),
                getKeyValueStore(),
                getSearchProvider(),
                fields,
                getDefaultDimensionFields(),
                getRegisteredLookupExtractionFns()
        );
    }

    @Override
    public TestRegisteredLookupDimensionConfig withDefaultFields(LinkedHashSet<DimensionField> defaultFields) {
        return new TestRegisteredLookupDimensionConfig(
                getApiNameEnum(),
                getPhysicalName(),
                getKeyValueStore(),
                getSearchProvider(),
                getFields(),
                defaultFields,
                getRegisteredLookupExtractionFns()
        );
    }

    public TestRegisteredLookupDimensionConfig withRegisteredLookupExtractionFns(
            List<ExtractionFunction> registeredLookupExtractionFns
    ) {
        return new TestRegisteredLookupDimensionConfig(
                getApiNameEnum(),
                getPhysicalName(),
                getKeyValueStore(),
                getSearchProvider(),
                getFields(),
                getDefaultDimensionFields(),
                registeredLookupExtractionFns
        );
    }
    //CHECKSTYLE:ON
}
