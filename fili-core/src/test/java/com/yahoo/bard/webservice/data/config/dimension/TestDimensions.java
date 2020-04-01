// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.dimension;

import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.BREED;
import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.COLOR;
import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.MODEL;
import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.OTHER;
import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.SEX;
import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.SHAPE;
import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.SIZE;
import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.SPECIES;
import static com.yahoo.bard.webservice.data.config.names.TestLogicalTableName.PETS;
import static com.yahoo.bard.webservice.data.config.names.TestLogicalTableName.SHAPES;

import com.yahoo.bard.webservice.data.config.names.TestApiDimensionName;
import com.yahoo.bard.webservice.data.config.names.TestLogicalTableName;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.MapStoreManager;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.LuceneSearchProviderManager;
import com.yahoo.bard.webservice.data.dimension.impl.NoOpSearchProviderManager;
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProvider;
import com.yahoo.bard.webservice.data.dimension.metadata.StorageStrategy;
import com.yahoo.bard.webservice.util.EnumUtils;
import com.yahoo.bard.webservice.util.Utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Bard test dimension configuration.
 */
public class TestDimensions {

    public LinkedHashMap<TestApiDimensionName, DimensionConfig> dimensionNameConfigs;
    public Map<TestLogicalTableName, Set<TestApiDimensionName>> tableDimensions;

    /**
     * Constructor.
     */
    public TestDimensions() {
        dimensionNameConfigs = new LinkedHashMap<>();
        dimensionNameConfigs.put(SIZE, TestDimensions.buildStandardDimensionConfig(SIZE));
        dimensionNameConfigs.put(SHAPE, TestDimensions.buildStandardDimensionConfig(SHAPE));
        dimensionNameConfigs.put(COLOR, TestDimensions.buildNonLoadedDimensionConfig(COLOR));
        dimensionNameConfigs.put(OTHER,
            new TestDimensionConfig(
                OTHER,
                "misc",
                getDefaultKeyValueStore(OTHER),
                NoOpSearchProviderManager.getInstance(OTHER.asName()),
                getDefaultFields(),
                getDefaultFields()
                )
        );

        DimensionConfig color = dimensionNameConfigs.get(COLOR);
        Collections.addAll(
                color.getFields(),
                TestDimensionField.BLUE_PIGMENT,
                TestDimensionField.RED_PIGMENT,
                TestDimensionField.GREEN_PIGMENT
        );

        dimensionNameConfigs.put(MODEL, TestDimensions.buildStandardLuceneDimensionConfig(MODEL));

        dimensionNameConfigs.put(
                BREED,
                TestDimensions.buildStandardLookupDimensionConfig(BREED, Arrays.asList("NAMESPACE1", "NAMESPACE2"))
        );
        dimensionNameConfigs.put(
                SPECIES,
                TestDimensions.buildStandardLookupDimensionConfig(SPECIES, Arrays.asList("NAMESPACE1"))
                        .withPhysicalName("class")
        );
        dimensionNameConfigs.put(SEX,
                TestDimensions.buildStandardLookupDimensionConfig(SEX, Collections.emptyList()));

        tableDimensions = new HashMap<>();
        tableDimensions.put(SHAPES, Utils.asLinkedHashSet(SIZE, SHAPE, COLOR, OTHER));
        tableDimensions.put(PETS, Utils.asLinkedHashSet(BREED, SPECIES, SEX));
    }

    /**
     * Build a standard-config dimension config.
     *
     * @param dimensionName  Name of the dimension to build
     *
     * @return the standard dimension config
     */
    protected static TestDimensionConfig buildStandardDimensionConfig(TestApiDimensionName dimensionName) {
        return new TestDimensionConfig(
                dimensionName,
                dimensionName.asName(),
                getDefaultKeyValueStore(dimensionName),
                getDefaultSearchProvider(),
                getDefaultFields(),
                getDefaultFields()
        );
    }

    /**
     * Build a non-loaded dimension config.
     *
     * @param dimensionName  Name of the dimension to build
     *
     * @return the standard dimension config
     */
    protected static TestDimensionConfig buildNonLoadedDimensionConfig(TestApiDimensionName dimensionName) {
        return new TestDimensionConfig(
                dimensionName,
                dimensionName.asName(),
                getDefaultKeyValueStore(dimensionName),
                getDefaultSearchProvider(),
                getDefaultFields(),
                getDefaultFields(),
                StorageStrategy.NONE
        );
    }

    /**
     * Build a standard-config Lucene-backed dimension config.
     *
     * @param dimensionName  Name of the dimension to build
     *
     * @return the standard lucene-backed dimension config
     */
    public static TestDimensionConfig buildStandardLuceneDimensionConfig(TestApiDimensionName dimensionName) {
        return new TestDimensionConfig(
                dimensionName,
                dimensionName.asName(),
                getDefaultKeyValueStore(dimensionName),
                LuceneSearchProviderManager.getInstance(dimensionName.asName()),
                getDefaultFields(),
                getDefaultFields()
        );
    }

    /**
     * Builds a lookup dimension config with a namespace.
     *
     * @param  dimensionName  Name of the dimension to build.
     * @param  namespaces  namespaces for lookup.
     *
     * @return  the lookup dimension config
     */
    public static TestLookupDimensionConfig buildStandardLookupDimensionConfig(
            TestApiDimensionName dimensionName,
            List<String> namespaces
    ) {
        return new TestLookupDimensionConfig(buildStandardDimensionConfig(dimensionName), namespaces);
    }

    /**
     * Get the default KeyValueStore for the given store name.
     *
     * @param storeName  The store name for which to get the default key value store
     *
     * @return the default KeyValueStore
     */
    public static KeyValueStore getDefaultKeyValueStore(TestApiDimensionName storeName) {
        switch (DimensionBackend.getBackend()) {
            case MEMORY:
            default:
                return MapStoreManager.getInstance(storeName.asName());
        }
    }

    /**
     * Get the default search provider for the test dimensions.
     *
     * @return the default search provider
     */
    public static SearchProvider getDefaultSearchProvider() {
        return new ScanSearchProvider();
    }

    /**
     * Get the default fields for the test dimensions.
     *
     * @return the default fields
     */
    private static LinkedHashSet<DimensionField> getDefaultFields() {
        LinkedHashSet<DimensionField> fields = new LinkedHashSet<>();
        fields.add(TestDimensionField.ID);
        fields.add(TestDimensionField.DESC);
        return fields;
    }

    /**
     * Get all of the dimension configurations.
     *
     * @return all of the dimension configurations
     */
    public LinkedHashSet<DimensionConfig> getAllDimensionConfigurations() {
        return new LinkedHashSet<>(dimensionNameConfigs.values());
    }

    /**
     * Get the dimension configurations by their API names.
     *
     * @param apiDimensionNames  The set of API names for which to retrieve dimensions.
     *
     * @return the dimensions with those API names
     */
    public LinkedHashSet<DimensionConfig> getDimensionConfigurationsByApiName(
            TestApiDimensionName... apiDimensionNames
    ) {
        return Arrays.stream(apiDimensionNames)
                .map(dimensionNameConfigs::get)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Fields for the test dimensions.
     */
    enum TestDimensionField implements DimensionField {
        ID("Dimension ID"),
        DESC("Dimension Description"),
        RED_PIGMENT("Red pigment"),
        BLUE_PIGMENT("Blue pigment"),
        GREEN_PIGMENT("Green pigment");

        private String description;
        private String camelName;

        /**
         * Constructor.
         *
         * @param description  Description of the dimension field
         */
        TestDimensionField(String description) {
            this.description = description;
            this.camelName = EnumUtils.camelCase(name());
        }

        @Override
        public String getName() {
            return camelName;
        }

        @Override
        public String getDescription() {
            return description;
        }

        @Override
        public String toString() {
            return camelName;
        }
    }
}
