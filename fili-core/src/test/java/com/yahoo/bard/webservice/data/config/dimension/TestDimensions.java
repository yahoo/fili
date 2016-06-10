// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
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
import com.yahoo.bard.webservice.data.dimension.RedisStore;
import com.yahoo.bard.webservice.data.dimension.RedisStoreManager;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.LuceneSearchProviderManager;
import com.yahoo.bard.webservice.data.dimension.impl.NoOpSearchProviderManager;
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProvider;
import com.yahoo.bard.webservice.util.EnumUtils;
import com.yahoo.bard.webservice.util.Utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Bard test dimension configuration
 */
public class TestDimensions {

    public LinkedHashMap<TestApiDimensionName, DimensionConfig> dimensionNameConfigs;
    public Map<TestLogicalTableName, Set<TestApiDimensionName>> tableDimensions;

    public TestDimensions() {
        dimensionNameConfigs = new LinkedHashMap<>();
        dimensionNameConfigs.put(SIZE, TestDimensions.buildStandardDimensionConfig(SIZE));
        dimensionNameConfigs.put(SHAPE, TestDimensions.buildStandardDimensionConfig(SHAPE));
        dimensionNameConfigs.put(COLOR, TestDimensions.buildStandardDimensionConfig(COLOR));
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

        dimensionNameConfigs.put(BREED, TestDimensions.buildStandardDimensionConfig(BREED));
        dimensionNameConfigs.put(SPECIES, TestDimensions.buildStandardDimensionConfig(SPECIES));
        dimensionNameConfigs.put(SEX, TestDimensions.buildStandardDimensionConfig(SEX));

        tableDimensions = new HashMap<>();
        tableDimensions.put(SHAPES, Utils.asLinkedHashSet(SIZE, SHAPE, COLOR, OTHER));
        tableDimensions.put(PETS, Utils.asLinkedHashSet(BREED, SPECIES, SEX));
    }

    private static TestDimensionConfig buildStandardDimensionConfig(TestApiDimensionName dimensionName) {
        return new TestDimensionConfig(
                dimensionName,
                dimensionName.asName(),
                getDefaultKeyValueStore(dimensionName),
                getDefaultSearchProvider(),
                getDefaultFields(),
                getDefaultFields()
        );
    }

    private static TestDimensionConfig buildStandardLuceneDimensionConfig(TestApiDimensionName dimensionName) {
        return new TestDimensionConfig(
                dimensionName,
                dimensionName.asName(),
                getDefaultKeyValueStore(dimensionName),
                LuceneSearchProviderManager.getInstance(dimensionName.asName()),
                getDefaultFields(),
                getDefaultFields()
        );
    }

    private static KeyValueStore getDefaultKeyValueStore(TestApiDimensionName storeName) {
        switch (DimensionBackend.getBackend()) {
            case REDIS:
                RedisStore store = RedisStoreManager.getInstance(storeName.asName());
                // Key/values stored in Redis persist between tests, so remove
                // them to give the test a clean environment.
                store.removeAllKeys();
                return store;

            case MEMORY:
            default:
                return MapStoreManager.getInstance(storeName.asName());
        }
    }

    private static SearchProvider getDefaultSearchProvider() {
        return new ScanSearchProvider();
    }

    private static LinkedHashSet<DimensionField> getDefaultFields() {
        LinkedHashSet<DimensionField> fields = new LinkedHashSet<>();
        fields.add(TestDimensionField.ID);
        fields.add(TestDimensionField.DESC);
        return fields;
    }

    public LinkedHashSet<DimensionConfig> getAllDimensionConfigurations() {
        return new LinkedHashSet<>(dimensionNameConfigs.values());
    }

    public LinkedHashSet<DimensionConfig> getDimensionConfigurationsByApiName(
            TestApiDimensionName... apiDimensionNames
    ) {
        LinkedHashSet<DimensionConfig> matches = new LinkedHashSet<>();
        for (TestApiDimensionName testApiDimensionName : apiDimensionNames) {
            matches.add(dimensionNameConfigs.get(testApiDimensionName));
        }
        return matches;
    }

    enum TestDimensionField implements DimensionField {
        ID("Dimension ID"),
        DESC("Dimension Description"),
        RED_PIGMENT("Red pigment"),
        BLUE_PIGMENT("Blue pigment"),
        GREEN_PIGMENT("Green pigment");

        private String description;
        private String camelName;

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
