// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl;

import com.yahoo.bard.webservice.data.cache.HashDataCache.Pair;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.metadata.StorageStrategy;
import com.yahoo.bard.webservice.util.DimensionStoreKeyUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * KeyValueStoreDimension implements key value collection of DimensionRows.
 * <p>
 * Supports point lookups:
 * <ul>
 *     <li>Id {@literal ->} DimensionRow
 * </ul>
 */
public class KeyValueStoreDimension implements Dimension {
    private static final String MISSING_ROW_KEY_FORMAT = "Dimension row '%s' doesn't contain expected key '%s'";
    private static final String FIELD_UNDEFINED_FORMAT = "Unknown dimensionField: '%s' on dimension: '%s'.";

    private static final Logger LOG = LoggerFactory.getLogger(KeyValueStoreDimension.class);

    private final String apiName;
    private final String longName;
    private final String category;
    private final String description;

    private final LinkedHashSet<DimensionField> dimensionFields;
    private final LinkedHashSet<DimensionField> defaultDimensionFields;
    private final Map<String, DimensionField> nameToDimensionField;

    private final KeyValueStore keyValueStore;
    private final ObjectMapper objectMapper;
    private final SearchProvider searchProvider;

    private final String lastUpdatedKey;

    private final DimensionField key;

    private final boolean isAggregatable;
    private final StorageStrategy storageStrategy;

    /**
     * Constructor.
     *
     * @param dimensionName  API Name of the dimension
     * @param longName  Long API Name of the dimension
     * @param category  Category of the dimension
     * @param description  Description of the dimension
     * @param dimensionFields  Metadata fields of the dimension
     * @param keyValueStore  Metadata store for the dimension
     * @param searchProvider  Search provider over the metadata for the dimension
     * @param defaultDimensionFields  Default fields for the dimension
     * @param isAggregatable  Whether the dimension is aggregatable
     * @param storageStrategy  Strategy of how dimension is loaded. See
     * {@link com.yahoo.bard.webservice.data.dimension.metadata.StorageStrategy}
     */
    public KeyValueStoreDimension(
            String dimensionName,
            String longName,
            String category,
            String description,
            @NotNull LinkedHashSet<DimensionField> dimensionFields,
            @NotNull KeyValueStore keyValueStore,
            SearchProvider searchProvider,
            @NotNull LinkedHashSet<DimensionField> defaultDimensionFields,
            boolean isAggregatable,
            StorageStrategy storageStrategy
    ) {
        this.apiName = dimensionName;
        this.longName = longName;
        this.category = category;
        this.lastUpdatedKey = DimensionStoreKeyUtils.getLastUpdatedKey();

        this.description = description;

        this.dimensionFields = dimensionFields;
        this.defaultDimensionFields = calculateDefaultDimensionFields(dimensionFields, defaultDimensionFields);
        this.nameToDimensionField = buildNameToDimensionField(dimensionFields);

        this.keyValueStore = keyValueStore;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        this.objectMapper.registerModule(new AfterburnerModule());
        this.key = dimensionFields.isEmpty() ? null : dimensionFields.iterator().next();
        this.searchProvider = searchProvider;

        this.searchProvider.setDimension(this);
        this.searchProvider.setKeyValueStore(keyValueStore);

        this.isAggregatable = isAggregatable;
        this.storageStrategy = storageStrategy;
    }

    /**
     * Constructor.
     *
     * @param dimensionName  API Name of the dimension
     * @param longName  Long API Name of the dimension
     * @param category  Category of the dimension
     * @param description  Description of the dimension
     * @param dimensionFields  Metadata fields of the dimension
     * @param keyValueStore  Metadata store for the dimension
     * @param searchProvider  Search provider over the metadata for the dimension
     * @param defaultDimensionFields  Default fields for the dimension
     * @param isAggregatable  Whether the dimension is aggregatable
     * {@link com.yahoo.bard.webservice.data.dimension.metadata.StorageStrategy}
     */
    public KeyValueStoreDimension(
            String dimensionName,
            String longName,
            String category,
            String description,
            @NotNull LinkedHashSet<DimensionField> dimensionFields,
            @NotNull KeyValueStore keyValueStore,
            SearchProvider searchProvider,
            @NotNull LinkedHashSet<DimensionField> defaultDimensionFields,
            boolean isAggregatable
    ) {
        this(
                dimensionName,
                longName,
                category,
                description,
                dimensionFields,
                keyValueStore,
                searchProvider,
                defaultDimensionFields,
                isAggregatable,
                StorageStrategy.LOADED
        );
    }

    /**
     * Constructor.
     * <p>
     * It constructs an aggregatable dimension that defaults the Long Api Name to the Api Name and sets the Category
     * to the default category.
     *
     * @param dimensionName  API Name of the dimension
     * @param description  Description of the dimension
     * @param dimensionFields  Metadata fields of the dimension
     * @param keyValueStore  Metadata store for the dimension
     * @param searchProvider  Search provider over the metadata for the dimension
     */
    public KeyValueStoreDimension(
            String dimensionName,
            String description,
            @NotNull LinkedHashSet<DimensionField> dimensionFields,
            @NotNull KeyValueStore keyValueStore,
            SearchProvider searchProvider
    ) {
        this(
                dimensionName,
                dimensionName,
                Dimension.DEFAULT_CATEGORY,
                description,
                dimensionFields,
                keyValueStore,
                searchProvider,
                new LinkedHashSet<>(),
                true,
                StorageStrategy.LOADED
        );
    }

    /**
     * Constructor.
     * <p>
     * Defaults the Long Api Name to the Api Name and sets the Category to the default category.
     *
     * @param dimensionName  API Name of the dimension
     * @param description  Description of the dimension
     * @param dimensionFields  Metadata fields of the dimension
     * @param keyValueStore  Metadata store for the dimension
     * @param searchProvider  Search provider over the metadata for the dimension
     * @param isAggregatable  Whether the dimension is aggregatable
     */
    public KeyValueStoreDimension(
            String dimensionName,
            String description,
            @NotNull LinkedHashSet<DimensionField> dimensionFields,
            @NotNull KeyValueStore keyValueStore,
            SearchProvider searchProvider,
            boolean isAggregatable
    ) {
        this(
                dimensionName,
                dimensionName,
                Dimension.DEFAULT_CATEGORY,
                description,
                dimensionFields,
                keyValueStore,
                searchProvider,
                new LinkedHashSet<>(),
                isAggregatable,
                StorageStrategy.LOADED
        );
    }

    /**
     * Constructor.
     * <p>
     * Build an aggregatable dimension that defaults the Long Api Name to the Api Name and sets the Category
     * to the default category.
     *
     * @param dimensionName  API Name of the dimension
     * @param description  Description of the dimension
     * @param dimensionFields  Metadata fields of the dimension
     * @param keyValueStore  Metadata store for the dimension
     * @param searchProvider  Search provider over the metadata for the dimension
     * @param dimensionRows  Dimension metadata rows for this dimension
     */
    public KeyValueStoreDimension(
            String dimensionName,
            String description,
            @NotNull LinkedHashSet<DimensionField> dimensionFields,
            @NotNull KeyValueStore keyValueStore,
            SearchProvider searchProvider,
            Set<DimensionRow> dimensionRows
    ) {
        this(
                dimensionName,
                dimensionName,
                Dimension.DEFAULT_CATEGORY,
                description,
                dimensionFields,
                keyValueStore,
                searchProvider,
                dimensionRows
        );
    }

    /**
     * Constructor.
     *
     * @param dimensionName  API Name of the dimension
     * @param longName  Long API Name of the dimension
     * @param category  Category of the dimension
     * @param description  Description of the dimension
     * @param dimensionFields  Metadata fields of the dimension
     * @param keyValueStore  Metadata store for the dimension
     * @param searchProvider  Search provider over the metadata for the dimension
     * @param dimensionRows  Dimension metadata rows for this dimension
     */
    public KeyValueStoreDimension(
            String dimensionName,
            String longName,
            String category,
            String description,
            @NotNull LinkedHashSet<DimensionField> dimensionFields,
            @NotNull KeyValueStore keyValueStore,
            SearchProvider searchProvider,
            Set<DimensionRow> dimensionRows
    ) {
        this(
                dimensionName,
                longName,
                category,
                description,
                dimensionFields,
                keyValueStore,
                searchProvider,
                new LinkedHashSet<>(),
                true,
                StorageStrategy.LOADED
        );
        this.addAllDimensionRows(dimensionRows);
    }

    /**
     * Constructor.
     *
     * @param dimensionConfig  Configuration holder for this dimension
     */
    public KeyValueStoreDimension(DimensionConfig dimensionConfig) {
        this(
                dimensionConfig.getApiName(),
                dimensionConfig.getLongName(),
                dimensionConfig.getCategory(),
                dimensionConfig.getDescription(),
                dimensionConfig.getFields(),
                dimensionConfig.getKeyValueStore(),
                dimensionConfig.getSearchProvider(),
                dimensionConfig.getDefaultDimensionFields(),
                dimensionConfig.isAggregatable(),
                dimensionConfig.getStorageStrategy()
        );
    }

    /**
     * Use the 1st 2 dimension fields as the default fields if the actual default fields set is empty.
     *
     * @param dimensionFields  Fields for the dimension
     * @param defaultDimensionFields  Specified default fields for the dimension
     *
     * @return the 1st 2 dimension fields as the default fields if the actual default fields set is empty.
     */
    private LinkedHashSet<DimensionField> calculateDefaultDimensionFields(
            @NotNull LinkedHashSet<DimensionField> dimensionFields,
            @NotNull LinkedHashSet<DimensionField> defaultDimensionFields
    ) {
        if (defaultDimensionFields.isEmpty()) {
            return dimensionFields.stream()
                    .limit(2)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        } else {
            return defaultDimensionFields;
        }
    }

    /**
     * Make a map of dimension field name to dimension field for fast lookups.
     *
     * @param dimensionFields  Set of dimension fields to build the map from
     *
     * @return A Map of dimension field names to dimension fields
     */
    private static Map<String, DimensionField> buildNameToDimensionField(
            @NotNull Set<DimensionField> dimensionFields
    ) {
        Map<String, DimensionField> nameToDimensionField = new HashMap<>(dimensionFields.size());
        for (DimensionField dimensionField : dimensionFields) {
            nameToDimensionField.put(dimensionField.getName(), dimensionField);
        }
        return nameToDimensionField;
    }

    @Override
    public void setLastUpdated(DateTime lastUpdated) {
        if (lastUpdated == null) {
            keyValueStore.remove(lastUpdatedKey);
        } else {
            keyValueStore.put(lastUpdatedKey, lastUpdated.toString());
        }
    }

    @Override
    public String getApiName() {
        return apiName;
    }

    @Override
    public DateTime getLastUpdated() {
        String lastUpdatedString = keyValueStore.get(lastUpdatedKey);
        if (lastUpdatedString == null) {
            return null;
        } else {
            return DateTime.parse(lastUpdatedString);
        }
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public String getCategory() {
        return category;
    }

    @Override
    public String getLongName() {
        return longName;
    }

    @Override
    public StorageStrategy getStorageStrategy() {
        return storageStrategy;
    }

    @Override
    public int getCardinality() {
        return searchProvider.getDimensionCardinality();
    }

    @Override
    public LinkedHashSet<DimensionField> getDimensionFields() {
        return dimensionFields;
    }

    @Override
    public LinkedHashSet<DimensionField> getDefaultDimensionFields() {
        return defaultDimensionFields;
    }

    @Override
    public DimensionField getFieldByName(String name) {
        DimensionField dimensionField = nameToDimensionField.get(name);
        if (dimensionField == null) {
            throw new IllegalArgumentException(String.format(FIELD_UNDEFINED_FORMAT, name, apiName));
        }
        return dimensionField;
    }

    @Override
    public SearchProvider getSearchProvider() {
        return searchProvider;
    }

    @Override
    public void addDimensionRow(DimensionRow dimensionRow) {
        addAllDimensionRows(Collections.singleton(dimensionRow));
    }

    @Override
    public void addAllDimensionRows(Set<DimensionRow> dimensionRows) {
        Map<String, String> storeRows = new LinkedHashMap<>(dimensionRows.size());
        Map<String, Pair<DimensionRow, DimensionRow>> indexRows = new LinkedHashMap<>(dimensionRows.size());

        for (DimensionRow dimensionRow : dimensionRows) {
            try {
                if (dimensionRow.isEmpty()) {
                    LOG.warn("Ignoring attempt to add a dimension row with no data {}", dimensionRow);
                    continue;
                } else if (dimensionRow.get(getKey()) == null) {
                    LOG.warn("Attempting to add a dimension row with a null key {}", dimensionRow);
                    throw new IllegalArgumentException("Cannot add dimension with null key.");
                }

                // rowId  - key to associate a dimension row to its id
                String rowIdKey = DimensionStoreKeyUtils.getRowKey(getKey().getName(), dimensionRow.get(getKey()));

                // check if the dimension row already exists in store
                DimensionRow dimensionRowOld = null;
                String row = keyValueStore.get(rowIdKey);
                if (row != null) {
                    LinkedHashMap<String, String> fieldNameValueMap = objectMapper.readValue(
                            row,
                            new TypeReference<LinkedHashMap<String, String>>() { }
                    );
                    dimensionRowOld = parseDimensionRow(fieldNameValueMap);
                    if (dimensionRow.equals(dimensionRowOld)) {
                        continue;
                    }
                }

                String dimRowSerialized = objectMapper.writeValueAsString(dimensionRow);
                storeRows.put(rowIdKey, dimRowSerialized);

                //update indexes
                indexRows.put(rowIdKey, new Pair<>(dimensionRow, dimensionRowOld));

            } catch (IOException e) {
                LOG.error("Exception while adding dimension row {}", e);
                throw new RuntimeException(e);
            }
        }

        keyValueStore.putAll(storeRows);
        searchProvider.refreshIndex(indexRows);
    }

    @Override
    public DimensionRow createEmptyDimensionRow(String keyFieldValue) {
        if (findDimensionRowByKeyValue(keyFieldValue) != null) {
            String error = String.format(
                    "Dimension row with keyFieldValue '%s' already exists for dimension '%s'",
                    keyFieldValue,
                    this
            );
            throw new IllegalArgumentException(error);
        }
        Map<DimensionField, String> dimensionFieldValueMap = new HashMap<>();
        for (DimensionField dimensionField : getDimensionFields()) {
            dimensionFieldValueMap.put(dimensionField, "");
        }
        dimensionFieldValueMap.put(getKey(), keyFieldValue);
        return new DimensionRow(key, dimensionFieldValueMap);
    }

    @Override
    public DimensionRow findDimensionRowByKeyValue(String value) {
        /*
         * Key to fetch row from keyValueStore
         * eg: if key column is ID and value is 12345
         * rowKey would be id_12345_row_key
         */
        String rowKey = DimensionStoreKeyUtils.getRowKey(getKey().getName(), value);
        DimensionRow drByKey = null;
        try {
            String dimRowJson = keyValueStore.get(rowKey);
            if (dimRowJson != null) {
                Map<String, String> dimensionRowMap = objectMapper.readValue(
                        dimRowJson,
                        new TypeReference<LinkedHashMap<String, String>>() { }
                );
                drByKey = parseDimensionRow(dimensionRowMap);
            }
        } catch (IOException e) {
            LOG.error("Cannot map string to DimensionRow object. {}", e);
            throw new RuntimeException(e);
        }
        return drByKey;
    }


    /**
     * Internal method for cleaning the dimension rows.
     */
    public void deleteAllDimensionRows() {
        try {
            String dimRowIndexes = keyValueStore.get(DimensionStoreKeyUtils.getAllValuesKey());
            if (dimRowIndexes == null) {
                return;
            }

            String[] keys = objectMapper.readValue(dimRowIndexes, String[].class);

            LinkedHashSet<String> dimRowKeys = new LinkedHashSet<>(Arrays.asList(keys));

            for (String dimRowKey : dimRowKeys) {
                keyValueStore.remove(dimRowKey);
            }
            searchProvider.setKeyValueStore(keyValueStore);

            // Reset cardinality to 0
            keyValueStore.put(DimensionStoreKeyUtils.getCardinalityKey(), "0");

            // Reset list to empty
            String allValuesKey = DimensionStoreKeyUtils.getAllValuesKey();
            keyValueStore.put(allValuesKey, "[]");

        } catch (IOException e) {
            LOG.error("Exception while reading dimension rows {}", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public DimensionField getKey() {
        return key;
    }

    @Override
    public DimensionRow parseDimensionRow(Map<String, String> fieldNameValueMap) {
        // TODO: This rewrite need to be removed once description is normalized in legacy implementations
        String desc = fieldNameValueMap.remove("description");
        if (desc != null) {
            fieldNameValueMap.put("desc", desc);
        }

        LinkedHashMap<DimensionField, String> dimensionRowFieldValues = new LinkedHashMap<>(fieldNameValueMap.size());
        // Load every field we expect and only fields we expect
        for (DimensionField field : dimensionFields) {
            String fieldName = field.getName();
            String value = fieldNameValueMap.get(fieldName);
            if (value == null) {
                // A missing key value is unacceptable
                if (field == getKey()) {
                    String error = String.format(MISSING_ROW_KEY_FORMAT, fieldNameValueMap.toString(), fieldName);
                    LOG.info(error);
                    throw new IllegalArgumentException(error);
                }
                // A missing value for another field is turned into the empty string
                value = "";
            }
            dimensionRowFieldValues.put(field, value);
        }
        return new DimensionRow(getKey(), dimensionRowFieldValues);
    }

    @Override
    public boolean isAggregatable() {
        return isAggregatable;
    }

    /**
     * Constructs a new KeyValueStoreDimension with specified
     * {@link com.yahoo.bard.webservice.data.dimension.metadata.StorageStrategy}.
     *
     * @param storageStrategy  The specified StorageStrategy
     *
     * @return the new KeyValueStoreDimension with the specified StorageStrategy
     */
    public KeyValueStoreDimension withStorageStrategy(StorageStrategy storageStrategy) {
        return new KeyValueStoreDimension(
                apiName,
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
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (!(o instanceof KeyValueStoreDimension)) { return false; }

        KeyValueStoreDimension that = (KeyValueStoreDimension) o;

        return
                Objects.equals(apiName, that.apiName) &&
                Objects.equals(description, that.description) &&
                Objects.equals(dimensionFields, that.dimensionFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(apiName, description, dimensionFields);
    }

    @Override
    public String toString() {
        return apiName + ":" +
                description + ":" +
                isAggregatable + ":" +
                (getLastUpdated() == null ? "" : getLastUpdated());
    }
}
