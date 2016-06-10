// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl;

import com.yahoo.bard.webservice.data.cache.HashDataCache.Pair;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.dimension.FilterDimensionRows;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.util.DimensionStoreKeyUtils;
import com.yahoo.bard.webservice.web.ApiFilter;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Search provider using scan on dimension rows
 */
public class ScanSearchProvider implements SearchProvider, FilterDimensionRows {
    private static final Logger LOG = LoggerFactory.getLogger(ScanSearchProvider.class);

    private final ObjectMapper objectMapper;

    private KeyValueStore keyValueStore;
    private Dimension dimension;

    public ScanSearchProvider() {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void setDimension(Dimension dimension) {
        this.dimension = dimension;
    }

    @Override
    public void setKeyValueStore(KeyValueStore keyValueStore) {
        this.keyValueStore = keyValueStore;

        // Check initialization for the cardinality in a keyValueStore
        if (keyValueStore.get(DimensionStoreKeyUtils.getCardinalityKey()) == null) {
            keyValueStore.put(DimensionStoreKeyUtils.getCardinalityKey(), "0");
        }

        //Check Dimension value keys availability
        String allValuesKey = DimensionStoreKeyUtils.getAllValuesKey();
        if (keyValueStore.get(allValuesKey) == null) {
            keyValueStore.put(allValuesKey, "[]");
        }

    }

    @Override
    public int getDimensionCardinality() {
        return Integer.parseInt(keyValueStore.get(DimensionStoreKeyUtils.getCardinalityKey()));
    }

    /**
     * Get a dimension row given an id
     * @param value  dimension id to look up
     * @return dimension row if exists else null
     */
    public DimensionRow findDimensionRowByKeyValue(String value) {
        return dimension.findDimensionRowByKeyValue(value);
    }

    /**
     * Getter for dimension rows
     * @return set of dimension rows
     */
    @Override
    public LinkedHashSet<DimensionRow> findAllDimensionRows() {
        LinkedHashSet<DimensionRow> dimensionRows = new LinkedHashSet<>();
        try {
            String dimRowIndexes = keyValueStore.get(DimensionStoreKeyUtils.getAllValuesKey());
            String[] keys = objectMapper.readValue(dimRowIndexes, String[].class);

            LinkedHashSet<String> dimRowKeys = new LinkedHashSet<>(Arrays.asList(keys));

            for (String dimRowKey : dimRowKeys) {
                String dimRowJson = keyValueStore.get(dimRowKey);
                if (dimRowJson != null) {
                    Map<String, String> dimensionRowMap = objectMapper.readValue(
                            dimRowJson,
                            new TypeReference<Map<String, String>>() { }
                    );
                    DimensionRow dimensionRow = dimension.parseDimensionRow(dimensionRowMap);
                    dimensionRows.add(dimensionRow);
                }
            }
        } catch (IOException e) {
            LOG.error("Exception while reading dimension rows {}", e);
            throw new RuntimeException(e);
        }
        return dimensionRows;
    }

    /**
     * Getter for dimension rows in tree set for consistent order
     * @return tree set of dimension rows
     */
    @Override
    public TreeSet<DimensionRow> findAllOrderedDimensionRows() {
        return new TreeSet<>(findAllDimensionRows());
    }

    /**
     * Get a set of dimension value(s) given the Dimension type and its value
     *
     * @param dimensionField  field type
     * @param fieldValue  field value
     * @return set of dimension value(s). For find by ID a set is returned with a single dimension row.
     */
    @Deprecated
    @Override
    public Set<DimensionRow> findAllDimensionRowsByField(DimensionField dimensionField, String fieldValue) {
        if (dimensionField == null) {
            throw new IllegalArgumentException("Unknown DimensionField");
        }

        if (dimensionField == dimension.getKey()) {
            DimensionRow drById = findDimensionRowByKeyValue(fieldValue);
            return drById == null ? Collections.<DimensionRow>emptySet() : Collections.singleton(drById);
        }

        Set<DimensionRow> result = new LinkedHashSet<>();
        if (fieldValue == null) {
            return result;
        }
        try {
            String rowKey = DimensionStoreKeyUtils.getRowKey(dimensionField.getName(), fieldValue);
            String refKeys = keyValueStore.get(rowKey);
            if (refKeys == null) {
                return result;
            }
            Set<String> refKeySet = new LinkedHashSet<>(Arrays.asList(objectMapper.readValue(refKeys, String[].class)));

            for (String key : refKeySet) {
                String dimRowJson = keyValueStore.get(key);
                if (dimRowJson != null) {
                    Map<String, String> dimensionRowMap = objectMapper.readValue(
                            dimRowJson,
                            new TypeReference<Map<String, String>>() { }
                    );
                    result.add(dimension.parseDimensionRow(dimensionRowMap));
                }
            }
        } catch (IOException e) {
            LOG.error("Cannot map string to DimensionRow object {}", e);
            throw new RuntimeException(e);
        }
        return result;
    }

    @Override
    public TreeSet<DimensionRow> findFilteredDimensionRows(Set<ApiFilter> filters) {

        TreeSet<DimensionRow> filteredDimensionRows = findAllOrderedDimensionRows();
        // filter chain
        for (ApiFilter filter : filters) {
            switch (filter.getOperation()) {
                case eq:
                    // fall through on purpose since eq and in have the same functionality
                case in:
                    filteredDimensionRows = inFilterOperation(filteredDimensionRows, filter);
                    break;
                case notin:
                    filteredDimensionRows = notinFilterOperation(filteredDimensionRows, filter);
                    break;
                case startswith:
                    filteredDimensionRows = startswithFilterOperation(filteredDimensionRows, filter);
                    break;
                case contains:
                    filteredDimensionRows = containsFilterOperation(filteredDimensionRows, filter);
                    break;
                default:
                    LOG.debug("Illegal Filter operation : {}", filter.getOperation());
                    throw new IllegalArgumentException("Invalid Filter Operation.");
            }
        }
        return filteredDimensionRows;
    }

    @Override
    public boolean isHealthy() {
        return true;
    }

    @Override
    public void clearDimension() {
        //Remove all dimension data from the store.
        findAllDimensionRows().stream()
                .flatMap(dimensionRow -> dimensionRow.entrySet().stream())
                .map(entry -> DimensionStoreKeyUtils.getRowKey(entry.getKey().getName(), entry.getValue()))
                .forEach(keyValueStore::remove);
        //Since the indices are being dropped, the dimension field stored via the columnKey is becoming stale.
        keyValueStore.remove(DimensionStoreKeyUtils.getColumnKey(dimension.getKey().getName()));
        // The allValues key mapping needs to reflect the fact that we are dropping all dimension data.
        keyValueStore.put(DimensionStoreKeyUtils.getAllValuesKey(), "[]");
        //We're resetting the keyValueStore, so we don't want any stale last updated date floating around.
        keyValueStore.remove(DimensionStoreKeyUtils.getLastUpdatedKey());

        refreshCardinality();
    }

    @Override
    public void refreshIndex(String rowId, DimensionRow dimensionRow, DimensionRow dimensionRowOld) {
        refreshIndexForDimensionKey(rowId);
        refreshIndexForDimensionFields(rowId, dimensionRow, dimensionRowOld);
        refreshCardinality();
    }

    @Override
    public void refreshIndex(Map<String, Pair<DimensionRow, DimensionRow>> changedRows) {
        for (String rowId : changedRows.keySet()) {
            // Get old and new rows from the pair
            DimensionRow newRow = changedRows.get(rowId).getKey();
            DimensionRow oldRow = changedRows.get(rowId).getValue();

            // Refresh index for the row
            refreshIndex(rowId, newRow, oldRow);
        }
    }

    /**
     * This method updates the set of values corresponding to a dimension
     *
     * Eg: For dimension dim1 there were 3 rows with ID's v1, v2, v3 and DESC's value1, value2, value3 respectively
     * the keyValueStore would look like
     * all_values_key -> [id_v1_row_key, id_v2_row_key, id_v3_row_key]
     *
     * After this refreshIndexForDimensionKey call, given a new row id: id_v4_row_key, it would look like
     * all_values_key -> [id_v1_row_key, id_v2_row_key, id_v3_row_key, id_v4_row_key]
     *
     * This is useful to list all values for <blah>/dim1/values endpoint
     *
     * @param rowId  The row id to be added
     */
    private void refreshIndexForDimensionKey(String rowId) {
        try {
            String allValuesKey = DimensionStoreKeyUtils.getAllValuesKey();
            String dimensionRows = keyValueStore.get(allValuesKey);
            Set<String> dimensionRowsSet = new LinkedHashSet<>();
            if (dimensionRows != null) {
                dimensionRowsSet.addAll(Arrays.asList(objectMapper.readValue(dimensionRows, String[].class)));
            }
            dimensionRowsSet.add(rowId);
            String dimRows = objectMapper.writeValueAsString(dimensionRowsSet);
            keyValueStore.put(allValuesKey, dimRows);
        } catch (IOException e) {
            LOG.error("Exception while adding dimension entry in KeyValueStore : {}", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * This method updates the store to allow point lookups using the non-key field(s)
     * It loops through all the fields for a dimension (ignores the key field)
     *
     * For a new dimensionRow
     * say ID v1 and DESC value1 a new entry is added in store
     *
     * desc_value1_row_key -> [id_v1_row_key]
     *
     * For a new dimensionRow with duplicate description
     * say ID v2 and DESC value1 the store is updated to look like this
     *
     * desc_value1_row_key -> [id_v1_row_key, id_v2_row_key]
     *
     * For a dimensionRow
     * say ID v2 and DESC value2,
     * dimensionRow -> (v2, value2)
     * dimensionRowOld -> (v2, value1)
     *
     * the store is updated to look like this
     *
     * desc_value1_row_key -> [id_v1_row_key]
     * desc_value2_row_key -> [id_v2_row_key]
     *
     * @param rowId  The id to be associated with the new dimension row
     * @param dimensionRow  The new dimension row to be added to the index
     * @param dimensionRowOld  The original dimension row associated to the given row id
     */
    private void refreshIndexForDimensionFields(
            String rowId,
            DimensionRow dimensionRow,
            DimensionRow dimensionRowOld
    ) {
        try {
            // oldRowValue - key to associate a Value to its dimensionRow/rows
            for (DimensionField field : dimension.getDimensionFields()) {
                // skip key field
                if (field == dimension.getKey()) {
                    continue;
                }
                if (dimensionRowOld != null) {
                    String oldRowValue = dimensionRowOld.get(field);
                    String oldRowValueKey = DimensionStoreKeyUtils.getRowKey(field.getName(), oldRowValue);

                    // oldRowValueSKeys holds all the secondary keys i.e. ref. to dimension rows in dimension storage
                    String oldRowValuesKeysJson = keyValueStore.get(oldRowValueKey);
                    String[] oldRowValueSKeys = objectMapper.readValue(oldRowValuesKeysJson, String[].class);
                    Set<String> oldRowValuesKeySet = new LinkedHashSet<>(Arrays.asList(oldRowValueSKeys));
                    oldRowValuesKeySet.remove(rowId);
                    if (oldRowValuesKeySet.isEmpty()) {
                        keyValueStore.remove(oldRowValueKey);
                    } else {
                        String updatedOldRowValueSKeys = objectMapper.writeValueAsString(oldRowValuesKeySet);
                        keyValueStore.put(oldRowValueKey, updatedOldRowValueSKeys);
                    }
                }

                String rowValue = dimensionRow.get(field);
                String rowValueKey = DimensionStoreKeyUtils.getRowKey(field.getName(), rowValue);

                // rowValueSKeys holds all the secondary keys i.e. ref. to dimension rows in store
                String rowValueSKeys = keyValueStore.get(rowValueKey);
                Set<String> rowValueSKeySet = new LinkedHashSet<>();
                if (rowValueSKeys != null) {
                    rowValueSKeySet.addAll(Arrays.asList(objectMapper.readValue(rowValueSKeys, String[].class)));
                }
                rowValueSKeySet.add(rowId);
                String updatedRowValueSKeys = objectMapper.writeValueAsString(rowValueSKeySet);
                keyValueStore.put(rowValueKey, updatedRowValueSKeys);
            }
        } catch (IOException e) {
            LOG.error("Exception while adding dimension description entry in Dimension Store : {}", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Update the cardinality count.
     */
    private void refreshCardinality() {
        keyValueStore.put(DimensionStoreKeyUtils.getCardinalityKey(), Integer.toString(findAllDimensionRows().size()));
    }

    /**
     * In-filter operation
     *
     * @param dimensionRows  The unfiltered set of dimension rows
     * @param filter  The api filter
     * @return Tree set of DimensionRows
     */
    @Override
    public TreeSet<DimensionRow> inFilterOperation(TreeSet<DimensionRow> dimensionRows, ApiFilter filter) {
        return dimensionRows.stream()
                .filter(row -> filter.getValues().contains(row.get(filter.getDimensionField())))
                .collect(Collectors.toCollection(TreeSet<DimensionRow>::new));
    }

    /**
     * Notin-filter operation
     *
     * @param dimensionRows  The unfiltered set of dimension rows
     * @param filter  The api filter
     * @return Tree set of DimensionRows
     */
    @Override
    public TreeSet<DimensionRow> notinFilterOperation(TreeSet<DimensionRow> dimensionRows, ApiFilter filter) {
        TreeSet<DimensionRow> filteredDimensionRows = new TreeSet<>(dimensionRows);
        for (DimensionRow dimensionRow : dimensionRows) {
            String value = dimensionRow.get(filter.getDimensionField());
            if (filter.getValues().contains(value)) {
                filteredDimensionRows.remove(dimensionRow);
            }
        }
        return filteredDimensionRows;
    }

    /**
     * Startswith-filter operation
     *
     * @param dimensionRows  The unfiltered set of dimension rows
     * @param filter  The api filter
     * @return Tree set of DimensionRows
     */
    @Override
    public TreeSet<DimensionRow> startswithFilterOperation(TreeSet<DimensionRow> dimensionRows, ApiFilter filter) {
        TreeSet<DimensionRow> filteredDimensionRows = new TreeSet<>();

        // regex string containing all starts with filter values
        StringBuilder startsWithRegex = new StringBuilder("(");
        for (String filterValue : filter.getValues()) {
            startsWithRegex.append(filterValue).append("|");
        }
        startsWithRegex.replace(startsWithRegex.length() - 1, startsWithRegex.length(), ").*");

        String startsWithRegexString = startsWithRegex.toString();
        for (DimensionRow dimensionRow : dimensionRows) {
            String value = dimensionRow.get(filter.getDimensionField());
            if (value.matches(startsWithRegexString)) {
                filteredDimensionRows.add(dimensionRow);
            }
        }
        return filteredDimensionRows;
    }

    /**
     * Contains filter operation
     *
     * @param dimensionRows  The unfiltered set of dimension rows
     * @param filter  The api filter
     * @return Tree set of DimensionRows
     */
    @Override
    public TreeSet<DimensionRow> containsFilterOperation(TreeSet<DimensionRow> dimensionRows, ApiFilter filter) {
        TreeSet<DimensionRow> filteredDimensionRows = new TreeSet<>();

        // regex string containing all contains filter values
        StringBuilder containsRegex = new StringBuilder(".*(");
        for (String filterValue : filter.getValues()) {
            containsRegex.append(filterValue).append("|");
        }
        containsRegex.replace(containsRegex.length() - 1, containsRegex.length(), ").*");

        for (DimensionRow dimensionRow : dimensionRows) {
            String value = dimensionRow.get(filter.getDimensionField());
            if (value.matches(containsRegex.toString())) {
                filteredDimensionRows.add(dimensionRow);
            }
        }
        return filteredDimensionRows;
    }
}
