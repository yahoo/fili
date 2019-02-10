// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl;

import com.yahoo.bard.webservice.data.cache.HashDataCache.Pair;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.dimension.FilterDimensionRows;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.util.DimensionStoreKeyUtils;
import com.yahoo.bard.webservice.util.Pagination;
import com.yahoo.bard.webservice.util.SinglePagePagination;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.DefaultFilterOperation;
import com.yahoo.bard.webservice.web.FilterOperation;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Search provider using scan on dimension rows.
 */
public class ScanSearchProvider implements SearchProvider, FilterDimensionRows {
    private static final Logger LOG = LoggerFactory.getLogger(ScanSearchProvider.class);

    private final ObjectMapper objectMapper;

    private KeyValueStore keyValueStore;
    private Dimension dimension;

    /**
     * Constructor.
     */
    public ScanSearchProvider() {
        // TODO: Make this use the shared Object Mapper
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

    @Override
    public int getDimensionCardinality(boolean refresh) {
        if (refresh) {
            refreshCardinality();
        }
        return getDimensionCardinality();
    }

    /**
     * Get a dimension row given an id.
     *
     * @param value  dimension id to look up
     *
     * @return dimension row if exists else null
     */
    public DimensionRow findDimensionRowByKeyValue(String value) {
        return dimension.findDimensionRowByKeyValue(value);
    }

    @Override
    public TreeSet<DimensionRow> findAllOrderedDimensionRows() {
        return new TreeSet<>(findAllDimensionRows());
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
     * This method updates the set of values corresponding to a dimension.
     * <p>
     * Eg: For dimension dim1 there were 3 rows with ID's v1, v2, v3 and DESC's value1, value2, value3 respectively
     * the keyValueStore would look like
     * all_values_key -> [id_v1_row_key, id_v2_row_key, id_v3_row_key]
     * <p>
     * After this refreshIndexForDimensionKey call, given a new row id: id_v4_row_key, it would look like
     * all_values_key -> [id_v1_row_key, id_v2_row_key, id_v3_row_key, id_v4_row_key]
     * <p>
     * This is useful to list all values for &lt;blah&gt;/dim1/values endpoint
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
     * This method updates the store to allow point lookups using the non-key field(s).
     * It loops through all the fields for a dimension (ignores the key field)
     * <p>
     * For a new dimensionRow
     * say ID v1 and DESC value1 a new entry is added in store
     * <p>
     * desc_value1_row_key -> [id_v1_row_key]
     * <p>
     * For a new dimensionRow with duplicate description
     * say ID v2 and DESC value1 the store is updated to look like this
     * <p>
     * desc_value1_row_key -> [id_v1_row_key, id_v2_row_key]
     * <p>
     * For a dimensionRow
     * say ID v2 and DESC value2,
     * dimensionRow -> (v2, value2)
     * dimensionRowOld -> (v2, value1)
     * <p>
     * the store is updated to look like this
     * <p>
     * desc_value1_row_key -> [id_v1_row_key]
     * desc_value2_row_key -> [id_v2_row_key]
     *
     * @param rowId  The id to be associated with the new dimension row
     * @param dimensionRow  The new dimension row to be added to the index
     * @param dimensionRowOld  The original dimension row associated to the given row id
     */
    private void refreshIndexForDimensionFields(String rowId, DimensionRow dimensionRow, DimensionRow dimensionRowOld) {
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

                    // oldRowValueSKeys holds all the secondary keys i.e. ref. to dimension rows in store
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
            LOG.error("Exception while adding dimension description entry in key value store : {}", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Update the cardinality count.
     */
    private void refreshCardinality() {
        keyValueStore.put(DimensionStoreKeyUtils.getCardinalityKey(), Integer.toString(findAllDimensionRows().size()));
    }

    @Override
    public TreeSet<DimensionRow> inFilterOperation(TreeSet<DimensionRow> dimensionRows, ApiFilter filter) {
        return dimensionRows.stream()
                .filter(row -> filter.getValues().contains(row.get(filter.getDimensionField())))
                .collect(Collectors.toCollection(TreeSet<DimensionRow>::new));
    }

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

    @Override
    public TreeSet<DimensionRow> startswithFilterOperation(
            TreeSet<DimensionRow> dimensionRows,
            ApiFilter filter
    ) {
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
     * Contains filter operation.
     *
     * @param dimensionRows  The unfiltered set of dimension rows
     * @param filter  The api filter
     *
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

    @Override
    public Pagination<DimensionRow> findAllDimensionRowsPaged(PaginationParameters paginationParameters) {
        return new SinglePagePagination<>(
                Collections.unmodifiableList(
                        new ArrayList<>(getAllDimensionRowsPaged(paginationParameters))
                ),
                paginationParameters,
                getDimRowIndexes().size()
        );
    }

    @Override
    public Pagination<DimensionRow> findFilteredDimensionRowsPaged(
            Set<ApiFilter> filters,
            PaginationParameters paginationParameters
    ) {
        TreeSet<DimensionRow> filteredDimensionRows = applyFilters(getAllOrderedDimensionRows(), filters);
        return new SinglePagePagination<>(
                doPagination(
                        filteredDimensionRows,
                        paginationParameters.getPage(filteredDimensionRows.size()),
                        paginationParameters.getPerPage()
                )
                        .stream()
                        .collect(Collectors.toList()),
                paginationParameters,
                filteredDimensionRows.size()
        );
    }

    /**
     * Find all dimension rows that belongs to a requested page.
     *
     * @param paginationParameters  The parameters defining the pagination (i.e. the number of rows per page,
     * and the desired page)
     *
     * @return  All dimension rows that belongs to a requested page
     */
    private TreeSet<DimensionRow> getAllDimensionRowsPaged(PaginationParameters paginationParameters) {
        TreeSet<DimensionRow> allRows = getAllOrderedDimensionRows();
        return doPagination(
                allRows,
                paginationParameters.getPage(allRows.size()),
                paginationParameters.getPerPage()
        );
    }

    /**
     * Find all dimension rows that belongs to a requested page and that preserves TreeSet order.
     *
     * @return  All ordered dimension rows that belongs to a requested page
     */
    private TreeSet<DimensionRow> getAllOrderedDimensionRows() {
        return getDimRowIndexes().stream()
                .map(keyValueStore::get)
                .filter(Objects::nonNull)
                .map(dimRowJson -> readValue(new TypeReference<Map<String, String>>() { }, dimRowJson))
                .map(dimension::parseDimensionRow)
                .collect(Collectors.toCollection(TreeSet::new));
    }

    /**
     * Paginate dimension rows based on the requested page number and the number of results per page.
     *
     * @param dimensionRows  The set of dimension rows to be paginated
     * @param page  The requested page number
     * @param perPage  The number of results per page
     *
     * @return  A page of "perPage" number dimension rows whose page number is specified by "page".
     */
    private TreeSet<DimensionRow> doPagination(TreeSet<DimensionRow> dimensionRows, int page, int perPage) {
        return dimensionRows.stream()
                .skip((page - 1) * perPage)
                .limit(perPage)
                .collect(Collectors.toCollection(TreeSet::new));
    }

    /**
     * Get index of rows.
     *
     * @return  The index of rows
     */
    private List<String> getDimRowIndexes() {
        return readValue(new TypeReference<List<String>>() { },
            keyValueStore.get(DimensionStoreKeyUtils.getAllValuesKey()));
    }

    /**
     * Helper method for reading value from an objectMapper that occurs many times in code.
     * Another advantage of it is to handle exception so that client code doesn't have to wrap themselves inside a
     * try-catch block
     *
     * @param typeReference  The type reference
     * @param value  Value of the key
     * @param <T>  Type of the TypeReference
     *
     * @return  T where T is the type of the TypeReference passed in
     */
    private <T> T readValue(TypeReference<T> typeReference, String value) {
        try {
            return objectMapper.readValue(value, typeReference);
        } catch (IOException e) {
            LOG.error("Exception while reading dimension rows {}", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Filter out dimension rows.
     *
     * @param dimensionRows  The set of dimension rows to be filtered
     * @param filters  The filters that are to be applied to the dimensionRows
     *
     * @return  The set of filtered dimension rows
     */
    private TreeSet<DimensionRow> applyFilters(TreeSet<DimensionRow> dimensionRows, Set<ApiFilter> filters) {
        // filter chain
        for (ApiFilter filter : filters) {
            FilterOperation op = filter.getOperation();
            if (!(op instanceof DefaultFilterOperation)) {
                LOG.error("Illegal Filter operation : {}, only default filter ops supported", filter.getOperation());
                throw new IllegalArgumentException(
                        "Only supports default filter operations: in, notin, startswith, contains, eq"
                );
            }
            DefaultFilterOperation defaultFilterOp = (DefaultFilterOperation) op;

            switch (defaultFilterOp) {
                case eq:
                    // fall through on purpose since eq and in have the same functionality
                case in:
                    dimensionRows = inFilterOperation(dimensionRows, filter);
                    break;
                case notin:
                    dimensionRows = notinFilterOperation(dimensionRows, filter);
                    break;
                case startswith:
                    dimensionRows = startswithFilterOperation(dimensionRows, filter);
                    break;
                case contains:
                    dimensionRows = containsFilterOperation(dimensionRows, filter);
                    break;
                default:
                    LOG.error("Illegal Filter operation : {}", filter.getOperation());
                    throw new IllegalArgumentException("Invalid Filter Operation.");
            }
        }

        return dimensionRows;
    }
}
