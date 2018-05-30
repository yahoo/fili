// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.rfc_dimension.impl;

import com.yahoo.bard.webservice.data.cache.HashDataCache;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.metadata.StorageStrategy;
import com.yahoo.bard.webservice.data.dimension.rfc_dimension.DomainSchema;
import com.yahoo.bard.webservice.data.dimension.rfc_dimension.impl.keystores.KeyValueStoreAdaptor;
import com.yahoo.bard.webservice.data.dimension.rfc_dimension.impl.keystores.OldKeyValueStore;
import com.yahoo.bard.webservice.data.dimension.rfc_dimension.mutable.MutableIndexedDomain;
import com.yahoo.bard.webservice.util.DimensionStoreKeyUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

// Replaces most of the existing KeyValueStoreDimension behaviors

public class ClassicMutableKeyValueStoreDomain extends KeyStoreSearchProviderIndexedDomain
        implements MutableIndexedDomain {

    private static final Logger LOG = LoggerFactory.getLogger(ClassicMutableKeyValueStoreDomain.class);

    public static final String LAST_UPDATED_KEY = DimensionStoreKeyUtils.getLastUpdatedKey();
    public static final StorageStrategy DEFAULT_STORAGE_STRATEGY = StorageStrategy.LOADED;


    OldKeyValueStore keyValueStore;
    ObjectMapper objectMapper;
    DimensionRowFactory dimensionRowFactory;

    public ClassicMutableKeyValueStoreDomain(
            String name,
            DomainSchema dimensionSchema,
            OldKeyValueStore keyValueStore,
            DimensionRowFactory dimensionRowFactory,
            SearchProvider searchProvider,
            ObjectMapper objectMapper
    ) {
        this(
                name,
                dimensionSchema,
                keyValueStore,
                dimensionRowFactory,
                searchProvider,
                objectMapper,
                DEFAULT_STORAGE_STRATEGY
        );
    }

    public ClassicMutableKeyValueStoreDomain(
            String name,
            DomainSchema domainSchema,
            OldKeyValueStore keyValueStore,
            DimensionRowFactory dimensionRowFactory,
            SearchProvider searchProvider,
            ObjectMapper objectMapper,
            StorageStrategy storageStrategy
    ) {
        super(
                name,
                domainSchema,
                new KeyValueStoreAdaptor(keyValueStore),
                dimensionRowFactory,
                searchProvider,
                storageStrategy
        );
        this.objectMapper = objectMapper;
        this.keyValueStore = keyValueStore;
        this.dimensionRowFactory = dimensionRowFactory;
    }

    @Override
    public void addDimensionRow(DimensionRow dimensionRow) {
        addAllDimensionRows(Collections.singleton(dimensionRow));
    }

    Optional<DimensionRow> replaceDimensionRow(DimensionRow newDimensionRow, DomainSchema domainSchema) {
        String row = keyValueStore.get(newDimensionRow.getKeyValue());

        if (row != null) {
            DimensionRow oldDimensionRow = dimensionRowFactory.apply(row.getBytes(), domainSchema);
            if (newDimensionRow.equals(oldDimensionRow)) {
                return Optional.of(oldDimensionRow);
            }
        }
        return Optional.empty();
    }

    @Override
    public void addAllDimensionRows(Set<DimensionRow> dimensionRows) {
        Map<String, String> storeRows = new LinkedHashMap<>(dimensionRows.size());
        Map<String, HashDataCache.Pair<DimensionRow, DimensionRow>> indexRows = new LinkedHashMap<>(dimensionRows.size());
        DimensionField keyField = domainSchema.getKey();
        String keyFieldName = keyField.getName();
        try {

            for (DimensionRow dimensionRow : dimensionRows) {
                String keyValue = dimensionRow.getKeyValue();
                if (dimensionRow.isEmpty()) {
                    LOG.warn("Ignoring attempt to add a dimension row with no data {}", dimensionRow);
                    continue;
                } else if (dimensionRow.get(keyField) == null) {
                    LOG.warn("Attempting to add a dimension row with a null key {}", dimensionRow);
                    throw new IllegalArgumentException("Cannot add dimension with null key.");
                }

                Optional<DimensionRow> dimensionRowOld = replaceDimensionRow(dimensionRow, domainSchema);

                if (dimensionRowOld.isPresent()) {
                    storeRows.put(keyValue, objectMapper.writeValueAsString(dimensionRow));
                }
                indexRows.put(
                        keyValue,
                        new HashDataCache.Pair<DimensionRow, DimensionRow>(
                                dimensionRow,
                                dimensionRowOld.orElse(null)
                        )
                );

            }
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        keyValueStore.putAll(storeRows);
        searchProvider.refreshIndex(indexRows);
    }

    @Override
    public DimensionRow findDimensionRowByKeyValue(final String keyValue) {
        return dimensionRowFactory.apply(keyValueStore.get(keyValue).getBytes(), domainSchema);
    }

    @Override
    public Optional<DateTime> getLastUpdated() {
        String lastUpdatedString = keyValueStore.get(LAST_UPDATED_KEY);
        if (lastUpdatedString == null) {
            return null;
        } else {
            return Optional.of(DateTime.parse(lastUpdatedString));
        }
    }

    @Override
    public void setLastUpdated(final DateTime lastUpdated) {
        if (lastUpdated == null) {
            keyValueStore.remove(LAST_UPDATED_KEY);
        } else {
            keyValueStore.put(LAST_UPDATED_KEY, lastUpdated.toString());
        }
    }

    @Override
    public void refreshIndex(String rowId, DimensionRow dimensionRow, DimensionRow dimensionRowOld)
    {
        searchProvider.refreshIndex(rowId, dimensionRow, dimensionRowOld);
    }

    @Override
    public void refreshIndex(final Map<String, HashDataCache.Pair<DimensionRow, DimensionRow>> changedRows) {
        searchProvider.refreshIndex(changedRows);
    }
}
