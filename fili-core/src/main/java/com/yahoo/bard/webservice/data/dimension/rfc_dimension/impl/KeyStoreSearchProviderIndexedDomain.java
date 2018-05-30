// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.rfc_dimension.impl;

import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.TimeoutException;
import com.yahoo.bard.webservice.data.dimension.metadata.StorageStrategy;
import com.yahoo.bard.webservice.data.dimension.rfc_dimension.DomainSchema;
import com.yahoo.bard.webservice.data.dimension.rfc_dimension.IndexedDomain;
import com.yahoo.bard.webservice.data.dimension.rfc_dimension.impl.keystores.ByteArrayKeyValueStore;
import com.yahoo.bard.webservice.util.Pagination;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import org.joda.time.DateTime;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.TreeSet;

// GOAL
/**
 * A domain backed by a {@link ByteArrayKeyValueStore} instance.
 *
 * Storage details
 */
public class KeyStoreSearchProviderIndexedDomain implements IndexedDomain {

    private final String domainName;

    protected final DomainSchema domainSchema;

    protected final SearchProvider searchProvider;

    protected final ByteArrayKeyValueStore keyValueStore;

    protected final DimensionRowFactory dimensionRowFactory;

    private final StorageStrategy storageStrategy;

    public KeyStoreSearchProviderIndexedDomain(
            String domainName,
            DomainSchema domainSchema,
            ByteArrayKeyValueStore keyValueStore,
            DimensionRowFactory dimensionRowFactory,
            SearchProvider searchProvider,
            StorageStrategy storageStrategy
    ) {
        this.domainName = domainName;
        this.domainSchema = domainSchema;
        this.searchProvider = searchProvider;
        this.keyValueStore = keyValueStore;
        this.dimensionRowFactory = dimensionRowFactory;
        this.storageStrategy = storageStrategy;
    }

    @Override
    public OptionalInt getDimensionCardinality() {
        return OptionalInt.of(searchProvider.getDimensionCardinality());
    }

    @Override
    public Pagination<DimensionRow> findAllDimensionRowsPaged(PaginationParameters paginationParameters) {
        return searchProvider.findAllDimensionRowsPaged(paginationParameters);
    }

    @Override
    public TreeSet<DimensionRow> findAllOrderedDimensionRows() throws TimeoutException {
        return searchProvider.findAllOrderedDimensionRows();
    }

    @Override
    public Pagination<DimensionRow> findFilteredDimensionRowsPaged(
            final Set<ApiFilter> filters, final PaginationParameters paginationParameters
    ) {
        return searchProvider.findFilteredDimensionRowsPaged(filters, paginationParameters);
    }

    @Override
    public String getName() {
        return domainName;
    }

    @Override
    public DomainSchema getSchema() {
        return domainSchema;
    }

    @Override
    public StorageStrategy getStorageStrategy() {
        return storageStrategy;
    }

    @Override
    public Optional<Integer> getCardinality() {
        return Optional.of(searchProvider.getDimensionCardinality());
    }

    @Override
    public DimensionRow findDimensionRowByKeyValue(String keyValue) {
        return dimensionRowFactory.apply(keyValueStore.get(keyValue), domainSchema);
    }

    @Override
    public Optional<DateTime> getLastUpdated() {
        return Optional.empty();
    }

    @Override
    public boolean isHealthy() {
        return keyValueStore.isHealthy() && searchProvider.isHealthy();
    }
}
