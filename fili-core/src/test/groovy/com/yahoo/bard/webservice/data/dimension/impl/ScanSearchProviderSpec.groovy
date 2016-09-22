// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl

import com.yahoo.bard.webservice.util.DimensionStoreKeyUtils

/**
 * Specification for behavior specific to the ScanSearchProvider.
 */
class ScanSearchProviderSpec extends SearchProviderSpec<ScanSearchProvider> {

    @Override
    ScanSearchProvider getSearchProvider(String dimensionName) {
        ScanSearchProviderManager.getInstance(dimensionName)
    }

    @Override
    void cleanSearchProvider(String dimensionName) {
        ScanSearchProviderManager.removeInstance(dimensionName)
    }

    @Override
    boolean indicesHaveBeenCleared() {
        return searchProvider.keyValueStore.store.size() == 2 &&
                searchProvider.keyValueStore[DimensionStoreKeyUtils.getCardinalityKey()] == "0" &&
                searchProvider.keyValueStore[DimensionStoreKeyUtils.getAllValuesKey()] == "[]"
    }
}
