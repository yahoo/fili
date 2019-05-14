package com.yahoo.bard.webservice.data.dimension.impl

class MapSearchProviderSpec extends SearchProviderSpec<MapSearchProvider>{

    Map rowMap = [:]

    @Override
    MapSearchProvider getSearchProvider(final String dimensionName) {
        return new MapSearchProvider(rowMap)
    }

    @Override
    void cleanSearchProvider(final String dimensionName) {

    }

    @Override
    boolean indicesHaveBeenCleared() {
        return true
    }
}
