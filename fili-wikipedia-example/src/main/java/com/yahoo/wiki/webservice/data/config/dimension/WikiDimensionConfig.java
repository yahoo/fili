// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.dimension;

import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.wiki.webservice.data.config.names.WikiApiDimensionName;

import java.util.LinkedHashSet;

import javax.validation.constraints.NotNull;

/**
 * The Wiki configuration source for dimensions.
 */
public class WikiDimensionConfig implements DimensionConfig {

    private final WikiApiDimensionName apiName;
    private final String physicalName;
    private final String description;
    private final LinkedHashSet<DimensionField> fields;
    private final KeyValueStore keyValueStore;
    private final SearchProvider searchProvider;

    /**
     * Constructor.
     *
     * @param apiName  The name of the dimension
     * @param physicalName  The physical name of the dimension in the fact table
     * @param keyValueStore  The key value store backing this dimension
     * @param searchProvider  The indexing provider for this dimension
     * @param fields  Dimension columns defining this dimension
     */
    public WikiDimensionConfig(
            @NotNull WikiApiDimensionName apiName,
            @NotNull String physicalName,
            @NotNull KeyValueStore keyValueStore,
            @NotNull SearchProvider searchProvider,
            @NotNull LinkedHashSet<DimensionField> fields
    ) {
        this.apiName = apiName;
        this.physicalName = physicalName;
        this.description = apiName.asName();
        this.keyValueStore = keyValueStore;
        this.searchProvider = searchProvider;
        this.fields = fields;
    }

    @Override
    public String getApiName() {
        return apiName.asName();
    }

    @Override
    public String getPhysicalName() {
        return physicalName;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public LinkedHashSet<DimensionField> getFields() {
        return fields;
    }

    @Override
    public KeyValueStore getKeyValueStore() {
        return keyValueStore;
    }

    @Override
    public SearchProvider getSearchProvider() {
        return searchProvider;
    }

    @Override
    public LinkedHashSet<DimensionField> getDefaultDimensionFields() {
        return new LinkedHashSet<>();
    }

    @Override
    public String getLongName() {
        return apiName.asName();
    }

    @Override
    public String getCategory() {
        return Dimension.DEFAULT_CATEGORY;
    }
}
