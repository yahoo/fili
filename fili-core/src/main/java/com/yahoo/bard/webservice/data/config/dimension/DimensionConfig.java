// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.dimension;

import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension;
import com.yahoo.bard.webservice.data.dimension.metadata.StorageStrategy;

import java.util.LinkedHashSet;

/**
 * Defines the core information needed to define a dimension.
 */
public interface DimensionConfig {

    /**
     * The API Name is the external, end-user-facing name for the dimension. This is the name of this dimension in API
     * requests.
     *
     * @return User facing name for this dimension
     */
    String getApiName();

    /**
     * The Long Name is the external, end-user-facing long  name for the dimension.
     *
     * @return User facing long name for this dimension
     */
    String getLongName();

    /**
     * The Category is the external, end-user-facing category for the dimension.
     *
     * @return User facing category for this dimension
     */
    String getCategory();

    /**
     * The internal, physical name for the dimension. This field (if set) is used as the only physical name.
     *
     * @return The name of the druid dimension
     */
    String getPhysicalName();

    /**
     * The description for this dimension.
     *
     * @return A description of the dimension and its meaning
     */
    String getDescription();

    /**
     * The storage strategy for this dimension.
     *
     * @return The storage strategy of the dimension
     */
    StorageStrategy getStorageStrategy();

    /**
     * The set of fields for this dimension.
     *
     * @return The set of all dimension fields for this dimension
     */
    LinkedHashSet<DimensionField> getFields();

    /**
     * The default set of fields for this dimension to be shown in the response.
     *
     * @return The default set of dimension fields to be shown in the response row for this dimension
     */
    LinkedHashSet<DimensionField> getDefaultDimensionFields();

    /**
     * The key value store holding dimension row data.
     *
     * @return The store for this dimension
     */
    KeyValueStore getKeyValueStore();

    /**
     * The search provider for field value lookups on this dimension.
     *
     * @return The SearchProvider for this dimension
     */
    SearchProvider getSearchProvider();

    /**
     * Return whether this dimension can be aggregated.
     * By default a dimension is aggregatable.
     *
     * @return  true if this dimension is aggregatable.
     */
    default boolean isAggregatable() {
        return true;
    }

    /**
     * The type of the Dimension this DimensionConfiguration is intended to build.
     *
     * @return  The type of the Dimension this DimensionConfiguration is intended to build,
     * KeyValueStoreDimension by default
     */
    default Class getType() {
        return KeyValueStoreDimension.class;
    }
}
