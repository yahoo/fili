// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.metadata;

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Locale;

/**
 * Allows clients to be notified if a dimension's values are browsable and searchable.
 * <p>
 * For the non-loaded dimensions(A "non-loaded dimension" is a fact based dimension, where we don't load any domain data
 * for it, but simply send queries directly to druid), we need to surface metadata to the UI. If there aren't dimension
 * values loaded, we can't validate when we build filters and you can't use the dimension values endpoint to browse
 * values. UI needs to know that a dimension isn't going to be validated and searched. The way that UI knows about this
 * is through this <tt>StorageStrategy</tt>
 */
public enum StorageStrategy {
    /**
     * Loaded dimension.
     */
    LOADED,
    /**
     * Non-loaded dimension.
     */
    NONE;

    private final String apiName;

    /**
     * Constructor.
     */
    StorageStrategy() {
        this.apiName = name().toLowerCase(Locale.ENGLISH);
    }

    /**
     * Returns the API name of this StorageStrategy.
     *
     * @return the API name of this StorageStrategy
     */
    @JsonValue
    public String getApiName() {
        return apiName;
    }

    @Override
    public String toString() {
        return getApiName();
    }
}
