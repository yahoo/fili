// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Interface for transforming logical tables into views.
 */
public class MetadataObject extends LinkedHashMap<String, Object> {

    /**
     * Constructor.
     */
    public MetadataObject() {
        super();
    }

    /**
     * Constructor.
     *
     * @param copy  A collection of metadata to duplicate.
     */
    public MetadataObject(Map<String, Object> copy) {
        super(copy);
    }

    /**
     * Constructor.
     *
     * @param key  Initial key with which to populate the map
     * @param value  Initial value with which to populate the map
     */
    public MetadataObject(String key, Object value) {
        this.put(key, value);
    }
}
