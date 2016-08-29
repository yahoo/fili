// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import java.util.HashMap;

/**
 * Interface for transforming logical tables into views.
 */
public class TableView extends HashMap<String, Object> {

    /**
     * Constructor.
     */
    public TableView () {
        super();
    }

    /**
     * Constructor.
     *
     * @param key  Initial key with which to populate the map
     * @param value  Initial value with which to populate the map
     */
    public TableView (String key, Object value) {
        this.put(key, value);
    }
}
