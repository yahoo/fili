// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.table;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;

/**
 * Defines the core interactions to load logical tables, physical tables, and table groups into the resource
 * dictionaries.
 */
public interface TableLoader {

    /**
     * Load datasource metadata service and call loadTableDictionary implemented by the user above.
     *
     * @param dictionaries  The resource dictionaries that will be loaded with table configuration
     */
    void loadTableDictionary(ResourceDictionaries dictionaries);
}
