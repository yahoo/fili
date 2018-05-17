// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.table;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;

/**
 * Defines the core interactions to load logical tables, physical tables, and table groups into the resource
 * dictionaries.
 */
@FunctionalInterface
public interface TableLoader {

    /**
     * Load all of the logical tables, their table groups, and the physical tables for those groups.
     *
     * @param dictionaries  The resource dictionaries that will be loaded with table configuration
     */
    void loadTableDictionary(ResourceDictionaries dictionaries);
}
