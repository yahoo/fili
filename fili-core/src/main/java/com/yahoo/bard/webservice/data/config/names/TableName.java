// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.names;

/**
 * Marker interface for objects that can be treated as a table name in druid or web services.
 */
public interface TableName {

    /**
     * Return a string representation of a table name.
     *
     * @return the name
     */
    String asName();

}
