// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.names;

/**
 * Marker interface for enums that can be treated as a metric or dimension field name for Druid Query fields.
 */
@FunctionalInterface
public interface FieldName {

    /**
     * Get the name as it should be used in Druid Queries.
     *
     * @return The name of the enum as it should be used in Druid Queries.
     */
    String asName();
}
