// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.names;

/**
 * Defines the logical name of a Dimension as used in the api query.
 */
@FunctionalInterface
public interface DimensionName {

    /**
     * The logical name of this dimension as used in the api query.
     *
     * @return Dimension Name
     */
    String asName();
}
