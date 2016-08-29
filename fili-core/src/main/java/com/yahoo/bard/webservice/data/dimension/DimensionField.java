// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

/**
 * Dimension field.
 */
public interface DimensionField {

    /**
     * The name of the dimension field.
     *
     * @return field name
     */
    String getName();

    /**
     * The description for the dimension field.
     *
     * @return the description
     */
    String getDescription();
}
