// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.dimension;

import com.yahoo.bard.webservice.data.dimension.DimensionField;

/**
 * DimensionField enum.
 */
public enum DefaultDimensionField implements DimensionField {
    ID("Dimension ID");

    private String description;
    private String camelName;

    /**
     * Constructor.
     *
     * @param description  Description of this field
     */
    DefaultDimensionField(String description) {
        this.description = description;
        this.camelName = name();
    }

    @Override
    public String getName() {
        return camelName;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return camelName;
    }
}
