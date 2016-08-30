// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.dimension;

import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.util.EnumUtils;

/**
 * DimensionField enum.
 */
public enum WikiDimensionField implements DimensionField {
    ID("Dimension ID"),
    DESC("Dimension Description");

    private String description;
    private String camelName;

    /**
     * Constructor.
     *
     * @param description  Description of this field
     */
    WikiDimensionField(String description) {
        this.description = description;
        this.camelName = EnumUtils.camelCase(name());
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
