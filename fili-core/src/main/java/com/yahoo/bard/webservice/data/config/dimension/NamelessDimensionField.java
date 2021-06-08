// Copyright 2021 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.dimension;

import com.yahoo.bard.webservice.data.dimension.DimensionField;

/**
 * DimensionField enum.
 */
public enum NamelessDimensionField implements DimensionField {
    EMPTY;

    /**
     * Constructor.
     *
     */
    NamelessDimensionField() {
    }

    @Override
    public String getName() {
        return "";
    }

    @Override
    public String getDescription() {
        return "";
    }

    @Override
    public String toString() {
        return "";
    }
}
