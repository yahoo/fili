// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl;

import com.yahoo.bard.webservice.data.dimension.DimensionField;

import java.util.Collections;
import java.util.LinkedHashSet;

/**
 * Support for a single field dimension supporting a flag dimension.
 */
public class DefaultTagKeyDimensionField implements DimensionField {

    public static final DefaultTagKeyDimensionField DEFAULT_FIELD = new DefaultTagKeyDimensionField();

    public static final LinkedHashSet<DimensionField> DEFAULT_FIELDS =
            new LinkedHashSet<>(
                    Collections.singleton(DEFAULT_FIELD)
            );

    /**
     * Private constructor.
     */
    private DefaultTagKeyDimensionField() {
        ;
    }

    private static String NAME = "id";
    private static String DESCRIPTION = "flag value";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getDescription() {
        return DESCRIPTION;
    }

    @Override
    public int hashCode() {
        return DefaultTagKeyDimensionField.class.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        return obj instanceof DefaultTagKeyDimensionField;
    }
}
