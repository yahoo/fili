// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.provider.impl;

import com.yahoo.bard.webservice.data.config.names.FieldName;

import java.util.Objects;

/**
 * A FieldName.
 */
public class FieldNameImpl implements FieldName {
    protected final String name;

    /**
     * Construct a new name.
     *
     * @param name  The field name
     */
    public FieldNameImpl(String name) {
        this.name = name;
    }

    @Override
    public String asName() {
        return name;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        final FieldNameImpl fieldName = (FieldNameImpl) o;
        return Objects.equals(name, fieldName.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
