// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.filter;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Filter for logical NOT applied to filter expression.
 */
public class NotFilter extends Filter implements ComplexFilter {

    private final Filter field;

    /**
     * Constructor.
     *
     * @param field  Child filter to "not" over
     */
    public NotFilter(Filter field) {
        super(DefaultFilterType.NOT);
        this.field = field;
    }

    public Filter getField() {
        return field;
    }

    /**
     * Get a new instance of this filter with the given field.
     *
     * @param field  Field of the new filter.
     *
     * @return a new instance of this filter with the given field
     */
    public Filter withField(Filter field) {
        return new NotFilter(field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), field);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null || getClass() != obj.getClass()) { return false; }
        NotFilter other = (NotFilter) obj;
        return super.equals(obj) &&
                Objects.equals(field, other.field);
    }

    @Override
    @JsonIgnore
    public List<Filter> getFields() {
        return Collections.singletonList(getField());
    }
}
