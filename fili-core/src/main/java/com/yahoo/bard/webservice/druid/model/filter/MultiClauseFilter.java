// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.filter;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Filter parent class for filters which take a list of child filters.
 */
public abstract class MultiClauseFilter extends Filter implements ComplexFilter {

    private final List<Filter> fields;

    /**
     * Constructor.
     *
     * @param type  Type of the filter
     * @param fields  Collection of child filters this filter wraps
     */
    protected MultiClauseFilter(FilterType type, List<Filter> fields) {
        super(type);
        this.fields =  Collections.unmodifiableList(fields);
    }

    @Override
    public List<Filter> getFields() {
        return fields;
    }

    /**
     * Get a new instance of this filter with the given fields.
     *
     * @param fields  Fields of the new filter.
     *
     * @return a new instance of this filter with the given fields
     */
    public abstract MultiClauseFilter withFields(List<Filter> fields);

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fields);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null || getClass() != obj.getClass()) { return false; }
        MultiClauseFilter other = (MultiClauseFilter) obj;
        return super.equals(obj) &&
                Objects.equals(fields, other.fields);
    }
}
