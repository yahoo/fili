// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.filter;

import java.util.ArrayList;
import java.util.List;

/**
 * Filter parent class for filters which take a list of child filters
 */
public abstract class MultiClauseFilter extends Filter {

    private final List<Filter> fields;

    protected MultiClauseFilter(FilterType type, List<Filter> fields) {
        super(type);
        this.fields = new ArrayList<>(fields);
    }

    protected MultiClauseFilter(FilterType type, Filter field) {
        super(type);
        fields = new ArrayList<>();
        fields.add(field);
    }

    public List<Filter> getFields() {
        return new ArrayList<>(fields);
    }

    public abstract MultiClauseFilter withFields(List<Filter> fields);

    public abstract MultiClauseFilter plusField(Filter field);

    public abstract MultiClauseFilter plusFields(List<Filter> fields);

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((fields == null) ? 0 : fields.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (!super.equals(obj)) { return false; }
        if (getClass() != obj.getClass()) { return false; }
        MultiClauseFilter other = (MultiClauseFilter) obj;
        if (fields == null) {
            if (other.fields != null) { return false; }
        } else if (!fields.equals(other.fields)) { return false; }
        return true;
    }
}
