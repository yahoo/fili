// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.filter;

/**
 * Filter for logical NOT applied to filter expression
 */
public class NotFilter extends Filter {

    private final Filter field;

    public NotFilter(Filter field) {
        super(DefaultFilterType.NOT);
        this.field = field;
    }

    public Filter getField() {
        return field;
    }

    public Filter withField(Filter field) {
        return new NotFilter(field);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((field == null) ? 0 : field.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (!super.equals(obj)) { return false; }
        if (getClass() != obj.getClass()) { return false; }
        NotFilter other = (NotFilter) obj;
        if (field == null) {
            if (other.field != null) { return false; }
        } else if (!field.equals(other.field)) { return false; }
        return true;
    }
}
