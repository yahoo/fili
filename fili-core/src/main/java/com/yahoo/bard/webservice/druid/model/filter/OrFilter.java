// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.filter;

import java.util.List;

/**
 * Filter for logical OR applied to a set of druid filter expressions
 */
public class OrFilter extends MultiClauseFilter {

    public OrFilter(List<Filter> fields) {
        super(DefaultFilterType.OR, fields);
    }

    public OrFilter(Filter field) {
        super(DefaultFilterType.OR, field);
    }

    @Override
    public OrFilter withFields(List<Filter> fields) {
        return new OrFilter(fields);
    }

    @Override
    public OrFilter plusField(Filter field) {
        List<Filter> fields = getFields();
        fields.add(field);
        return new OrFilter(fields);
    }

    @Override
    public OrFilter plusFields(List<Filter> fields) {
        List<Filter> oldFields = getFields();
        fields.addAll(oldFields);
        return new OrFilter(fields);
    }

    public AndFilter asAndFilter() {
        return new AndFilter(getFields());
    }
}
