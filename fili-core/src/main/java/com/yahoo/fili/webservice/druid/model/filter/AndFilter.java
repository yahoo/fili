// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.druid.model.filter;

import java.util.List;

/**
 * Filter for logical AND applied to a set of druid filter expressions.
 */
public class AndFilter extends MultiClauseFilter {

    /**
     * Constructor.
     *
     * @param fields  Filters to AND across.
     */
    public AndFilter(List<Filter> fields) {
        super(DefaultFilterType.AND, fields);
    }

    @Override
    public AndFilter withFields(List<Filter> fields) {
        return new AndFilter(fields);
    }
}
