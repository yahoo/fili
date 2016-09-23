// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.filter;

import java.util.List;

/**
 * Filter for logical OR applied to a set of druid filter expressions.
 */
public class OrFilter extends MultiClauseFilter {

    /**
     * Constructor.
     *
     * @param fields  Filters to OR across.
     */
    public OrFilter(List<Filter> fields) {
        super(DefaultFilterType.OR, fields);
    }

    @Override
    public OrFilter withFields(List<Filter> fields) {
        return new OrFilter(fields);
    }
}
