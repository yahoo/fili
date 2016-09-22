// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.having;

import java.util.List;

/**
 * Having clause model for logical OR applied to a set of druid having expressions.
 */
public class OrHaving extends MultiClauseHaving {

    /**
     * Constructor.
     *
     * @param havings  Child havings to wrap Or boolean logic
     */
    public OrHaving(List<Having> havings) {
        super(DefaultHavingType.OR, havings);
    }

    @Override
    public OrHaving withHavings(List<Having> havings) {
        return new OrHaving(havings);
    }
}
