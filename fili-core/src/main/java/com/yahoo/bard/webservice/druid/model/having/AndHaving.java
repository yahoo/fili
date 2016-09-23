// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.having;

import java.util.List;

/**
 * Having clause model for logical AND applied to a set of druid having expressions.
 */
public class AndHaving extends MultiClauseHaving {

    /**
     * Constructor.
     *
     * @param havings  Child havings that this having wraps with an And.
     */
    public AndHaving(List<Having> havings) {
        super(DefaultHavingType.AND, havings);
    }

    @Override
    public AndHaving withHavings(List<Having> havings) {
        return new AndHaving(havings);
    }
}
