// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.having;

import com.fasterxml.jackson.annotation.JsonGetter;

import java.util.Collections;
import java.util.List;

/**
 * Abstract parent of having models which take a set of other having filters.
 */
public abstract class MultiClauseHaving extends Having {

    private final List<Having> having;

    /**
     * Constructor.
     *
     * @param type  Type of Having
     * @param havings  Child havings that this having wraps
     */
    protected MultiClauseHaving(HavingType type, List<Having> havings) {
        super(type);
        this.having = Collections.unmodifiableList(havings);
    }

    @JsonGetter("havingSpecs")
    public List<Having> getHavings() {
        return having;
    }

    /**
     * Get a new instance of this having with the given Havings.
     *
     * @param havings  Child havings of the new having.
     *
     * @return a new instance of this having with the given child havings
     */
    public abstract MultiClauseHaving withHavings(List<Having> havings);
}
