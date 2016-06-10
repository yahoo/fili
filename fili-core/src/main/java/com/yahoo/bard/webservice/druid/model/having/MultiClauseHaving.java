// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.having;

import com.fasterxml.jackson.annotation.JsonGetter;

import java.util.Collections;
import java.util.List;

/**
 * Abstract parent of having models which take a set of other having filters
 */
public abstract class MultiClauseHaving extends Having {

    private final List<Having> having;

    protected MultiClauseHaving(HavingType type, List<Having> havings) {
        super(type);
        this.having = Collections.unmodifiableList(havings);
    }

    protected MultiClauseHaving(HavingType type, Having having) {
        super(type);
        this.having = Collections.unmodifiableList(Collections.singletonList(having));
    }

    @JsonGetter("havingSpecs")
    public List<Having> getHavings() {
        return having;
    }

    public abstract MultiClauseHaving withHavings(List<Having> havings);

    public abstract MultiClauseHaving plusHaving(Having having);

    public abstract MultiClauseHaving plusHavings(List<Having> havings);
}
