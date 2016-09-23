// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query;

/**
 * Baseclass for specifying SearchQuerySpec for DruidSearchQuery.
 */
public abstract class SearchQuerySpec {
    protected final SearchQueryType type;

    /**
     * Constructor.
     *
     * @param type  Type of this search query
     */
    protected SearchQuerySpec(SearchQueryType type) {
        this.type = type;
    }

    public SearchQueryType getType() {
        return type;
    }
}
