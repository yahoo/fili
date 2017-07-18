// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.druid.model.query;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Class for specifying the FragmentSearchQuerySpec for DruidSearchQuery.
 */
public class FragmentSearchQuerySpec extends SearchQuerySpec {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final List<String> values;

    /**
     * Constructor.
     *
     * @param values  Fragments to search for
     */
    public FragmentSearchQuerySpec(Collection<String> values) {
        super(DefaultSearchQueryType.FRAGMENT);
        this.values = Collections.unmodifiableList(new ArrayList<>(values));
    }

    public List<String> getValues() {
        return values;
    }
}
