// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.filters;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.web.ApiFilter;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * ApiFilters specializes the type of the ApiFilterMap.
 */
public class ApiFilters extends LinkedHashMap<Dimension, Set<ApiFilter>> {

    /**
     * Constructor.
     */
    public ApiFilters() {
    }

    /**
     * Constructor.
     *
     * @param filters  A set of filters to copy.
     */
    public ApiFilters(Map<Dimension, Set<ApiFilter>> filters) {
        super(filters);
    }
}
