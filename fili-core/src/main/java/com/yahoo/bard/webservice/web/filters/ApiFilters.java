// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.filters;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.web.ApiFilter;

import java.util.HashSet;
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

    public static ApiFilters merge(ApiFilters f1, ApiFilters f2) {
        ApiFilters result = new ApiFilters(f1);
        f2.forEach(
                (dim, value) -> {
                    Set<ApiFilter> filters = new HashSet<>(value);
                    if (result.containsKey(dim)) {
                        filters.addAll(result.get(dim));
                    }
                    result.put(dim, filters);
                }
        );
        return result;
    }
}

