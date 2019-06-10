// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data;

import com.yahoo.bard.webservice.web.ApiFilter;

import java.util.Collection;

/**
 * Interface that indicates the implementing object can optimize a collection of {@link ApiFilter} by transforming them
 * into a (possibly) different collection of {@link ApiFilter}.
 */
public interface FilterOptimizable {

    /**
     * Optimizes a set of filters on this dimension by transforming them into different set of filters. A dimension
     * is assumed to NOT be able to optimize its filters, so by default this method just returns the original set of
     * filters.
     *
     * @param filters A set of filters on this Dimension.
     * @return the optimized set of filters.
     */
    default Collection<ApiFilter> optimizeFilters(Collection<ApiFilter> filters) {
        return filters;
    }
}
