// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.builders;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionRowNotFoundException;
import com.yahoo.bard.webservice.druid.model.filter.AndFilter;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.druid.model.filter.InFilter;
import com.yahoo.bard.webservice.druid.model.filter.NotFilter;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.DefaultFilterOperation;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@code DruidInFilterBuilder} builds a conjunction of a in-filter and another in-filter wrapped in a not-filter.
 * <p>
 * {@code DruidInFilterBuilder} sees API filters as in two categories
 * <ol>
 *     <li>
 *         positive filters whose API filter operations are one of the following:
 *         <ul>
 *             <li> {@link DefaultFilterOperation#in}
 *             <li> {@link DefaultFilterOperation#startswith}
 *             <li> {@link DefaultFilterOperation#contains}
 *             <li> {@link DefaultFilterOperation#eq}
 *         </ul>
 *     <li> negative filters whose API filter operation is {@link DefaultFilterOperation#notin}
 * </ol>
 * Dimension row values matching the positive API filters are grouped in the single in-filter. Those values matching the
 * negative API filters are grouped in the other in-filter wrapped in the not-filter.
 * <p>
 * For example, suppose a dimension {@code D} has dimension rows {@code 1}, {@code 2}, {@code 3}, {@code 4}, {@code 5}.
 * A filter clause in a request like {@code D|id-in[1, 2],D|id-notin[4, 5]} results in an and-filter by
 * {@code DruidInFilterBuilder}: {@code AND(IN(1, 2), NOT(IN(4, 5)))}. This and-filter is a conjunction of a in-filter
 * and another in-filter wrapped in a not-filter.
 * <p>
 * Compared with {@link DruidOrFilterBuilder}, the advantage of {@code DruidInFilterBuilder} is reducing the number of
 * Druid filters in a single Druid query. {@code DruidInFilterBuilder} is an enhancement of the
 * {@link DruidOrFilterBuilder} and is the default Druid filter builder in Fili.
 */
public class DruidInFilterBuilder extends ConjunctionDruidFilterBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(DruidInFilterBuilder.class);

    @Override
    protected Filter buildDimensionFilter(Dimension dimension, Set<ApiFilter> filters)
            throws DimensionRowNotFoundException {
        LOG.trace("Building dimension filter using dimension: {} \n\n and set of filter: {}", dimension, filters);

        // split ApiFilters into two groups: positive filters & negative filters
        Pair<Set<ApiFilter>, Set<ApiFilter>> positiveAndNegativeSplitFilters = splitApiFilters(filters);
        Set<ApiFilter> positiveFilters = positiveAndNegativeSplitFilters.getLeft();
        Set<ApiFilter> negatedNegativeFilters =
                negateNegativeFilters(positiveAndNegativeSplitFilters.getRight());

        // search for matched values of the positive filter by sending all of the filters down to search provider once
        List<String> inValues = positiveFilters.isEmpty()
                ? Collections.emptyList()
                : getFilteredDimensionRowValues(dimension, positiveFilters);

        // search for matched values of the negative filter by sending each filter down to search provider one-by-one
        List<String> notInValues = negatedNegativeFilters.stream()
                .map(apiFilter -> {
                    try {
                        return getFilteredDimensionRowValues(dimension, Collections.singleton(apiFilter));
                    } catch (DimensionRowNotFoundException exception) {
                        throw new IllegalStateException(exception);
                    }
                })
                .flatMap(List::stream)
                .distinct()
                .collect(Collectors.toList());

        List<Filter> inFilters = new ArrayList<>(); // A set with at most two in-filters(positive & negative)

        // add a Druid in-filter out of the matched values of the positive filter
        if (!inValues.isEmpty()) {
            inFilters.add(new InFilter(dimension, inValues));
        }

        // build a Druid not-filter containing a in-filter out of the matched values of the negative filter
        if (!notInValues.isEmpty()) {
            inFilters.add(new NotFilter(new InFilter(dimension, notInValues)));
        }

        // combine the two in-filters
        Filter newFilter = inFilters.size() == 1 ? inFilters.get(0) : new AndFilter(inFilters);

        LOG.trace("Filter: {}", newFilter);
        return newFilter;
    }
}
