// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.builders;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionRowNotFoundException;
import com.yahoo.bard.webservice.druid.model.filter.AndFilter;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.druid.model.filter.NotFilter;
import com.yahoo.bard.webservice.druid.model.filter.OrFilter;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.DefaultFilterOperation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Builds Dimension filters in a way to keep the size of filter clauses in Druid small.
 * <p>
 * If there is at least one positive filter amongst the filters of a given Dimension, then all of the filters are
 * resolved into a set of DimensionRows at once. A disjunction of selector filters is then built, one for each
 * dimension row in the resolved set.
 * <p>
 * So the filters:
 * <p>
 * {@code sports|id-notin[baseball,soccer],sports|desc-contains[ball]}
 * <p>
 * are translated into something like:
 * <p>
 * {@code OrFilter(select(sports, basketball), select(sports, lacrosse), select(sports, americanFootball))}
 * <p>
 * If all of the filters are negative, then a direct translation of the ApiFilters is returned. So, the following
 * filters:
 * <p>
 * {@code sports|id-notin[baseball,soccer],sports|season-notin[summer]}
 * <p>
 * are translated into something like:
 * <p>
 * {@code AndFilter(
 *              NotFilter(OrFilter(select(sports, soccer), select(sports, baseball))),
 *              NotFilter(OrFilter(select(sports, baseball), select(sports, track), select(sports, lacrosse)))
 *       )
 * }
 */
public class ConsolidatingDruidFilterBuilder extends ConjunctionDruidFilterBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(ConsolidatingDruidFilterBuilder.class);

    @Override
    protected Filter buildDimensionFilter(Dimension dimension, Set<ApiFilter> filters)
            throws DimensionRowNotFoundException {
        LOG.trace("Building dimension filter using dimension: {} and set of filters: {}", dimension, filters);

        // A positive filter will usually reduce the set of rows by a lot.
        if (!filters.stream().map(ApiFilter::getOperation).allMatch(DefaultFilterOperation.notin::equals)) {
            // The search provider returns the set of dimension rows that satisfy all the filters, which are translated
            // into a disjunction of selector filters on their ids for Druid to use.
            List<Filter> druidFilters = buildSelectorFilters(dimension, getFilteredDimensionRows(dimension, filters));
            return druidFilters.size() == 1 ? druidFilters.get(0) : new OrFilter(druidFilters);
        }
        // The search providers do not support disjunctions across dimension|field terms, so we can't use
        // DeMorgan's Law to send the positive versions of all the filters to the search provider at once.
        Set<ApiFilter> negatedFilters = filters.stream()
                .map(filter -> filter.withOperation(DefaultFilterOperation.in))
                .collect(Collectors.toCollection(LinkedHashSet::new));

        // Can't really stream because getFilteredDimensionRows throws a very specific checked exception.
        List<Filter> druidFilters = new ArrayList<>(negatedFilters.size());
        for (ApiFilter negatedFilter : negatedFilters) {
            List<Filter> selectorFilters = buildSelectorFilters(
                    dimension,
                    getFilteredDimensionRows(dimension, Collections.singleton(negatedFilter))
            );
            druidFilters.add(new NotFilter(
                    selectorFilters.size() == 1 ?
                            selectorFilters.get(0) :
                            new OrFilter(selectorFilters)
            ));
        }
        return druidFilters.size() == 1 ? druidFilters.get(0) : new AndFilter(druidFilters);
    }
}
