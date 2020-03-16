// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.builders;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionRowNotFoundException;
import com.yahoo.bard.webservice.data.dimension.impl.NoOpSearchProvider;
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
import java.util.List;
import java.util.Set;

/**
 * A DruidOrFilterBuilder builds a conjunction of disjunctions for each Dimension, where each disjunction
 * corresponds to a filter term. So, the filter terms on dimension {@code category}:
 * <p>
 * {@code category|id-in[finance,sports],category|desc-contains[baseball]}
 * <p>
 * are translated into:
 * <p>
 * {@code AndFilter(OrFilter(select(category, finance), select(category, sports)), OrFilter(select(category, sports)))}
 * <p>
 * Each filter term is resolved independently of the other filter terms.
 */
public class DruidOrFilterBuilder extends ConjunctionDruidFilterBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(DruidOrFilterBuilder.class);

    @Override
    protected Filter buildDimensionFilter(Dimension dimension, Set<ApiFilter> filters)
            throws DimensionRowNotFoundException {
        LOG.trace("Building dimension filter using dimension: {} \n\n and set of filter: {}", dimension, filters);

        List<Filter> orFilters = new ArrayList<>();
        for (ApiFilter filter : filters) {
            ApiFilter normalizedFilter = filter;
            if (normalizedFilter.getOperation().equals(DefaultFilterOperation.notin)) {
                normalizedFilter = filter.withOperation(DefaultFilterOperation.in);
            }

            Filter disjunction = null;
            if (dimension.getSearchProvider() instanceof NoOpSearchProvider &&
                    filter.getOperation() == DefaultFilterOperation.contains) {
                disjunction = new OrFilter(buildContainsSearchFilters(
                        dimension,
                        filter.getValuesList()
                ));
            } else {
                disjunction = new OrFilter(buildSelectorFilters(
                        dimension,
                        getFilteredDimensionRows(dimension, Collections.singleton(normalizedFilter))
                ));
            }
            orFilters.add(normalizedFilter == filter ? disjunction : new NotFilter(disjunction));
        }

        Filter newFilter = orFilters.size() == 1 ? orFilters.get(0) : new AndFilter(orFilters);
        LOG.trace("Filter: {}", newFilter);
        return newFilter;
    }
}
