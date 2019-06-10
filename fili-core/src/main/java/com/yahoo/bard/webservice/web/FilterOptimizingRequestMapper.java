// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.data.FilterOptimizable;
import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest;
import com.yahoo.bard.webservice.web.filters.ApiFilters;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.ws.rs.container.ContainerRequestContext;

/**
 * Request mapper that checks if any of the Dimensions that are being filtered on can optimize their filters, and if
 * so performs that optimization.
 */
public class FilterOptimizingRequestMapper extends ChainingRequestMapper<DataApiRequest> {

    private static BiFunction<Set<ApiFilter>, Set<ApiFilter>, Set<ApiFilter>> FILTER_MERGE =
            (filters1, filters2) -> Stream.of(filters1, filters2).flatMap(Set::stream).collect(Collectors.toSet());

    /**
     * Constructor.
     *
     * @param resourceDictionaries The dictionaries to use for request mapping.
     * @param next The next request mapper in the chain.
     */
    public FilterOptimizingRequestMapper(
            ResourceDictionaries resourceDictionaries,
            RequestMapper<DataApiRequest> next
    ) {
        super(resourceDictionaries, next);
    }

    @Override
    protected DataApiRequest internalApply(DataApiRequest request, ContainerRequestContext context)
            throws RequestValidationException {
        if (request.getApiFilters() == null || request.getApiFilters().isEmpty()) {
            return request;
        }

        ApiFilters newFilters = new ApiFilters();
        for (Map.Entry<Dimension, Set<ApiFilter>> entry : request.getApiFilters().entrySet()) {
            if (!(entry.getKey() instanceof FilterOptimizable)) {
                newFilters.merge(
                        entry.getKey(),
                        entry.getValue(),
                        FILTER_MERGE
                );
                continue;
            }
            FilterOptimizable optimizer = (FilterOptimizable) entry.getKey();
            Collection<ApiFilter> optimizedFilters = optimizer.optimizeFilters(entry.getValue());
            optimizedFilters.forEach(
                    filter ->
                            newFilters.merge(
                                    filter.getDimension(),
                                    Collections.singleton(filter),
                                    FILTER_MERGE
                            )
            );
        }
        return request.withFilters(newFilters);
    }
}
