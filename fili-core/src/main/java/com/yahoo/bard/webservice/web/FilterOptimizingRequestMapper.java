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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.ws.rs.container.ContainerRequestContext;

public class FilterOptimizingRequestMapper extends RequestMapper<DataApiRequest> {
    /**
     * Constructor.
     *

     * @param resourceDictionaries  The dictionaries to use for request mapping.
     */
    public FilterOptimizingRequestMapper(
            ResourceDictionaries resourceDictionaries
    ) {
        super(resourceDictionaries);
    }

    @Override
    public DataApiRequest apply(DataApiRequest request, ContainerRequestContext context)
            throws RequestValidationException {
        if (request.getApiFilters() == null || request.getApiFilters().isEmpty()) {
            return request;
        }

        ApiFilters newFilters = new ApiFilters();
        for (Map.Entry<Dimension, Set<ApiFilter>> entry : request.getApiFilters().entrySet()) {
            if (! (entry.getKey() instanceof FilterOptimizable)) {
                newFilters.put(entry.getKey(), entry.getValue());
                continue;
            }
            FilterOptimizable optimizer = (FilterOptimizable) entry.getKey();
            Collection<ApiFilter> optimizedFilters = optimizer.optimizeFilters(entry.getValue());
            newFilters.putAll(
                    optimizedFilters.stream().collect(Collectors.toMap(
                            ApiFilter::getDimension,
                            Collections::singleton,
                            (val1, val2) -> Stream.of(val1, val2).flatMap(Set::stream).collect(Collectors.toSet())
                    )));
        }
        return request.withFilters(newFilters);
    }
}
