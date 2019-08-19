// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.binders;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.util.Incubating;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.FilterOperation;
import com.yahoo.bard.webservice.web.apirequest.binders.FilterGenerationUtils.FilterComponents;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Filter factory provides a chain of responsibility for building ApiFilters.
 */
//TODO This class feels overly complicated. Do we really need a chain of responsibility for building ApiFilters? I
// think an interface for producing an ApiFilter with a default implementation is sufficient.
@Incubating
public class FilterFactory {

    private final FilterFactoryFunction defaultFilterFactory;

    private final List<Map.Entry<Predicate<FilterComponents>, FilterFactoryFunction>> filterFactoryProviders;

    /**
     * Typedef for Filter Factory methods.
     */
    interface FilterFactoryFunction extends Function<FilterComponents, ApiFilter> { }

    public List<Map.Entry<Predicate<FilterComponents>, FilterFactoryFunction>> getFilterFactoryProviders() {
        return filterFactoryProviders;
    }

    /**
     * Default constructor for FilterFactory.
     *
     * This implements only a default proxy to the ApiFilter constructor.
     */
    public FilterFactory() {
        this(
                new ArrayList<>(),
                component -> new ApiFilter(
                        component.dimension,
                        component.dimensionField,
                        component.operation,
                        new LinkedHashSet<>(component.values)
                )
        );
    }

    /**
     * Constructor.
     *
     * @param filterFactoryProviders  List of matchers and factory methods.
     * @param defaultFilterFactory  Default factory method for building ApiFilters.
     */
    protected FilterFactory(
            List<Map.Entry<Predicate<FilterGenerationUtils.FilterComponents>, FilterFactoryFunction>> filterFactoryProviders,
            FilterFactoryFunction defaultFilterFactory
    ) {
        this.filterFactoryProviders = filterFactoryProviders;
        this.defaultFilterFactory = defaultFilterFactory;
    }

    /**
     * Build an ApiFilter based on filter components.
     *
     * @param dimension  Dimension for ApiFilter.
     * @param dimensionField  Field for ApiFilter.
     * @param operation  Operation for ApiFilter.
     * @param values  Values for ApiFilters.
     *
     * @return  A newly constructed ApiFilter
     */
    public ApiFilter buildFilter(
            Dimension dimension,
            DimensionField dimensionField,
            FilterOperation operation,
            List<String> values
    ) {
        FilterComponents components = new FilterComponents(dimension, dimensionField, operation, values);
        return filterFactoryProviders.stream()
                .filter(entry -> entry.getKey().test(components))
                .findFirst()
                .map(Map.Entry::getValue)
                .orElse(defaultFilterFactory).apply(components);
    }
}
