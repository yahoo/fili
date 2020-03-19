// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.filter;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.FilterOperation;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Filter factory provides a chain of responsibility for building ApiFilters.
 */
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
     * Data object for collecting the bound arguments needed to build an ApiFilter.
     */
    public static class FilterComponents {
        public final Dimension dimension;
        public final DimensionField dimensionField;
        public final FilterOperation operation;
        public final List<String> values;

        /**
         * Constructor.
         *
         * @param dimension  Dimension for ApiFilter.
         * @param field  Field for ApiFilter.
         * @param operation  Operation for ApiFilter.
         * @param values  Values for ApiFilters.
         */
        public FilterComponents(
                Dimension dimension,
                DimensionField field,
                FilterOperation operation,
                List<String> values
        ) {
            this.dimension = dimension;
            this.dimensionField = field;
            this.operation = operation;
            this.values = values;
        }
    }

    /**
     * Constructor.
     *
     * @param filterFactoryProviders  List of matchers and factory methods.
     * @param defaultFilterFactory  Default factory method for building ApiFilters.
     */
    protected FilterFactory(
            List<Map.Entry<Predicate<FilterComponents>, FilterFactoryFunction>> filterFactoryProviders,
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
