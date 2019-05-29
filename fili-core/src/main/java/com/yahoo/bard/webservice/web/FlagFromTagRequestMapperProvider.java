// Copyright 2019 Verizon Media Group.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.impl.FlagFromTagDimension;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest;
import com.yahoo.bard.webservice.web.apirequest.DimensionsApiRequest;
import com.yahoo.bard.webservice.web.apirequest.TablesApiRequest;
import com.yahoo.bard.webservice.web.filters.ApiFilters;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.validation.constraints.NotNull;
import javax.ws.rs.container.ContainerRequestContext;

/**
 * Provides mappers to transform API filters for flag from tag dimensions for different request types. A new mapper is
 * generated with each call to the getters.
 */
public class FlagFromTagRequestMapperProvider {

    private final Set<FilterOperation> positiveOps;
    private final Set<FilterOperation> negativeOps;

    private final FilterOperation positiveInvertedFilterOperation;
    private final FilterOperation negativeInvertedFilterOperation;

    /**
     * Builder for a {@link FlagFromTagRequestMapperProvider}.
     */
    public static class Builder {
        public static final Set<FilterOperation> DEFAULT_POSITIVE_OPS = Stream.of(
                DefaultFilterOperation.in,
                DefaultFilterOperation.startswith,
                DefaultFilterOperation.contains,
                DefaultFilterOperation.eq
        ).collect(Collectors.collectingAndThen(
                Collectors.toSet(),
                Collections::unmodifiableSet
        ));

        public static final Set<FilterOperation> DEFAULT_NEGATIVE_OPS = Collections.unmodifiableSet(
                Collections.singleton(
                        DefaultFilterOperation.notin
                )
        );

        public static final FilterOperation DEFAULT_POSITIVE_INVERTED_FILTER_OPERATION = DefaultFilterOperation.eq;
        public static final FilterOperation DEFAULT_NEGATIVE_INVERTED_FILTER_OPERATION = DefaultFilterOperation.notin;


        private Set<FilterOperation> positiveOps = new HashSet<>(DEFAULT_POSITIVE_OPS);
        private Set<FilterOperation> negativeOps = new HashSet<>(DEFAULT_NEGATIVE_OPS);

        private FilterOperation positiveInvertedFilterOperation = DEFAULT_POSITIVE_INVERTED_FILTER_OPERATION;
        private FilterOperation negativeInvertedFilterOperation = DEFAULT_NEGATIVE_INVERTED_FILTER_OPERATION;

        /**
         * Set the accepted set of operations with positive truth value.
         *
         * @param positiveOps the set of ops to be set.
         * @return the builder
         */
        public Builder positiveOps(Set<FilterOperation> positiveOps) {
            this.positiveOps = positiveOps;
            return this;
        }

        /**
         * Set the accepted set of operations with negative truth value.
         *
         * @param negativeOps the set of ops to be set.
         * @return the builder
         */
        public Builder negativeOps(Set<FilterOperation> negativeOps) {
            this.negativeOps = negativeOps;
            return this;
        }

        /**
         * Sets the filter operation to be used if a negative filter needs to be inverted.
         *
         * @param positiveInvertedFilterOperation the filter operation
         * @return the builder
         */
        public Builder positiveInvertedFilterOperation(FilterOperation positiveInvertedFilterOperation) {
            this.positiveInvertedFilterOperation = positiveInvertedFilterOperation;
            return this;
        }

        /**
         * Sets the filter operation to be used if a positive filter needs to be inverted.
         *
         * @param negativeInvertedFilterOperation the filter operation
         * @return the builder
         */
        public Builder negativeInvertedFilterOperation(FilterOperation negativeInvertedFilterOperation) {
            this.negativeInvertedFilterOperation = negativeInvertedFilterOperation;
            return this;
        }

        /**
         * Builds the mapper provider.
         *
         * @return the mapper provider
         */
        public FlagFromTagRequestMapperProvider build() {
            return new FlagFromTagRequestMapperProvider(
                    positiveOps,
                    negativeOps,
                    positiveInvertedFilterOperation,
                    negativeInvertedFilterOperation
            );
        }

        /**
         * Builds a FlagFromTagRequestMapperProvider with default values in all optional parameters.
         *
         * @return the mapper provider
         */
        public static FlagFromTagRequestMapperProvider simpleProvider() {
            return new Builder().build();
        }
    }

    /**
     * Constructor. Uses custom positive and negative filter sets. If one of the sets is empty the default set is used
     * instead.
     *
     * @param positiveOps Set of valid positive filter operations for flag from tag dimensions.
     * @param negativeOps Set of valid negative filter operations for flag from tag dimensions.
     * @param positiveInvertedFilterOperation The positive filter operation to use if a negative filter needs to be
     * inverted.
     * @param negativeInvertedFilterOperation The negative filter operation use if a positive filter needs to be
     * inverted.
     */
    private FlagFromTagRequestMapperProvider(
            Set<FilterOperation> positiveOps,
            Set<FilterOperation> negativeOps,
            @NotNull FilterOperation positiveInvertedFilterOperation,
            @NotNull FilterOperation negativeInvertedFilterOperation
    ) {
        this.positiveOps = Collections.unmodifiableSet(positiveOps);
        this.negativeOps = Collections.unmodifiableSet(negativeOps);

        this.positiveInvertedFilterOperation = Objects.requireNonNull(positiveInvertedFilterOperation);
        this.negativeInvertedFilterOperation = Objects.requireNonNull(negativeInvertedFilterOperation);
    }

    /**
     * Creates a new {@link RequestMapper} for {@link DataApiRequest} that converts all {@link ApiFilter} on
     * {@link FlagFromTagDimension} to ApiFilters on the filtering dimension of the FlagFromTagDimension.
     *
     * @param dictionaries  The dictionaries to use for request mapping.
     * @return the request mapper.
     */
    public RequestMapper<DataApiRequest> dataMapper(ResourceDictionaries dictionaries) {
        return new RequestMapper<DataApiRequest>(dictionaries) {
            @Override
            public DataApiRequest apply(DataApiRequest request, ContainerRequestContext context)
                    throws RequestValidationException {
                return request.withFilters(transformApiFilters(request.getApiFilters()));
            }
        };
    }

    /**
     * Creates a new {@link RequestMapper} for {@link DimensionsApiRequest} that converts all {@link ApiFilter} on
     * {@link FlagFromTagDimension} to ApiFilter on the filtering dimension of the FlagFromTagDimension.
     *
     * @param dictionaries  The dictionaries to use for request mapping.
     * @return the request mapper.
     */
    public RequestMapper<DimensionsApiRequest> dimensionsMapper(ResourceDictionaries dictionaries) {
        return new RequestMapper<DimensionsApiRequest>(dictionaries) {
            @Override
            public DimensionsApiRequest apply(DimensionsApiRequest request, ContainerRequestContext context)
                    throws RequestValidationException {
                return request.withFilters(transformApiFilterSet(request.getFilters()));
            }
        };
    }

    /**
     * Creates a new {@link RequestMapper} for {@link TablesApiRequest} that converts all {@link ApiFilter} on
     * {@link FlagFromTagDimension} to ApiFilters on the filtering dimension of the FlagFromTagDimension.
     *
     * @param dictionaries  The dictionaries to use for request mapping.
     * @return the request mapper.
     */
    public RequestMapper<TablesApiRequest> tablesMapper(ResourceDictionaries dictionaries) {
        return new RequestMapper<TablesApiRequest>(dictionaries) {
            @Override
            public TablesApiRequest apply(TablesApiRequest request, ContainerRequestContext context)
                    throws RequestValidationException {
                return request.withFilters(transformApiFilters(request.getApiFilters()));
            }
        };
    }

    /**
     * Transforms all filters on {@link FlagFromTagDimension} into filters on the underlying filtering dimension. Any
     * filters on non-FlagFromTag dimensions are maintained.
     *
     * @param requestFilters The set of api filters that may contain filters to transform.
     * @return the ApiFilters with all FlagFromTag dimensions transformed.
     */
    private ApiFilters transformApiFilters(ApiFilters requestFilters) {
        ApiFilters newFilters = new ApiFilters();
        for (Map.Entry<Dimension, Set<ApiFilter>> entry : requestFilters.entrySet()) {
            if (!(entry.getKey() instanceof FlagFromTagDimension)) {
                newFilters.put(entry.getKey(), entry.getValue());
                continue;
            }
            newFilters.put(
                    ((FlagFromTagDimension) entry.getKey()).getFilteringDimension(),
                    transformApiFilterSet(entry.getValue())
            );
        }
        return newFilters;
    }

    /**
     * Transforms the {@link ApiFilter}s based on {@link FlagFromTagDimension}s into filters based on the filtering
     * dimension backing the FlagFromTagDimension. Filters NOT based on a FlagFromTagDimension are
     * maintained and not transformed.
     *
     * @param apiFilters The set of filters to check and transform if applicable.
     * @return The transformed set of filters.
     */
    private Set<ApiFilter> transformApiFilterSet(Set<ApiFilter> apiFilters) {
        Set<ApiFilter> newFilters = new HashSet<>();
        for (ApiFilter filter : apiFilters) {
            ApiFilter newFilter = filter;
            if (newFilter.getDimension() instanceof FlagFromTagDimension) {
                validateFlagFromTagFilter(newFilter);

                FlagFromTagDimension dim = (FlagFromTagDimension) newFilter.getDimension();
                newFilter = new ApiFilter(
                        dim.getFilteringDimension(),
                        dim.getFilteringDimension().getKey(),
                        transformFilterOperation(
                                dim,
                                filter.getOperation(),
                                filter.getValues().iterator().next()
                        ),
                        Collections.singleton(dim.getTagValue())
                );
            }
            newFilters.add(newFilter);
        }
        return newFilters;
    }

    /**
     * Validates the provided {@link ApiFilter}. The {@link ApiFilter} MUST be on a FlagFromTag dimension; otherwise
     * it is ignored.
     *
     * @param filter The ApiFilter to validate
     */
    private void validateFlagFromTagFilter(ApiFilter filter) {
        if (!(filter.getDimension() instanceof FlagFromTagDimension)) {
            return;
        }

        FlagFromTagDimension dim = (FlagFromTagDimension) filter.getDimension();
        Set<String> filterValues = filter.getValues();

        // requested values should really only contain a single value. If it contains both true and false values we
        // need to fail the nonsensical filter, as there is no way to transform that into a meaningful api filter.
        Set<String> validFilterValues = Stream.of(dim.getTrueValue(), dim.getFalseValue()).collect(Collectors.toSet());
        if (filterValues.size() != 1 || validFilterValues.stream().noneMatch(filterValues::contains)) {
            throw new BadApiRequestException(
                    String.format(
                            "Filter on dimension %s formatted incorrectly. Flag dimensions must filter on " +
                                    "exactly one of the following flag values: %s",
                            filter.getDimension().getApiName(),
                            String.join(", ", validFilterValues)
                    ));
        }

        // The filter operation should be in either the positive or negative operation sets
        if (
                Stream.of(positiveOps, negativeOps)
                        .flatMap(Set::stream)
                        .noneMatch(op -> op.equals(filter.getOperation()))
        ) {
            throw new BadApiRequestException(
                    String.format(
                            "Dimension %s doesn't support the operation %s. Try using one of the " +
                                    "following operations: %s",
                            filter.getDimension().getApiName(),
                            filter.getOperation(),
                            String.join(
                                    ", ",
                                    Stream.of(
                                            positiveOps,
                                            negativeOps
                                    ).flatMap(Set::stream).map(FilterOperation::getName).collect(Collectors.toSet())
                            )
                    )
            );
        }
    }

    /**
     * Transforms the filter operation if required. The combination of filter operation and flag value needs to be
     * converted into the equivalent filter operation on the tag value. For example, the filter/flag value combination:
     *  notin[false_value]
     *
     *  must be converted to the combination:
     *  in[tag_value]
     *
     *  as there is no equivalent to a negative tag value to match directly to the negative flag.
     *
     * @param dimension The flag from tag dimension
     * @param op The original filter operation
     * @param filterValue The flag value being filtered on
     * @return the filter operation required for the new tag filter
     */
    private FilterOperation transformFilterOperation(
            FlagFromTagDimension dimension,
            FilterOperation op,
            String filterValue
    ) {
        // If the filter value is the negative value, the truth of the operation must be inverted.
        FilterOperation newOp = op;
        if (filterValue.equalsIgnoreCase(dimension.getFalseValue())) {
            if (positiveOps.contains(op)) {
                newOp = negativeInvertedFilterOperation;
            } else {
                newOp = positiveInvertedFilterOperation;
            }
        }
        return newOp;
    }
}
