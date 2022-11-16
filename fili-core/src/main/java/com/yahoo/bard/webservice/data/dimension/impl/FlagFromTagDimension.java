// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl;

import com.yahoo.bard.webservice.data.FilterOptimizable;
import com.yahoo.bard.webservice.data.config.dimension.FlagFromTagDimensionConfig;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException;
import com.yahoo.bard.webservice.web.FilterOperation;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Build a Flag dimension with a simple true and false filter corresponding to a multivalued 'tag' dimension.
 */
public class FlagFromTagDimension extends RegisteredLookupDimension implements FilterOptimizable {

    private final Map<String, DimensionRow> rowMap;

    private final Dimension filteringDimension;

    private final String tagValue;
    private final String trueValue;
    private final String falseValue;

    private final Set<FilterOperation> positiveOps;
    private final Set<FilterOperation> negativeOps;

    private final FilterOperation positiveInvertedFilterOperation;
    private final FilterOperation negativeInvertedFilterOperation;


    /**
     * Create a flag dimension based on a tag dimension.
     *
     * A flag dimension has values true and false, and corresponds to another dimension that has or doesn't have
     * a given identifier in a multivalued list of identifiers.  The expected contract for the flag dimension
     * is that it will have one dimension that can be filtered using default serialization, substituting the
     * expression tag|key-in[true] with flag|key-eq[flagName] where flag is the targeted value.  The expectation
     * for grouping is that a comma delimited string dimension will be present for which a custom extraction function
     * will be used for grouping.
     *
     * @param config The config for this dimension
     * @param dimensionDictionary populated dimension dictionary which is used to resolve the filtering dimension
     */
    public FlagFromTagDimension(FlagFromTagDimensionConfig config, DimensionDictionary dimensionDictionary) {
        super(config);
        this.filteringDimension = dimensionDictionary.findByApiName(config.filteringDimensionApiName);
        this.tagValue = config.tagValue;
        this.trueValue = config.trueValue;
        this.falseValue = config.falseValue;
        this.positiveOps = config.positiveOps;
        this.negativeOps = config.negativeOps;
        this.positiveInvertedFilterOperation = config.positiveInvertedFilterOperation;
        this.negativeInvertedFilterOperation = config.negativeInvertedFilterOperation;
        this.rowMap = Collections.unmodifiableMap(config.rowMap);
    }

    @Override
    public void addDimensionRow(DimensionRow dimensionRow) {
        throw new UnsupportedOperationException("Dimension values for Tag Dimensions are immutable");
    }

    @Override
    public void addAllDimensionRows(Set<DimensionRow> dimensionRows) {
        throw new UnsupportedOperationException("Dimension values for Tag Dimensions are immutable");
    }

    @Override
    public DimensionRow findDimensionRowByKeyValue(String value) {
        return rowMap.getOrDefault(value, null);
    }

    public Dimension getFilteringDimension() {
        return filteringDimension;
    }

    public String getTagValue() {
        return tagValue;
    }

    public String getTrueValue() {
        return trueValue;
    }

    public String getFalseValue() {
        return falseValue;
    }

    @Override
    public Collection<ApiFilter> optimizeFilters(Collection<ApiFilter> filters) {
        return transformApiFilterSet(filters);
    }

    /**
     * Transforms the {@link ApiFilter}s based on {@link FlagFromTagDimension}s into filters based on the filtering
     * dimension backing the FlagFromTagDimension. Filters NOT based on a FlagFromTagDimension are
     * maintained and not transformed.
     *
     * @param apiFilters The set of filters to check and transform if applicable.
     * @return The transformed set of filters.
     */
    private Set<ApiFilter> transformApiFilterSet(Collection<ApiFilter> apiFilters) {
        Set<ApiFilter> newFilters = new HashSet<>();
        for (ApiFilter filter : apiFilters) {
            ApiFilter newFilter = filter;
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
            throw new IllegalArgumentException(
                    String.format(
                            "Tried to optimize non-FlagFromTag dimension %s using FlagFromTag dimension %s",
                            filter.getDimension().getApiName(),
                            getApiName()
                    )
            );
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FlagFromTagDimension)) {
            return false;
        }

        FlagFromTagDimension that = (FlagFromTagDimension) o;

        return super.equals(that) &&
                Objects.equals(getFilteringDimension(), that.getFilteringDimension()) &&
                Objects.equals(getTagValue(), that.getTagValue()) &&
                Objects.equals(getTrueValue(), that.getTrueValue()) &&
                Objects.equals(getFalseValue(), that.getFalseValue()) &&
                Objects.equals(positiveOps, that.positiveOps) &&
                Objects.equals(negativeOps, that.negativeOps) &&
                Objects.equals(positiveInvertedFilterOperation, that.positiveInvertedFilterOperation) &&
                Objects.equals(negativeInvertedFilterOperation, that.negativeInvertedFilterOperation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                getTagValue(),
                getTrueValue(),
                getFalseValue(),
                positiveOps,
                negativeOps,
                positiveInvertedFilterOperation,
                negativeInvertedFilterOperation
        );
    }

    /**
     * Returns a string representation of this dimension.
     * <p>
     * The format of the string is "FlagFromTagDimension{apiName=XXX, extractionFunctions=YYY, tagValue=AAA,
     * trueValue=BBB, falseValue=CCC}", where XXX is the Webservice API name of this dimension, YYY is the list of
     * registered lookup extraction functions of this dimension, AAA is the druid indexed tag value this dimension is
     * based on, and BBB & CCC are the true and false output values produced by the presence of the tag value in druid.
     * Note that there is a single space separating the values after each comma. The API name, tag value, and truth
     * values are surrounded by pairs of single quotes.
     *
     * @return the string representation of this dimension
     */
    @Override
    public String toString() {
        return String.format(
                "FlagFromTagDimension{apiName='%s', extractionFunctions=%s, " +
                        "tagValue='%s', trueValue='%s', falseValue='%s'}",
                getApiName(),
                getRegisteredLookupExtractionFns(),
                getTagValue(),
                getTrueValue(),
                getFalseValue()
        );
    }
}
