// Copyright 2018 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.filter;

import com.fasterxml.jackson.annotation.JsonValue;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.util.EnumUtils;

import javax.validation.constraints.Null;
import java.util.Objects;

/**
 * Bound filters supports filtering on ranges of dimension values.
 */
public class BoundFilter extends DimensionalFilter<BoundFilter> {

    /**
     * Ordering specified for the range filtering.
     */
    public enum Ordering {
        LEXICOGRAPHIC,
        ALPHANUMERIC,
        NUMERIC,
        STRLEN;

        final String orderingType;

        /**
         * Constructor.
         */
        Ordering() {
            this.orderingType = EnumUtils.enumJsonName(this);
        }

        /**
         * Get the JSON representation of this class.
         *
         * @return the JSON representation.
         */
        @JsonValue
        public String toJson() {
            return orderingType;
        }
    }

    private final String lower;
    private final String upper;
    private final Boolean lowerStrict;
    private final Boolean upperStrict;
    private final Ordering ordering;

    /**
     * Constructor.
     *
     * @param dimension The druid dimension to be filtered
     * @param lower The lower bound of the dimension value to be filtered (Optional)
     * @param upper The upper bound of the dimension value to be filtered (Optional)
     * @param lowerStrict Boolean to enable/ disable strict filtering for lower bounds (Optional)
     * @param upperStrict Boolean to enable/ disable strict filtering for upper bounds (Optional)
     * @param ordering The Ordering to be applied for the dimension filtering
     */
    public BoundFilter (
            Dimension dimension,
            @Null String lower,
            @Null String upper,
            @Null Boolean lowerStrict,
            @Null Boolean upperStrict,
            @Null Ordering ordering
    ) {
        super(dimension, DefaultFilterType.BOUND);
        this.lower = lower;
        this.upper = upper;
        this.lowerStrict = lowerStrict;
        this.upperStrict = upperStrict;
        this.ordering = ordering;
    }

    public String getLower() {
        return this.lower;
    }

    public String getUpper() {
        return this.upper;
    }

    public Boolean isLowerStrict() {
        return this.lowerStrict;
    }

    public Boolean isUpperStrict() {
        return this.upperStrict;
    }

    public Ordering getOrdering() {
        return this.ordering;
    }

    //CHECKSTYLE:OFF
    @Override
    public BoundFilter withDimension(Dimension dimension) {
        return new BoundFilter(dimension, null, null, false, false, null);
    }

    public BoundFilter withLowerBound(String lower) {
        return new BoundFilter(getDimension(), lower, null, false, false, null);
    }

    public BoundFilter withUpperBound(String upper) {
        return new BoundFilter(getDimension(), null, upper, false, false, null);
    }

    public BoundFilter withLowerAndUpperBound(String lower, String upper) {
        return new BoundFilter(getDimension(), lower, upper, false, false, null);
    }

    public BoundFilter withLowerBoundStrict(String lower, Boolean lowerStrict) {
        return new BoundFilter(getDimension(), lower, null, lowerStrict, false, null);
    }

    public BoundFilter withUpperBoundStrict(String upper, Boolean upperStrict) {
        return new BoundFilter(getDimension(), null, upper, false, upperStrict, null);
    }

    public BoundFilter withLowerBoundStrictUpperBound(String lower, String upper, Boolean lowerStrict) {
        return new BoundFilter(getDimension(), lower, upper, lowerStrict, null, null);
    }

    public BoundFilter withLowerBoundUpperBoundStrict(String lower, String upper, Boolean upperStrict) {
        return new BoundFilter(getDimension(), lower, upper, null, upperStrict, null);
    }

    public BoundFilter withLowerBoundStrictUpperBoundStrict(String lower, String upper, Boolean lowerStrict, Boolean upperStrict) {
        return new BoundFilter(getDimension(), lower, upper, lowerStrict, upperStrict, null);
    }
    public BoundFilter withLowerBoundOrdering(String lower, Ordering ordering) {
        return new BoundFilter(getDimension(), lower, null, false, false, ordering);
    }

    public BoundFilter withUpperBoundOrdering(String upper, Ordering ordering) {
        return new BoundFilter(getDimension(), null, upper, false, false, ordering);
    }

    public BoundFilter withLowerAndUpperBoundOrdering(String lower, String upper, Ordering ordering) {
        return new BoundFilter(getDimension(), lower, upper, false, false, ordering);
    }

    public BoundFilter withLowerBoundStrictOrdering(String lower, Boolean lowerStrict, Ordering ordering) {
        return new BoundFilter(getDimension(), lower, null, lowerStrict, false, ordering);
    }

    public BoundFilter withUpperBoundStrictOrdering(String upper, Boolean upperStrict, Ordering ordering) {
        return new BoundFilter(getDimension(), null, upper, false, upperStrict, ordering);
    }

    public BoundFilter withLowerBoundStrictUpperBoundOrdering(String lower, String upper, Boolean lowerStrict, Ordering ordering) {
        return new BoundFilter(getDimension(), lower, upper, lowerStrict, null, ordering);
    }

    public BoundFilter withLowerBoundUpperBoundStrictOrdering(String lower, String upper, Boolean upperStrict, Ordering ordering) {
        return new BoundFilter(getDimension(), lower, upper, null, upperStrict, ordering);
    }

    public BoundFilter withLowerBoundStrictUpperBoundStrictOrdering(String lower, String upper, Boolean lowerStrict, Boolean upperStrict, Ordering ordering) {
        return new BoundFilter(getDimension(), lower, upper, lowerStrict, upperStrict, ordering);
    }

    //CHECKSTYLE:ON
    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), DefaultFilterType.BOUND);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null || getClass() != obj.getClass()) { return false; }
        return super.equals(obj);
    }
}
