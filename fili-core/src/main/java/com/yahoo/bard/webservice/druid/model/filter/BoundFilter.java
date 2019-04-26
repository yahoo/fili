// Copyright 2018 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.filter;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.Ordering;

import java.util.Objects;

/**
 * Bound filters supports filtering on ranges of dimension values.
 */
public class BoundFilter extends DimensionalFilter<BoundFilter> {

    private final String lower;
    private final String upper;
    /**
     * <p>If true, performs exclusive lower bound (less than); else performs inclusive lower bound (less than equal to)
     * by default.</p>
     */
    private final Boolean lowerStrict;
    /**
     * <p>If true, performs exclusive upper bound (greater than); else performs inclusive upper bound (greater than
     * equal to) by default.</p>
     */
    private final Boolean upperStrict;
    /**
     * <p>
     *     Specifies the sorting order to be specified to the DRUID Filter Query. The default sorting order is
     *     <b>Lexicographic</b>
     *     @see <a href="http://druid.io/docs/latest/querying/sorting-orders.html">Sorting orders</a>
     * </p>
     */
    private final Ordering ordering;

    /**
     * Constructor.
     *
     * @param dimension The druid dimension to be filtered
     * @param lower The lower bound of the dimension value to be filtered (Optional)
     * @param upper The upper bound of the dimension value to be filtered (Optional)
     */
    public BoundFilter (
            Dimension dimension,
            String lower,
            String upper
    ) {
        super(dimension, DefaultFilterType.BOUND);
        this.lower = lower;
        this.upper = upper;
        this.lowerStrict = null;
        this.upperStrict = null;
        this.ordering = null;
    }

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
            String lower,
            String upper,
            Boolean lowerStrict,
            Boolean upperStrict,
            Ordering ordering
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

    /**
     * Returns a BoundFilter with upper bound value specified.
     *
     * @param dimension The druid dimension to be filtered
     * @param value The bound value to be used
     * @param inclusive A boolean that indicates whether to include the bounds or not
     *
     * @return BoundFilter with upper bound
     */
    public static BoundFilter buildUpperBoundFilter(Dimension dimension, String value, Boolean inclusive) {
        return new BoundFilter(dimension, null, value, null, !inclusive, null);
    }

    /**
     * Returns a BoundFilter with lower bound value specified.
     *
     * @param dimension The druid dimension to be filtered
     * @param value The bound value to be used
     * @param inclusive A boolean that indicates whether to include the bounds or not
     *
     * @return BoundFilter with lower bound
     */
    public static BoundFilter buildLowerBoundFilter(Dimension dimension, String value, Boolean inclusive) {
        return new BoundFilter(dimension, value, null, !inclusive, null, null);
    }

    //CHECKSTYLE:OFF
    @Override
    public BoundFilter withDimension(Dimension dimension) {
        return new BoundFilter(dimension, getLower(), getUpper(), isLowerStrict(), isUpperStrict(), getOrdering());
    }

    public BoundFilter withLowerBound(String lower) {
        return new BoundFilter(getDimension(), lower, getUpper(), isLowerStrict(), isUpperStrict(), getOrdering());
    }

    public BoundFilter withUpperBound(String upper) {
        return new BoundFilter(getDimension(), getLower(), upper, isLowerStrict(), isUpperStrict(), getOrdering());
    }

    public BoundFilter withLowerBoundStrict(Boolean lowerStrict) {
        return new BoundFilter(getDimension(), getLower(), getUpper(), lowerStrict, isUpperStrict(), getOrdering());
    }

    public BoundFilter withUpperBoundStrict(Boolean upperStrict) {
        return new BoundFilter(getDimension(), getLower(), getUpper(), isLowerStrict(), upperStrict, getOrdering());
    }

    public BoundFilter withOrdering(Ordering ordering) {
        return new BoundFilter(getDimension(), getLower(), getUpper(), isLowerStrict(), isUpperStrict(), ordering);
    }
    //CHECKSTYLE:ON

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                DefaultFilterType.BOUND,
                lower,
                upper,
                lowerStrict,
                upperStrict,
                ordering
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null || getClass() != obj.getClass()) { return false; }
        BoundFilter other = (BoundFilter) obj;

        return  super.equals(obj) &&
                Objects.equals(lower, other.lower) &&
                Objects.equals(upper, other.upper) &&
                Objects.equals(lowerStrict, other.lowerStrict) &&
                Objects.equals(upperStrict, other.upperStrict) &&
                Objects.equals(ordering, other.ordering)
        ;
    }

    @Override
    public String toString() {
        return String.format("Lower: [%s] strict %s, Upper: [%s] strict %s, ordering: %s",
                lower == null ? "" : lower,
                lowerStrict == null ? "?" : lowerStrict.toString(),
                upper == null ? "" : upper,
                upperStrict == null ? "?" : upperStrict.toString(),
                ordering == null ? "" : ordering.toString()
        );
    }
}
