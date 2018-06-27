// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.dimension.extractionfunction;

import java.util.Objects;

/**
 * RegisteredLookup ExtractionFunction that maps dimension values to some corresponding pre-defined values in druid.
 */
public class RegisteredLookupExtractionFunction extends ExtractionFunction {

    private final String lookup;
    private final Boolean retainMissingValue;
    private final String replaceMissingValueWith;
    private final Boolean injective;
    private final Boolean optimize;

    /**
     * Constructs a new {@code RegisteredLookupExtractionFunction} with the specified function attributes.
     *
     * @param lookup  Name of the lookup
     * @param retainMissingValue  A flag indicating whether or not the original dimension value is returned if lookup
     * mapping is not found for a dimension value. It is illegal to set retainMissingValue = true and also specify a
     * replaceMissingValueWith
     * @param replaceMissingValueWith  The default lookup value if lookup mapping is not found for a dimension value.
     * Setting this to "" has the same effect as setting it to null or omitting the property. It is illegal to set
     * retainMissingValue = true and also specify a replaceMissingValueWith
     * @param injective  A flag indicating whether or not to apply some optimization for aggregation query involving
     * lookups given that mapping is one-to-one, may cause undefined behavior if retainMissingValue is false and
     * injective is true
     * @param optimize  A flag indicating whether or not to allow optimization of lookup based extraction filter
     */
    public RegisteredLookupExtractionFunction(
            String lookup,
            Boolean retainMissingValue,
            String replaceMissingValueWith,
            Boolean injective,
            Boolean optimize
    ) {
        super(DefaultExtractionFunctionType.REGISTERED_LOOKUP);
        this.lookup = lookup;
        this.retainMissingValue = retainMissingValue;
        this.replaceMissingValueWith = replaceMissingValueWith;
        this.injective = injective;
        this.optimize = optimize;
    }

    /**
     * Constructs a new {@code RegisteredLookupExtractionFunction} with the specified lookup name.
     * <p>
     * This constructor initializes the following dimension specs to their default values
     * <ul>
     *     <li> retainMissingValue=false
     *     <li> replaceMissingValueWith="Unknown {@code <lookup>}"
     *     <li> injective=false
     *     <li> optimize=true
     * </ul>
     *
     * @param lookup  lookup property specified by the user
     */
    public RegisteredLookupExtractionFunction(String lookup) {
        this(lookup, false, String.format("Unknown %s", lookup), false, true);
    }

    /**
     * Returns the name of this lookup.
     *
     * @return the name of this lookup
     */
    public String getLookup() {
        return lookup;
    }

    /**
     * Returns the flag indicating whether or not the original dimension value is returned if lookup mapping is not
     * found for a dimension value.
     *
     * @return the flag indicating whether or not the original dimension value is returned if lookup mapping is not
     * found for a dimension value
     */
    public Boolean getRetainMissingValue() {
        return retainMissingValue;
    }

    /**
     * Returns the default lookup value if lookup mapping is not found for a dimension value.
     *
     * @return the default lookup value if lookup mapping is not found for a dimension value
     */
    public String getReplaceMissingValueWith() {
        return replaceMissingValueWith;
    }

    /**
     * Returns the flag indicating whether or not to apply some optimization for aggregation query involving lookups.
     *
     * @return the flag indicating whether or not to apply some optimization for aggregation query involving lookups
     */
    public Boolean getInjective() {
        return injective;
    }

    /**
     * Returns the flag indicating whether or not to allow optimization of lookup based extraction filter.
     *
     * @return the flag indicating whether or not to allow optimization of lookup based extraction filter
     */
    public Boolean getOptimize() {
        return optimize;
    }

    // CHECKSTYLE:OFF
    public RegisteredLookupExtractionFunction withLookup(String lookup) {
        return new RegisteredLookupExtractionFunction(lookup, retainMissingValue, replaceMissingValueWith, injective, optimize);
    }

    public RegisteredLookupExtractionFunction withRetainMissingValue(Boolean retainMissingValue) {
        return new RegisteredLookupExtractionFunction(lookup, retainMissingValue, replaceMissingValueWith, injective, optimize);
    }

    public RegisteredLookupExtractionFunction withReplaceMissingValueWith(String replaceMissingValueWith) {
        return new RegisteredLookupExtractionFunction(lookup, retainMissingValue, replaceMissingValueWith, injective, optimize);
    }

    public RegisteredLookupExtractionFunction withInjective(Boolean injective) {
        return new RegisteredLookupExtractionFunction(lookup, retainMissingValue, replaceMissingValueWith, injective, optimize);
    }

    public RegisteredLookupExtractionFunction withOptimize(Boolean optimize) {
        return new RegisteredLookupExtractionFunction(lookup, retainMissingValue, replaceMissingValueWith, injective, optimize);
    }
    // CHECKSTYLE:ON

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                lookup,
                retainMissingValue,
                replaceMissingValueWith,
                injective,
                optimize
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null || getClass() != obj.getClass()) { return false; }
        RegisteredLookupExtractionFunction other = (RegisteredLookupExtractionFunction) obj;

        return super.equals(obj) &&
                Objects.equals(lookup, other.lookup) &&
                Objects.equals(retainMissingValue, other.retainMissingValue) &&
                Objects.equals(replaceMissingValueWith, other.replaceMissingValueWith) &&
                Objects.equals(injective, other.injective) &&
                Objects.equals(optimize, other.optimize);
    }

    /**
     * Returns a string representation of this extraction function.
     * <p>
     * The format of the string is "RegisteredLookupExtractionFunction{type=A, lookup='B', retainMissingValue=C,
     * replaceMissingValueWith='D', injective=E, optimize=F}", where values (A - E) are given by {@link #getType()},
     * {@link #getLookup()}, {@link #getRetainMissingValue()}, {@link #getReplaceMissingValueWith()},
     * {@link #getInjective()}, {@link #getOptimize()}, respectively. Note that there is a single space separating each
     * value after a comma. The lookup name and the default lookup value are surrounded by pairs of single quotes.
     *
     * @return the string representation of this extraction function
     */
    @Override
    public String toString() {
        return String.format(
                "RegisteredLookupExtractionFunction{type=%s, lookup='%s', retainMissingValue=%s, " +
                        "replaceMissingValueWith='%s', injective=%s, optimize=%s}",
                getType(),
                getLookup(),
                getRetainMissingValue(),
                getReplaceMissingValueWith(),
                getInjective(),
                getOptimize()
        );
    }
}
