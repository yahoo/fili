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
     * Constructor.
     *
     * @param lookup  lookup's name specified by the user
     * @param retainMissingValue  when true: returns original dimension value if mapping is not found, also note that
     * replaceMissingValueWith must be null or empty string, when false: missing values are treated as missing
     * @param replaceMissingValueWith  replaces dimension values not found in mapping with this value and
     * retainMissingValue must be false if this value is not null or is not empty string
     * @param injective  set to true to apply some optimization given that mapping is one-to-one,
     * may cause undefined behavior if retainMissingValue is false and injective is true
     * @param optimize  set to false to turn off rewriting extraction filter as selector filters
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
     * Convenience Constructor,
     * defaults: retainMissingValue=false, replaceMissingValueWith=null, injective=false, optimize=true.
     *
     * @param lookup  lookup property specified by the user
     */
    public RegisteredLookupExtractionFunction(String lookup) {
        this(lookup, false, null, false, true);
    }

    public String getLookup() {
        return lookup;
    }

    public Boolean getRetainMissingValue() {
        return retainMissingValue;
    }

    public String getReplaceMissingValueWith() {
        return replaceMissingValueWith;
    }

    public Boolean getInjective() {
        return injective;
    }

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
}
