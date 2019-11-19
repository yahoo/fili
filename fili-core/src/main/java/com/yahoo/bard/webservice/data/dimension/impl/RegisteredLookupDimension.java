// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl;

import com.yahoo.bard.webservice.data.config.dimension.RegisteredLookupDimensionConfig;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.CascadeExtractionFunction;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.ExtractionFunction;
import com.yahoo.bard.webservice.druid.serializers.LookupDimensionToDimensionSpec;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;


import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.validation.constraints.NotNull;

/**
 * A dimension whose dimension value is mapped by a registered lookup extraction function or a chain of such functions
 * by Druid.
 */
@JsonSerialize(using = LookupDimensionToDimensionSpec.class)
public class RegisteredLookupDimension extends KeyValueStoreDimension implements ExtractionFunctionDimension {

    private final List<ExtractionFunction> registeredLookupExtractionFns;

    /**
     * Constructs a new {@code RegisteredLookupDimension} with a specified dimension config object.
     *
     * @param registeredLookupDimensionConfig Configuration holder for this dimension
     */
    public RegisteredLookupDimension(@NotNull RegisteredLookupDimensionConfig registeredLookupDimensionConfig) {
        super(registeredLookupDimensionConfig);
        this.registeredLookupExtractionFns = Collections.unmodifiableList(
                registeredLookupDimensionConfig.getRegisteredLookupExtractionFns()
        );
    }

    @Override
    @JsonIgnore
    public Optional<ExtractionFunction> getExtractionFunction() {
        List<ExtractionFunction> extractionFunctions = getRegisteredLookupExtractionFns();
        return Optional.ofNullable(
                extractionFunctions.size() > 1 ?
                        new CascadeExtractionFunction(extractionFunctions) :
                        extractionFunctions.size() == 1 ? extractionFunctions.get(0) : null
        );
    }

    /**
     * Returns an immutable list of registered lookup extraction functions used by this dimension.
     *
     * @return the immutable list of registered lookup extraction functions used by this dimension
     */
    public List<ExtractionFunction> getRegisteredLookupExtractionFns() {
        return registeredLookupExtractionFns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RegisteredLookupDimension)) {
            return false;
        }

        RegisteredLookupDimension that = (RegisteredLookupDimension) o;

        return super.equals(that) && Objects.equals(getExtractionFunction(), that.getExtractionFunction());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), getExtractionFunction());
    }

    /**
     * Returns a string representation of this dimension.
     * <p>
     * The format of the string is "RegisteredLookupDimension{apiName=XXX, registeredLookupExtractionFns=YYY}", where
     * XXX is the Webservice API name of this dimension, and YYY the list of registered lookup extraction functions of
     * this dimension. Note that there is a single space separating the two values after each comma. The API name is
     * surrounded by a pair of single quotes.
     *
     * @return the string representation of this dimension
     */
    @Override
    public String toString() {
        return String.format(
                "RegisteredLookupDimension{apiName='%s', registeredLookupExtractionFns=%s}",
                getApiName(),
                getRegisteredLookupExtractionFns()
        );
    }
}
