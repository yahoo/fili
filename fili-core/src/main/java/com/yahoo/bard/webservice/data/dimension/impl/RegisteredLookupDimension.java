// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl;

import com.yahoo.bard.webservice.data.config.dimension.RegisteredLookupDimensionConfig;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.CascadeExtractionFunction;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.ExtractionFunction;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.RegisteredLookupExtractionFunction;
import com.yahoo.bard.webservice.druid.serializers.LookupDimensionToDimensionSpec;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.codehaus.jackson.annotate.JsonIgnore;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * RegisteredLookupDimension creates a registered look up dimension based on the lookup chain.
 */
@JsonSerialize(using = LookupDimensionToDimensionSpec.class)
public class RegisteredLookupDimension extends ExtractionFunctionDimension {

    private final List<String> lookups;

    /**
     * Constructor.
     *
     * @param registeredLookupDimensionConfig Configuration holder for this dimension
     */
    public RegisteredLookupDimension(@NotNull RegisteredLookupDimensionConfig registeredLookupDimensionConfig) {
        super(registeredLookupDimensionConfig);
        this.lookups = Collections.unmodifiableList(registeredLookupDimensionConfig.getLookups());
    }

    public List<String> getLookups() {
        return lookups;
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

        return
            super.equals(that) &&
            Objects.equals(lookups, that.lookups);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), lookups);
    }

    @Override
    public String toString() {
        return super.getApiName() + ":" +
               super.getCategory() + ":" +
                lookups;
    }

    /**
     * Build an extraction function model object.
     *
     * @return  Take the internal namespaces and construct a model object for the extraction functions.
     */
    @Override
    @JsonIgnore
    public Optional<ExtractionFunction> getExtractionFunction() {

        List<ExtractionFunction> extractionFunctions = getLookups().stream()
                .map(
                        lookup -> new RegisteredLookupExtractionFunction(
                                lookup,
                                false,
                                "Unknown " + lookup,
                                false,
                                true
                        )
                ).collect(Collectors.toList());

        return Optional.ofNullable(
                extractionFunctions.size() > 1 ?
                        new CascadeExtractionFunction(extractionFunctions) :
                        extractionFunctions.size() == 1 ? extractionFunctions.get(0) : null
        );
    }
}
