// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl;

import com.yahoo.bard.webservice.data.config.dimension.LookupDimensionConfig;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.CascadeExtractionFunction;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.ExtractionFunction;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.LookupExtractionFunction;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.NamespaceLookup;
import com.yahoo.bard.webservice.druid.serializers.LookupDimensionToDimensionSpec;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * LookupDimension creates a Look up dimension based on the namespace chain.
 * <p>
 * {@link LookupDimension} is essentially a special case of {@link RegisteredLookupDimension} using namespace. Hence
 * this class ONLY applies to the Druid namespace lookup serialization. See
 * http://druid.io/docs/latest/querying/dimensionspecs.html#lookup-extraction-function for more details.
 */
@JsonSerialize(using = LookupDimensionToDimensionSpec.class)
public class LookupDimension extends KeyValueStoreDimension implements ExtractionFunctionDimension {

    private final List<String> namespaces;

    /**
     * Constructor.
     *
     * @param lookupDimensionConfig Configuration holder for this dimension
     */
    public LookupDimension(@NotNull LookupDimensionConfig lookupDimensionConfig) {
        super(lookupDimensionConfig);
        this.namespaces = Collections.unmodifiableList(lookupDimensionConfig.getNamespaces());
    }

    public List<String> getNamespaces() {
        return namespaces;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LookupDimension)) {
            return false;
        }

        LookupDimension that = (LookupDimension) o;

        return
            super.equals(that) &&
            Objects.equals(namespaces, that.namespaces);
    }

    /**
     * Build an extraction function model object.
     *
     * @return  Take the internal namespaces and construct a model object for the extraction functions.
     */
    @JsonIgnore
    @Override
    public Optional<ExtractionFunction> getExtractionFunction() {

        List<ExtractionFunction> extractionFunctions = getNamespaces().stream()
                .map(
                        namespace -> new LookupExtractionFunction(
                                new NamespaceLookup(namespace),
                                false,
                                "Unknown " + namespace,
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

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), namespaces);
    }

    @Override
    public String toString() {
        return super.getApiName() + ":" +
               super.getCategory() + ":" +
               namespaces;
    }
}
