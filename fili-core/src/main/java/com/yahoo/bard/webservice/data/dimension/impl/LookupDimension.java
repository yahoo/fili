// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl;

import com.yahoo.bard.webservice.data.config.dimension.LookupDimensionConfig;
import com.yahoo.bard.webservice.druid.serializers.LookupDimensionToDimensionSpec;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.validation.constraints.NotNull;

/**
 * LookupDimension creates a Look up dimension based on the namespace chain.
 */
@JsonSerialize(using = LookupDimensionToDimensionSpec.class)
public class LookupDimension extends KeyValueStoreDimension {

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
