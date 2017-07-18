// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.data.dimension.impl;

import com.yahoo.fili.webservice.data.config.dimension.RegisteredLookupDimensionConfig;
import com.yahoo.fili.webservice.druid.serializers.LookupDimensionToDimensionSpec;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.validation.constraints.NotNull;

/**
 * RegisteredLookupDimension creates a registered look up dimension based on the lookup chain.
 */
@JsonSerialize(using = LookupDimensionToDimensionSpec.class)
public class RegisteredLookupDimension extends KeyValueStoreDimension {

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
}
