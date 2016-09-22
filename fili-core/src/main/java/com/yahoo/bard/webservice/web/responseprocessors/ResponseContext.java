// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionField;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Objects;

/**
 * A container to hold context information associated with a response.
 */
public class ResponseContext extends LinkedHashMap<String, Serializable> {

    /**
     * A Map from Dimension to DimensionFields relevant for a given request. The combination of DimensionApiName and
     * DimensionFieldName helps uniquely identify a DimensionRow
     */
    private final LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> dimensionToDimensionFieldMap;

    /**
     * Constructor.
     * Builds with an empty map of dimensions to dimension fields
     */
    public ResponseContext() {
        dimensionToDimensionFieldMap = new LinkedHashMap<>();
    }

    /**
     * Build a ResponseContext using dimensionToDimensionFieldMap.
     *
     * @param dimensionToDimensionFieldMap  A Map from Dimension to DimensionFields relevant for a given request.
     */
    public ResponseContext(LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> dimensionToDimensionFieldMap) {
        this.dimensionToDimensionFieldMap = dimensionToDimensionFieldMap;
    }

    public LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> getDimensionToDimensionFieldMap() {
        return dimensionToDimensionFieldMap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        ResponseContext that = (ResponseContext) o;
        return super.equals(o) &&
                Objects.equals(this.getDimensionToDimensionFieldMap(), that.getDimensionToDimensionFieldMap());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dimensionToDimensionFieldMap);
    }
}
