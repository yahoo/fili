// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.dimension.extractionfunction;

import java.util.Objects;

/**
 * Base class model for Lookup property of Lookup Extraction Function.
 */
public abstract class Lookup {
    private final String type;

    /**
     * Constructor.
     *
     * @param type  type of the lookup, can be "map" or "namespace".
     */
    protected Lookup(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null || getClass() != obj.getClass()) { return false; }
        Lookup other = (Lookup) obj;
        return Objects.equals(type, other.type);
    }
}
