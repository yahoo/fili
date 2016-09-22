// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.dimension.extractionfunction;

import java.util.Objects;

/**
 * Lookup property of Lookup Extraction Function using namespace as key value mapping.
 */
public class NamespaceLookup extends Lookup {
    private final String namespace;

    /**
     * Constructor.
     *
     * @param namespace  name of the namespace containing the mapping provided by the user.
     */
    public NamespaceLookup(String namespace) {
        super("namespace");
        this.namespace = namespace;
    }

    public String getNamespace() {
        return namespace;
    }

    // CHECKSTYLE:OFF
    public NamespaceLookup withNamespace(String namespace) {
        return new NamespaceLookup(namespace);
    }
    // CHECKSTYLE:ON

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), namespace);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null || getClass() != obj.getClass()) { return false; }
        NamespaceLookup other = (NamespaceLookup) obj;
        return super.equals(obj) && Objects.equals(namespace, other.namespace);
    }
}
