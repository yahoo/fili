// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config;

import com.google.common.base.Objects;

/**
 * Simple value object that exposes metadata that many different types of Fili objects can possess.
 */
public class SimpleMetadata implements GlobalMetadata {

    private final String name;
    private final String id;
    private final String description;

    public SimpleMetadata(String id) {
        this(id, id);
    }

    public SimpleMetadata(String id, String name) {
        this(id, name, name);
    }

    /**
     * Constructor.
     *
     * @param id  Unique ID of the object. Can be used by selectors to identify the instance this metadata is attached
     *            to.
     * @param name  Name of the object.
     * @param description  Description of the object.
     */
    public SimpleMetadata(String name, String id, String description) {
        this.id = id;
        this.name = name;
        this.description = description;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return "SimpleMetadata{" +
                "name=" + name +
                ",id=" + id +
                ",description=" + description +
                "}";
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof SimpleMetadata)) {
            return false;
        }
        SimpleMetadata that = (SimpleMetadata) other;

        return Objects.equal(this.name, that.name) &&
                Objects.equal(this.id, that.id) &&
                Objects.equal(this.description, that.description);
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(this.name);
        result = 32 * result + Objects.hashCode(this.id);
        return 32 * result + Objects.hashCode(description);
    }
}
