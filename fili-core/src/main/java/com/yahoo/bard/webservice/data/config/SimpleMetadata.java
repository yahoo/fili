// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config;

import com.google.common.base.Objects;

/**
 * Simple value object that exposes metadata that many different types of Fili objects can possess.
 */
public class SimpleMetadata {

    /**
     * Builder object to construct a SimpleMetadata instance.
     */
    public static class Builder {

        private String name;
        private String id;
        private String description;

        /**
         * Constructor. Private, use static factory method {@link SimpleMetadata#builder(String)}.
         *
         * @param name  Simple metadata REQUIRES a name, all other fields are optional.
         */
        private Builder(String name) {
            this.name = name;
            this.id = name;
            this.description = name;
        }

        /**
         * Fluid setter for id field.
         *
         * @param id  Id. // TODO what does id represent?
         * @return the in-progress builder.
         */
        public Builder id(String id) {
            this.id = id;
            return this;
        }

        /**
         * Fluid setter for description field.
         *
         * @param description  Description of the object this metadata is attached to.
         * @return the in-progress builder.
         */
        public Builder description(String description) {
            this.description = description;
            return this;
        }

        /**
         * Finalizes the data currently in the builder as a SimpleMetadata instance.
         *
         * @return the SimpleMetadata instance.
         */
        public SimpleMetadata build() {
            return new SimpleMetadata(name, id, description);
        }
    }

    /**
     * Static factory method for the builder object used to build instances of this class.
     *
     * @param name  Name is the only REQUIRED field for a SimpleMetadata instance.
     * @return the fresh builder.
     */
    public static Builder builder(String name) {
        return new Builder(name);
    }

    private final String name;
    private final String id;
    private final String description;

    /**
     * Constructor.
     *
     * @param id  Unique ID of the object. Can be used by selectors to identify the instance this metadata is attached
     *            to.
     * @param name  Name of the object.
     * @param description  Description of the object.
     */
    private SimpleMetadata(String name, String id, String description) {
        this.id = id;
        this.name = name;
        this.description = description;
    }

    /**
     * Getter.
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Getter.
     *
     * @return the id
     */
    public String getId() {
        return id;
    }

    /**
     * Getter.
     *
     * @return the description
     */
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
