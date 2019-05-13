// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import javax.validation.constraints.NotNull;

/**
 * The model object for the Json DimensionUpdateDate data.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DimensionUpdateDate {
    private static final Logger LOG = LoggerFactory.getLogger(DimensionUpdateDate.class);

    private final String name;

    @JsonInclude(Include.NON_NULL)
    private DateTime lastUpdated;

    /**
     * A constructor used to capture dimension update dates.
     *
     * @param name  The dimension name
     */
    public DimensionUpdateDate(@NotNull String name) {
        this(name, null);
        this.lastUpdated = null;
    }

    /**
     * A constructor for use by the web service client to build data objects carrying data from the web service.
     *
     * @param name  The name of the dimension
     * @param lastUpdated  The last updated date for the dimension (is nullable )
     */
    @JsonCreator()
    public DimensionUpdateDate(
            @NotNull @JsonProperty("name") String name,
            @JsonProperty("lastUpdated") DateTime lastUpdated
    ) {
        if (name == null) {
            String message = "Dimension update date name cannot be null.";
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }
        this.name = name;
        this.lastUpdated = lastUpdated;
    }

    public String getName() {
        return name;
    }

    public DateTime getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(DateTime lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    @Override
    public String toString() {
        return "Name: " + name + " updated on: " + lastUpdated;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DimensionUpdateDate) {
            DimensionUpdateDate that = (DimensionUpdateDate) obj;
            return Objects.equals(this.getName(), that.getName());
        }
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name);
    }
}
