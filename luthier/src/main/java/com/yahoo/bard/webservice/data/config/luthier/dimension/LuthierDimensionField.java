// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.luthier.dimension;

import com.yahoo.bard.webservice.data.dimension.DimensionField;

import java.util.List;
import java.util.Objects;

/**
 * Implementation of DimensionField interface that matches configuration specified in JSON object.
 */
public class LuthierDimensionField implements DimensionField {

    private final String camelName;

    private final String description;

    private final List<String> tags;

    /**
     * Constructor.
     *
     * @param camelName  camelCased name of a dimensionField.
     * @param description  description of that dimensionField.
     * @param tags  A list of Strings that represents the tags of that field
     */
    public LuthierDimensionField(String camelName, String description, List<String> tags) {
        this.camelName = camelName;
        this.description = description;
        this.tags = tags;
    }

    public List<String> getTags() {
        return tags;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public String getName() {
        return camelName;
    }

    @Override
    public String toString() {
        return getName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LuthierDimensionField that = (LuthierDimensionField) o;
        return camelName.equals(that.camelName) &&
                Objects.equals(description, that.description) &&
                Objects.equals(tags, that.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(camelName, description, tags);
    }
}
