// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Interface for tags used for tagging dimension fields to add additional properties implicitly specified by its name.
 */
@FunctionalInterface
public interface Tag {

    /**
     * Gets the String representation of the tag.
     *
     * @return the String representation of the tag
     */
    @JsonValue
    String getName();
}
