// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * An interface for tag.
 */
public interface Tag {

    /**
     * Gets the String representation of the tag.
     *
     * @return the String representation of the tag
     */
    @JsonValue
    String getName();
}
