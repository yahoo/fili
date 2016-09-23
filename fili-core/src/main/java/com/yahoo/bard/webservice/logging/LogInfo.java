// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Interface for enums or classes that are meant to enclose log related information that can be exported to JSON format.
 */
public interface LogInfo {

    /**
     * Get the name of this logging information part.
     * The default implementation returns the simple name of the class or enum that implements this interface.
     *
     * @return The name of that corresponds to this logging information part.
     */
    @JsonIgnore
    default String getName() {
        return this.getClass().getSimpleName();
    }
}
