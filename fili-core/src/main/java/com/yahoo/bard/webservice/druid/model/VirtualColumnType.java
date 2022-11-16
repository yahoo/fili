// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Types of virtual columns supported by this application.
 */
@FunctionalInterface
public interface VirtualColumnType {

    /**
     * Get the JSON serialization of the virtual column type.
     *
     * @return the json representation of this type
     */
    @JsonValue
    String toJson();
}
