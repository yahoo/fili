// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Types of queries supported by this application.
 */
@FunctionalInterface
public interface QueryType {

    /**
     * Get the JSON serialization of the query type.
     *
     * @return the json representation of this type
     */
    @JsonValue
    String toJson();
}
