// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.postaggregation;

/**
 * An interface for post aggregation types.
 */
@FunctionalInterface
public interface PostAggregationType {

    /**
     * Return the json equivalent of this post aggregation type.
     *
     * @return The string that corresponds to the json value of this post aggregation type.
     */
    String toJson();
}
