// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata;

import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;

import java.util.Optional;

/**
 * Defines the interface of a service that can hold the segment metadata of physical tables.
 *
 * @param <T>  the type that corresponds to the id of the set of segments referenced by a druid query.
 */
@FunctionalInterface
public interface QuerySigningService<T> {

    /**
     * Return an identifier that corresponds to the set of segments that a query references.
     *
     * @param query  The druid aggregation query.
     *
     * @return An optional that identifies the set of segments referenced by this query. The optional is empty if no
     * segment information was available for this query.
     */
    Optional<T> getSegmentSetId(DruidAggregationQuery<?> query);
}
