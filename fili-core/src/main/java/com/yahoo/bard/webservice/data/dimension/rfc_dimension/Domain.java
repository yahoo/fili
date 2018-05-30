// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.rfc_dimension;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.dimension.metadata.StorageStrategy;

import org.joda.time.DateTime;

import java.util.Optional;
import java.util.stream.Stream;

/**
 * The client to retrieve dimension rows (dimension values).
 *
 * Cardinality and LastUpdated date are optional contracts that may or may not be available depending on the
 * underlying domain implementation.
 */
public interface Domain {

    String getName();

    DomainSchema getSchema();

    /**
     * Returns the {@link StorageStrategy} of the dimension.
     *
     * @see {@link Dimension#getStorageStrategy()}
     *
     * @return the storage strategy of the dimension.
     */
    StorageStrategy getStorageStrategy();

    /**
     * Get the cardinality of the dimension, if reportable.
     *
     * @see {@link Dimension#getCardinality()}
     *
     * @return cardinality, if measurable, otherwise Optional.empty()
     */
    Optional<Integer> getCardinality();

    /**
     * Get a dimension row given an id.
     *
     * @see {@link Dimension#findDimensionRowByKeyValue(String)}
     *
     * @param keyValue  key value
     *
     * @return a dimension row - returns the first one found if there are multiple, or null if no matching row is found
     */
    DimensionRow findDimensionRowByKeyValue(String keyValue);

    /**
     * Get a dimension row given an id.
     *
     * Supplied as a convenience method for clients able to issue bulk requests and connect them with domains which
     * can more efficiently handle bulk requests.
     *
     * @param keyValues  key values
     *
     * @return a stream of dimension rows or nulls corresponding to the key values in the requesting stream
     */
    default Stream<DimensionRow> findDimensionRowsByKeyValues(Stream<String> keyValues) {
        return keyValues.map(this::findDimensionRowByKeyValue);
    }

    /**
     * Getter for lastUpdated.
     *
     * @see {@link Dimension#getLastUpdated()}
     *
     * @return lastUpdated
     */
    Optional<DateTime> getLastUpdated();
}
