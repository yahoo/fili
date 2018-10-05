// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.exception;

import com.yahoo.bard.webservice.data.dimension.FilterBuilderException;
import com.yahoo.bard.webservice.druid.model.builders.DruidOrFilterBuilder;

/**
 * Unchecked exception for situations when too many Druid filters a generated for a Druid query.
 * <p>
 * Dimensions with extremely large cardinalities could result in such error when user put a "contain" filter on the
 * dimension value. For example, a filter of dimension|id-contains[123], where there are 10,000 ID's starting with 123,
 * could generate 10,000 Druid filters using
 * {@link DruidOrFilterBuilder}. This giant query shall eventually
 * timeout the Druid query and returns the timeout error to API user.
 */
public class TooManyDruidFiltersException extends FilterBuilderException {

    /**
     * Constructor.
     *
     * @param message  Message of the exception
     */
    public TooManyDruidFiltersException(String message) {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param cause  Cause of the exception
     */
    public TooManyDruidFiltersException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructor.
     *
     * @param message  Message of the exception
     * @param cause  Cause of the exception
     */
    public TooManyDruidFiltersException(String message, Throwable cause) {
        super(message, cause);
    }
}
