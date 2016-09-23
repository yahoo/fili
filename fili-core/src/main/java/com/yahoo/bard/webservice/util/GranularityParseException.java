// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.UNKNOWN_GRANULARITY;

/**
 * Thrown when there's a problem parsing a string into a Granularity.
 */
public class GranularityParseException extends Exception {

    /**
     * Constructor.
     *
     * @param value  String that was not able to be parsed into a Granularity.
     */
    public GranularityParseException(String value) {
        super(UNKNOWN_GRANULARITY.format(value));
    }
}
