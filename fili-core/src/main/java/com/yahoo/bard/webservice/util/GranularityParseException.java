// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.UNKNOWN_GRANULARITY;

public class GranularityParseException extends Exception {

    public GranularityParseException(String value) {
        super(UNKNOWN_GRANULARITY.format(value));
    }
}
