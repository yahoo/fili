// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import java.util.Locale;

/**
 * Response format type.
 */
public enum ResponseFormatType {
    JSON,
    CSV,
    DEBUG,
    JSONAPI;

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ENGLISH);
    }
}
