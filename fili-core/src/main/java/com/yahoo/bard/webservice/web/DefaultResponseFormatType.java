// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import java.util.Locale;
import java.util.Objects;

/**
 * Standard reponse formats.
 */
public enum DefaultResponseFormatType implements ResponseFormatType {
    JSON,
    CSV,
    DEBUG,
    JSONAPI;

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ENGLISH);
    }

    @Override
    public boolean accepts(String responseFormatValue) {
        return Objects.equals(toString(), responseFormatValue);
    }

    @Override
    public boolean accepts(ResponseFormatType formatType) {
        return formatType.accepts(this.toString());
    }
}
