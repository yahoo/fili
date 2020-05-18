// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.orderby;

import java.util.Locale;

/**
 * Enum for sort direction.
 */
public enum SortDirection {
    ASC,
    DESC;

    /**
     * Match a string to the sort direction.
     *
     * Left in as a bulwark against the possibility of null weighting sort directions later.
     *
     * @param name  The requested sort direction.
     *
     * @return `ASC` is the sort direction starts with "ASC", `DESC` if it starts with "DESC", otherwise error
     */
    static SortDirection valueByName(String name) {
        if (name.toUpperCase(Locale.ENGLISH).startsWith("ASC")) {
            return ASC;
        } else if (name.toUpperCase(Locale.ENGLISH).startsWith("DESC")) {
            return DESC;
        }
        throw new IllegalArgumentException(String.format("Unparseable sort direction: %s", name));
    }
}
