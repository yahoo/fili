// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

/**
 * Types of legal filter operations.
 */
public enum DefaultFilterOperation implements FilterOperation {
    in,
    notin,
    startswith,
    contains,
    eq
    ;

    @Override
    public String getName() {
        return name();
    }
}
