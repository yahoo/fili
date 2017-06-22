// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors;

/**
 * Enumerates the list of keys expected to be found in the FullResponseProcessor.
 */
public enum DruidJsonRequestContentKeys {
    ETAG("If-None-Match"),
    NON_EXISTING_ETAG_VALUE("non-existing-etag")
    ;

    private final String name;

    /**
     * Constructor.
     *
     * @param name  Name of the context key
     */
    DruidJsonRequestContentKeys(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
