// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.utils

import org.glassfish.jersey.internal.util.collection.MultivaluedStringMap

import javax.ws.rs.core.MultivaluedMap
import javax.ws.rs.core.PathSegment

class TestPathSegment implements PathSegment {
    String name
    String showFields

    TestPathSegment(String path, String fields) {
        this.name = path
        this.showFields = fields
    }
    @Override
    String getPath() {
        return name
    }

    @Override
    MultivaluedMap<String, String> getMatrixParameters() {
        MultivaluedStringMap map = new MultivaluedStringMap();
        if (showFields != null) {
            map.add("show", showFields)
        }
        return map
    }
}
