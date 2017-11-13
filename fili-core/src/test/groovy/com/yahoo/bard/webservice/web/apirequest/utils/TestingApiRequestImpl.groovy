// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.utils

import static javax.ws.rs.core.Response.Status.OK

import com.yahoo.bard.webservice.web.apirequest.ApiRequestImpl

import javax.ws.rs.core.Response

class TestingApiRequestImpl extends ApiRequestImpl {
    TestingApiRequestImpl() {
        super(null, Long.MAX_VALUE, null, null, Response.status(OK))
    }
}
