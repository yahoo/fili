// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.utils

import com.yahoo.bard.webservice.web.apirequest.ApiRequestImpl

<<<<<<< dc42c05507c7823fbfaef0e0657e55e263debd93
class TestingApiRequestImpl extends ApiRequestImpl {
    TestingApiRequestImpl() {
        super(null, Long.MAX_VALUE, null)
=======
import javax.ws.rs.core.Response
import javax.ws.rs.core.UriInfo

class TestingApiRequestImpl extends ApiRequestImpl {
    TestingApiRequestImpl() {
        super(null, Long.MAX_VALUE, Optional.empty(), (UriInfo) null, Response.status(OK))
>>>>>>> Fixing Pagination
    }
}
