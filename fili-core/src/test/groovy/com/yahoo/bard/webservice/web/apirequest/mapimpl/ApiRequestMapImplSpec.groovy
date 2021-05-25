// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.mapimpl

import com.yahoo.bard.webservice.web.apirequest.ApiRequest
import com.yahoo.bard.webservice.web.apirequest.ApiRequestImplSpec
import com.yahoo.bard.webservice.web.apirequest.utils.TestingApiRequestProvider

class ApiRequestMapImplSpec extends ApiRequestImplSpec {

    ApiRequest buildApiRequestImpl(String format, String filename, String async, String perPage, String page) {
        return new ApiRequestMapImpl(MapRequestUtil.apiConstructorConverter(format, filename, async, perPage, page)) {}
    }
    def setup() {
        apiRequestImpl = TestingApiRequestProvider.buildMapImpl();
    }
}
