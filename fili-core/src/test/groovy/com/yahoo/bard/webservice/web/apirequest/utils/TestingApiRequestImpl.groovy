// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.utils

import com.yahoo.bard.webservice.web.ResponseFormatType
import com.yahoo.bard.webservice.web.apirequest.ApiRequestImpl
import com.yahoo.bard.webservice.web.util.PaginationParameters

class TestingApiRequestImpl extends ApiRequestImpl {
    TestingApiRequestImpl() {
        super((ResponseFormatType) null, "", Long.MAX_VALUE, (PaginationParameters) null)
    }
}
