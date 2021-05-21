// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.utils

import com.yahoo.bard.webservice.web.DefaultResponseFormatType
import com.yahoo.bard.webservice.web.ResponseFormatType
import com.yahoo.bard.webservice.web.apirequest.ApiRequest
import com.yahoo.bard.webservice.web.apirequest.TablesApiRequest
import com.yahoo.bard.webservice.web.apirequest.TablesApiRequestImpl
import com.yahoo.bard.webservice.web.apirequest.beanimpl.ApiRequestBeanImpl
import com.yahoo.bard.webservice.web.apirequest.mapimpl.ApiRequestMapImpl
import com.yahoo.bard.webservice.web.util.BardConfigResources
import com.yahoo.bard.webservice.web.util.PaginationParameters
import com.yahoo.bard.webservice.web.util.PaginationParametersSpec

import com.fasterxml.jackson.annotation.JsonFormat

class TestingApiRequestProvider  {

    private TestingApiRequestProvider() {
        ;
    }

    public static ApiRequest buildBeanImpl() {
        return new ApiRequestBeanImpl(DefaultResponseFormatType.CSV.toString(), "filename", "-1", "1", "1") {};
    }

    public static TablesApiRequest buildTablesBeanImpl(BardConfigResources bardConfigResources) {
        PaginationParameters p = new PaginationParameters(1, 1);
        return new TablesApiRequestImpl("tab1", "filename", "json", "1", "1", bardConfigResources);
    }

    public static ApiRequest buildMapImpl() {
        Map<String, String> values  = new HashMap<String, String>() {{
            put(
                    ApiRequestMapImpl.ASYNC_AFTER_KEY,
                    Long.MAX_VALUE.toString()
            );
        }};
        return new ApiRequestMapImpl(values);
    }
}
