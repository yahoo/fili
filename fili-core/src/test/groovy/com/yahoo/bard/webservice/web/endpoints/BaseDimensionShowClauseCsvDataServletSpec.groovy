// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

abstract class BaseDimensionShowClauseCsvDataServletSpec extends BaseDimensionShowClauseDataServletSpec {

    @Override
    Map<String, List<String>> getQueryParams() {
        [
                "metrics" : ["width"],
                "dateTime": ["2014-06-02%2F2014-06-09"],
                "format": ["csv"]
        ]
    }

    @Override
    boolean validateExpectedApiResponse(String expectedApiResponse) {
        // NoOp, we don't have a way to validate CSV right now
        true
    }

    @Override
    boolean compareResult(String result, String expectedResult) {
        result == expectedResult
    }
}
