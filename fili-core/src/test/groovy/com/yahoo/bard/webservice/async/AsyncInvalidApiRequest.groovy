// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.async

import com.yahoo.bard.webservice.util.GroovyTestUtils

/**
 * Verifies that when the user has a syntax error (or some other error that triggers an InvalidApiRequestException) that
 * the error is returned immediately.
 */
class AsyncInvalidApiRequest extends AsyncFunctionalSpec {
    /*
        This executes the following requests, and expects the following responses. All caps are placeholders.
             Send:
                 http://localhost:9998/data/shapes/day?dateTime=2016-08-30%2F2016-08-31&asyncAfter=always
             Receive:
                {
                    "status": 400,
                    "statusName": "Bad Request",
                    "reason": "com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException",
                    "description": "Required parameter metrics is missing or empty. Use 'metrics=METRICNAME1,METRICNAME2' in the query string.",
                    "druidQuery": null
                }
    */

    String EXPECTED_ERROR_MESSAGE =
            """{
                    "status": 400,
                    "statusName": "Bad Request",
                    "reason": "com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException",
                    "description": "Required parameter metrics is missing or empty. Use 'metrics=METRICNAME1,METRICNAME2' in the query string.",
                    "druidQuery": null
            }"""

    @Override
    LinkedHashMap<String, Closure<String>> getResultsToTargetFunctions() {
        [ data: { "data/shapes/day" } ]
    }

    @Override
    LinkedHashMap<String, Closure<Void>> getResultAssertions() {
        [ data: { assert GroovyTestUtils.compareErrorPayload(it.readEntity(String), EXPECTED_ERROR_MESSAGE) } ]

    }

    @Override
    LinkedHashMap<String, Closure<Map<String, List<String>>>> getQueryParameters() {
        [
                data: { [
                        asyncAfter: ["always"],
                        dateTime: ["2016-08-30/2016-08-31"]
                ] }
        ]
    }

    @Override
    Closure<String> getFakeDruidResponse() {
        return {"I will never leave this curly prison."}
    }
}
