// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.async

import com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField
import com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobStatus

import spock.lang.Timeout

import java.util.concurrent.CountDownLatch

import javax.ws.rs.core.Response

/**
 * Verifies that if the result of an asynchronous query is not ready yet, then the user gets back the exact same payload
 * from the jobs endpoint they received from the data endpoint.
 */
@Timeout(30)
class AsyncResultsNotReadySpec extends AsyncFunctionalSpec {
    /*
        This executes the following requests, and expects the following responses. All caps are placeholders.
             Send:
                 http://localhost:9998/data/shapes/day?dateTime=2016-08-30%2F2016-08-31&metrics=height&asyncAfter=always
             Receive:
                 {
                     "self": "http://localhost:9998/jobs/gregUUID",
                     "userId": "greg",
                     "ticket": "gregUUID",
                     "results": "http://localhost:9998/jobs/gregUUID/results",
                     "syncResults": "http://localhost:9998/jobs/gregUUID/results?asyncAfter=never",
                     "dateCreated": "DATETIME",
                     "dateUpdated": "SAME_DATETIME_AS_ABOVE",
                     "status": "pending"
                 }
            Send:
                 http://localhost:9998/jobs/gregUUID/results?asyncAfter=always
            Receive:
                SAME AS ABOVE
            Send:
                 http://localhost:9998/jobs/gregUUID
            Receive:
                SAME AS ABOVE
    */

    final CountDownLatch validationFinished = new CountDownLatch(3)

    String getQuery() {
        return "http://localhost:${jtb.getHarness().getPort()}/data/shapes/day/color?dateTime=2016-08-30%2F2016-08-31&metrics=height&asyncAfter=always"
    }

    @Override
    LinkedHashMap<String, Closure<String>> getResultsToTargetFunctions() {
        [
                data: {"data/shapes/day/color"},
                jobs: {AsyncTestUtils.buildTicketLookup(it.data.readEntity(String))},
                results: {payloads ->
                    String dataPayload = payloads.data.readEntity(String)
                    "jobs/${AsyncTestUtils.getJobFieldValue(dataPayload, DefaultJobField.JOB_TICKET.name)}/results"
                }
        ]
    }

    @Override
    LinkedHashMap<String, Closure<Void>> getResultAssertions() {
        [
                data: {Response response ->
                    try {
                        assert response.status == 202 : response.toString()
                        AsyncTestUtils.validateJobPayload(
                                jtb,
                                response.readEntity(String),
                                getQuery(),
                                DefaultJobStatus.PENDING.name
                        )
                    } finally {
                        validationFinished.countDown()
                    }
                    sleep(10)
                },
                jobs: {Response response ->
                    try {
                        assert response.status == 200 : response.toString()
                        //The jobs endpoint returns job metadata containing the same expected value as the data endpoint
                        AsyncTestUtils.validateJobPayload(
                                jtb,
                                response.readEntity(String),
                                getQuery(),
                                DefaultJobStatus.PENDING.name
                        )
                    } finally {
                        validationFinished.countDown()
                    }
                },
                results: {Response response ->
                    try {
                        assert response.status == 200 : response.toString()
                        //The results endpoint returns job metadata containing the same expected value as the data
                        // endpoint
                        AsyncTestUtils.validateJobPayload(
                                jtb,
                                response.readEntity(String),
                                getQuery(),
                                DefaultJobStatus.PENDING.name
                        )
                    } finally {
                        validationFinished.countDown()
                    }
                }
        ]
    }

    @Override
    LinkedHashMap<String, Closure<Map<String, List<String>>>> getQueryParameters() {
        [
                data: {[
                        metrics: ["height"],
                        asyncAfter: ["always"],
                        dateTime: ["2016-08-30/2016-08-31"]
                ]},
                jobs: {[:]},
                results: { [ asyncAfter: ["always"] ] }
        ]
    }

    @Override
    Closure<String> getFakeDruidResponse() {
        return {
            //We don't want the backend to return until all the validation has been performed, to simulate the backend
            //taking a while to respond.
            validationFinished.await()
            return "[]"
        }
    }
}
