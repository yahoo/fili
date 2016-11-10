package com.yahoo.bard.webservice.async

import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobStatus.FAILURE
import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobStatus.PENDING

import com.yahoo.bard.webservice.async.workflows.TestAsynchronousWorkflowsBuilder
import com.yahoo.bard.webservice.util.GroovyTestUtils

import rx.Observer

import java.util.concurrent.CountDownLatch

/**
 * Verifies that the job status is updated to error, and results links return the error message when Druid experiences
 * an error.
 */
class AsyncDruidSendsErrorSpec extends AsyncFunctionalSpec {
    /*
        This executes the following requests, and expects the following responses. All caps are placeholders.
             Send:
                 http://localhost:9998/data/shapes/day?dateTime=2016-08-30%2F2016-08-31&metrics=height&asyncAfter=0
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
                 http://localhost:9998/jobs/gregUUID/results?asyncAfter=never
             Receive:
                {
                    "status" : 500,
                    "statusName" : "Internal Server Error",
                    "reason" : "All the things have broken.",
                    "description" : "All the things have broken.",
                    "druidQuery" : null
                }
            Send:
                 http://localhost:9998/jobs/gregUUID/results
            Receive:
                {
                    "status" : 500,
                    "statusName" : "Internal Server Error",
                    "reason" : "All the things have broken.",
                    "description" : "All the things have broken.",
                    "druidQuery" : null
                }

            Send:
                 http://localhost:9998/jobs/gregUUID
            Receive:
                {
                     "self": "http://localhost:9998/jobs/gregUUID",
                     "userId": "greg",
                     "ticket": "gregUUID",
                     "results": "http://localhost:9998/jobs/gregUUID/results",
                     "syncResults": "http://localhost:9998/jobs/gregUUID/results?asyncAfter=never",
                     "dateCreated": "DATETIME",
                     "dateUpdated": "SAME_DATETIME_AS_ABOVE",
                     "status": "failure"
                }
    */

    static final String QUERY =
            "http://localhost:9998/data/shapes/day?dateTime=2016-08-30%2F2016-08-31&metrics=height&asyncAfter=always"

    static final String ERROR_MESSAGE = """{
                        "status" : 500,
                        "statusName" : "Internal Server Error",
                        "reason" : "All the things have broken.",
                        "description" : "All the things have broken.",
                        "druidQuery" : null,
                        "requestId": "SOME UUID"
                    }"""

    final CountDownLatch jobMetadataReady = new CountDownLatch(1)

    def setup() {
        TestAsynchronousWorkflowsBuilder.addSubscriber(
                TestAsynchronousWorkflowsBuilder.Workflow.JOB_MARKED_COMPLETE,
                {jobMetadataReady.countDown()},
                {throw it}
        )
    }

    def cleanup() {
        TestAsynchronousWorkflowsBuilder.clearSubscribers()
    }

    @Override
    Map<String, Closure<String>> getResultsToTargetFunctions() {
        [
                data: { "data/shapes/day" },
                //By querying the syncResults link first, we wait until the results are ready, thanks to the
                //BroadcastChannel. So we have no race condition between the data request updating the job ticket, and
                //the query for the job metadata
                syncResults: { AsyncTestUtils.extractTargetFromField(it.data.readEntity(String), "syncResults") },
                results: { AsyncTestUtils.extractTargetFromField(it.data.readEntity(String), "results") },
                jobs: {
                    jobMetadataReady.await()
                    AsyncTestUtils.buildTicketLookup(it.data.readEntity(String))
                }
        ]
    }

    @Override
    Map<String, Closure<Void>> getResultAssertions() {
        [
                data: {
                    assert it.status == 202
                    AsyncTestUtils.validateJobPayload(it.readEntity(String), QUERY, PENDING.name)
                },
                syncResults: {
                    // However, there was a problem in the backend, and the job failed. So when we go to get the
                    // results, we instead get the error status (500) of the error, along with an error message
                    // describing the problem.
                    assert it.status == 500
                    assert GroovyTestUtils.compareErrorPayload(it.readEntity(String), ERROR_MESSAGE)
                },
                results: {
                    // This returns the same results as syncResults, since the two only differ in how long they
                    // wait for a response.
                    assert it.status == 500
                    assert GroovyTestUtils.compareErrorPayload(it.readEntity(String), ERROR_MESSAGE)
                },
                jobs: { response ->
                    assert response.status == 200
                    AsyncTestUtils.validateJobPayload(response.readEntity(String), QUERY, FAILURE.name)
                }
        ]
    }

    @Override
    Map<String, Closure<Map<String, List<String>>>> getQueryParameters() {
        [
                data: {[
                        metrics: ["height"],
                        asyncAfter: ["always"],
                        dateTime: ["2016-08-30/2016-08-31"]
                ]},
                syncResults: {
                    AsyncTestUtils.extractQueryParameters(
                            new URI(AsyncTestUtils.getJobFieldValue(it.data.readEntity(String), "syncResults"))
                    )
                },
                results: {[:]},
                jobs: {[:]},
        ]
    }

    @Override
    Closure<String> getFakeDruidResponse() {
        return {"All the things have broken."}
    }

    @Override
    int getDruidStatusCode() {
        return 500
    }
}
