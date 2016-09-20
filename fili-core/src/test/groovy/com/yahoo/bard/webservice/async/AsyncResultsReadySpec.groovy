package com.yahoo.bard.webservice.async

import com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobStatus
import com.yahoo.bard.webservice.async.workflows.TestAsynchronousWorkflowsBuilder
import com.yahoo.bard.webservice.util.GroovyTestUtils

import rx.Observer

import java.util.concurrent.CountDownLatch

/**
 * Verifies that when the results of an asynchronous request are ready, that the job status is updated to "success" and
 * the results are accessible through both the {@code results} and {@code syncResults} links.
 */
class AsyncResultsReadySpec extends AsyncFunctionalSpec {
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
                     "rows" : [
                          {
                            "dateTime" : "2016-08-30 00:00:00.000",
                            "height" : 100
                          }
                     ]
                }
            Send:
                 http://localhost:9998/jobs/gregUUID/results
            Receive:
                {
                     "rows" : [
                          {
                            "dateTime" : "2016-08-30 00:00:00.000",
                            "height" : 100
                          }
                     ]
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
                     "status": "success"
                }
    */

    static final String QUERY =
            "http://localhost:9998/data/shapes/day?dateTime=2016-08-30%2F2016-08-31&metrics=height&asyncAfter=always"

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
                //BroadcastChannel. Thus, we wait until the results are ready before verifying that the job status
                //has been updated correctly.
                syncResults: { AsyncTestUtils.extractTargetFromField(it.data.readEntity(String), "syncResults") },
                results: { AsyncTestUtils.extractTargetFromField(it.data.readEntity(String), "results") },
                jobs: {
                    // We won't even return the target for the request, until the job metadata has been updated,
                    // guaranteeing that the result will be marked ready before we make the request.
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
                    AsyncTestUtils.validateJobPayload(it.readEntity(String), QUERY, DefaultJobStatus.PENDING.name)
                },
                syncResults: {
                    assert it.status == 200
                    assert GroovyTestUtils.compareJson(it.readEntity(String), getExpectedApiResponse())
                },
                results: {
                    assert it.status == 200
                    assert GroovyTestUtils.compareJson(it.readEntity(String), getExpectedApiResponse())
                },
                jobs: { response ->
                        assert response.status == 200
                        AsyncTestUtils.validateJobPayload(
                                response.readEntity(String),
                                QUERY,
                                DefaultJobStatus.SUCCESS.name
                        )
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
        return {
            """[
                    {
                        "version" : "v1",
                        "timestamp": "2016-08-30",
                        "result" : {
                            "height" : 100
                        }
                    }
            ]"""
        }
    }

    String getExpectedApiResponse() {
        """{
              "rows" : [
                  {
                    "dateTime" : "2016-08-30 00:00:00.000",
                    "height" : 100
                  }
              ]
        }"""
    }
}
