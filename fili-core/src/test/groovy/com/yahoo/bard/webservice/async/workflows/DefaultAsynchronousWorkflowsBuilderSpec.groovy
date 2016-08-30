// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.workflows

import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.DATE_UPDATED
import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.JOB_TICKET
import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.STATUS

import com.yahoo.bard.webservice.async.ResponseException
import com.yahoo.bard.webservice.async.jobs.stores.ApiJobStore
import com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobStatus
import com.yahoo.bard.webservice.async.jobs.jobrows.JobRow
import com.yahoo.bard.webservice.async.preresponses.stores.PreResponseStore
import com.yahoo.bard.webservice.data.ResultSet
import com.yahoo.bard.webservice.druid.model.query.AllGranularity
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.bard.webservice.table.Schema
import com.yahoo.bard.webservice.util.Either
import com.yahoo.bard.webservice.web.PreResponse
import com.yahoo.bard.webservice.web.responseprocessors.ResponseContext
import com.yahoo.bard.webservice.web.responseprocessors.ResponseContextKeys

import org.joda.time.DateTime

import rx.Observable
import rx.observers.TestSubscriber
import rx.subjects.PublishSubject
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Subject
import spock.lang.Timeout
import spock.lang.Unroll
import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import java.util.function.Function

@Timeout(30)
class DefaultAsynchronousWorkflowsBuilderSpec extends Specification {

    ApiJobStore apiJobStore = Mock(ApiJobStore)
    PreResponseStore preResponseStore = Mock(PreResponseStore)

    Instant now = Instant.now()
    String timestamp = new DateTime(now.toEpochMilli()).toDateTimeISO().toString()


    JobRow jobMetadata = new JobRow(JOB_TICKET, [(JOB_TICKET): "greg0", (STATUS): "pending"])
    PreResponse preResponse = Stub(PreResponse)
    PublishSubject<Either<PreResponse, JobRow>> payloadEmitter = PublishSubject.create()
    PublishSubject<PreResponse> preResponseEmitter = PublishSubject.create()

    Function<JobRow, String> jobRowSerializer = {it.toString()}

    TestSubscriber<String> metadataChannel = new TestSubscriber<>()
    TestSubscriber<PreResponse> resultsChannel = new TestSubscriber<>()
    TestSubscriber<String> preResponseStoredSubscriber = new TestSubscriber<>()
    TestSubscriber<JobRow> jobRowUpdatedSubscriber = new TestSubscriber<>()

    @Shared ResponseException exception = new ResponseException(
            404,
            "not found",
            "not found",
            Mock(DruidAggregationQuery)
    )

    @Subject DefaultAsynchronousWorkflowsBuilder asynchronousProcessBuilder = new DefaultAsynchronousWorkflowsBuilder(
            apiJobStore,
            preResponseStore,
            Clock.fixed(now, ZoneId.systemDefault())
    )

    @Subject AsynchronousWorkflows asynchronousWorkflows = asynchronousProcessBuilder.buildAsynchronousWorkflows(
            preResponseEmitter,
            payloadEmitter,
            jobMetadata,
            jobRowSerializer
    )


    def setup() {
        asynchronousWorkflows.synchronousPayload.subscribe(resultsChannel)
        asynchronousWorkflows.asynchronousPayload.subscribe(metadataChannel)
        asynchronousWorkflows.preResponseReadyNotifications.subscribe(preResponseStoredSubscriber)
        asynchronousWorkflows.jobMarkedCompleteNotifications.subscribe(jobRowUpdatedSubscriber)
    }

    def "When the payload emitter emits a PreResponse, the results are sent to the user"() {
        when: "We send a PreResponse"
        payloadEmitter.onNext(Either.left(preResponse))

        then: "The payload is sent to the user along the results channel"
        resultsChannel.assertValue(preResponse)

        and: "The metadata and error channels send nothing"
        metadataChannel.assertNoValues()
        metadataChannel.assertNoErrors()

        and: "The stores are not touched"
        0 * apiJobStore.save(_ as JobRow)
        0 * preResponseStore.save(_ as String, _ as PreResponse)

        and: "The notification channels send nothing"
        preResponseStoredSubscriber.assertNoErrors()
        preResponseStoredSubscriber.assertNoValues()
        jobRowUpdatedSubscriber.assertNoErrors()
        jobRowUpdatedSubscriber.assertNoValues()
    }

    @Unroll
    def "When the asynchronous trigger is sent, but #beforeAfter the PreResponse is sent, the query is asynchronous"() {
        given: "The JobRow with a status of success"
        JobRow updatedJobMetadata = new JobRow(
                JOB_TICKET,
                [(JOB_TICKET): "greg0", (STATUS): "success", (DATE_UPDATED): timestamp]
        )

        when: "We send the job row, and the PreResponse in either order"
        if (beforeAfter == "before") {
            payloadEmitter.onNext(Either.right(jobMetadata))
            preResponseEmitter.onNext(preResponse)
        } else {
            preResponseEmitter.onNext(preResponse)
            payloadEmitter.onNext(Either.right(jobMetadata))
        }

        then: "The metadata is sent to the user"
        metadataChannel.assertValue(jobMetadata.toString())

        and: "Neither the error channel nor the results channel send anything"
        resultsChannel.assertNoValues()
        metadataChannel.assertNoErrors()

        and: "The metadata is stored in the ApiJobStore"
        1 * apiJobStore.save(jobMetadata) >> Observable.just(jobMetadata)

        and: "The preResponse is stored in the PreResponsStore"
        1 * preResponseStore.save("greg0", preResponse) >> Observable.just("greg0")

        and: "The metadata is updated in the ApiJobStore"
        1 * apiJobStore.save(updatedJobMetadata) >> Observable.just(updatedJobMetadata)

        and: "The two notification channels receive the appropriate messages"
        preResponseStoredSubscriber.assertValue("greg0")
        jobRowUpdatedSubscriber.assertValue(updatedJobMetadata)

        where:
        beforeAfter << ["before", "after"]
    }

    @Unroll
    def "When an asynchronous query generates #error, then #expectedErrorMap is stored in the store"() {
        given: "The response context containing the expected error information"
        ResponseContext expectedResponseContext = new ResponseContext([:])
        expectedResponseContext.putAll(expectedErrorMap)

        and: "The expected error JobRow"
        JobRow expectedUpdatedJobRow = new JobRow(
                JOB_TICKET,
                [(JOB_TICKET): "greg0", (STATUS): "failure", (DATE_UPDATED): timestamp]
        )

        and: "An ApiJobStore that just saves and transmits the metadata"
        apiJobStore.save(jobMetadata) >> Observable.just(jobMetadata)

        when: "An error is generated instead of a PreResponse"
        payloadEmitter.onNext(Either.right(jobMetadata))
        preResponseEmitter.onError(error)

        then: "The error message is stored in the PreResponseStore"
        1 * preResponseStore.save(
                "greg0",
                new PreResponse(new ResultSet([], new Schema(AllGranularity.INSTANCE)), expectedResponseContext)
        ) >> Observable.just("greg0")

        and: "The JobRow's status is updated with with error"
        1 * apiJobStore.save(expectedUpdatedJobRow) >> Observable.just(expectedUpdatedJobRow)

        and: "The notification channels receive the appropriate notifications"
        preResponseStoredSubscriber.assertValue("greg0")
        jobRowUpdatedSubscriber.assertValue(expectedUpdatedJobRow)

        where:
        error                   || expectedErrorMap
        exception               || [(ResponseContextKeys.ERROR_MESSAGE.getName()): "not found", (ResponseContextKeys.STATUS.getName()): 404]
        new Exception("Error!") || [(ResponseContextKeys.ERROR_MESSAGE.getName()): "Error!", (ResponseContextKeys.STATUS.getName()): 500]
    }

    def "When a synchronous query generates #error, then no asynchronous processing happens"() {
        when: "The payloadEmitter and preResponseEmitter generate errors"
        preResponseEmitter.onError(error)
        payloadEmitter.onError(error)

        then: "Neither of the asynchronous stores are touched"
        0 * apiJobStore.save(_)
        0 * preResponseStore.save(_)

        and: "Neither of the notification channels receive notifications"
        preResponseStoredSubscriber.assertNoValues()
        jobRowUpdatedSubscriber.assertNoValues()

        where:
        error << [exception, new Exception("Error!")]
    }

    def "When the initial attempt to save the job metadata fails, the PreResponse is saved and the updated row sent"() {
        given: "The expected success ApiJobRow"
        JobRow expectedUpdatedJobRow = new JobRow(
                JOB_TICKET,
                [(JOB_TICKET): "greg0", (DATE_UPDATED): timestamp, (STATUS): DefaultJobStatus.SUCCESS.getName()]
        )

        and: "Attempting to save the original metadata generates an error message"
        apiJobStore.save(jobMetadata) >> Observable.error(new Exception())

        when: "We send a JobRow, and a PreResponse"
        payloadEmitter.onNext(Either.right(jobMetadata))
        preResponseEmitter.onNext(preResponse)

        then: "The PreResponse is still saved in the PreResponseStore"
        1 * preResponseStore.save("greg0", preResponse) >> Observable.just("greg0")

        and: "The updated job row is still saved to the ApiJobStore"
        1 * apiJobStore.save(expectedUpdatedJobRow) >> Observable.just(expectedUpdatedJobRow)

        and: "The notification channels receive the appropriate notifications"
        preResponseStoredSubscriber.assertValue("greg0")
        jobRowUpdatedSubscriber.assertValue(expectedUpdatedJobRow)
    }
}
