// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import com.yahoo.bard.webservice.jobs.ApiJobStore
import com.yahoo.bard.webservice.jobs.BroadcastChannel
import com.yahoo.bard.webservice.jobs.DefaultJobField
import com.yahoo.bard.webservice.jobs.DefaultJobPayloadBuilder
import com.yahoo.bard.webservice.jobs.HashJobStore
import com.yahoo.bard.webservice.jobs.HashPreResponseStore
import com.yahoo.bard.webservice.jobs.JobField
import com.yahoo.bard.webservice.jobs.JobRow
import com.yahoo.bard.webservice.jobs.JobPayloadBuilder
import com.yahoo.bard.webservice.jobs.PreResponseStore
import com.yahoo.bard.webservice.jobs.PreResponseTestingUtils
import com.yahoo.bard.webservice.jobs.SimpleBroadcastChannel
import com.yahoo.bard.webservice.util.ReactiveTestUtils

import rx.Observable
import rx.observers.TestSubscriber
import rx.subjects.PublishSubject;
import spock.lang.Specification

import javax.ws.rs.core.UriBuilder
import javax.ws.rs.core.UriInfo

/**
 * Tests for JobsApiRequest.
 */
class JobsApiRequestSpec extends Specification {
    BroadcastChannel<String> broadcastChannel
    UriInfo uriInfo
    JobPayloadBuilder jobPayloadBuilder
    ApiJobStore apiJobStore
    PreResponseStore preResponseStore
    PreResponse ticket1PreResponse

    def setup() {
        uriInfo = Mock(UriInfo)

        uriInfo.getBaseUriBuilder() >> {
            UriBuilder.fromPath("https://localhost:9998/v1/")
        }

        apiJobStore = new HashJobStore()
        Map<JobField, String> fieldValueMap = [
                (DefaultJobField.JOB_TICKET):"ticket1",
                (DefaultJobField.DATE_CREATED): "2016-01-01",
                (DefaultJobField.DATE_UPDATED): "2016-01-01",
                (DefaultJobField.QUERY): "https://localhost:9998/v1/data/QUERY",
                (DefaultJobField.STATUS): "success",
                (DefaultJobField.USER_ID): "momo"
        ]
        JobRow jobRow = new JobRow(DefaultJobField.JOB_TICKET, fieldValueMap)
        apiJobStore.save(jobRow)

        Map<JobField, String> badFieldValueMap  = new HashMap<>();
        badFieldValueMap.put(DefaultJobField.JOB_TICKET, "badTicket");
        JobRow badJobRow = new JobRow(DefaultJobField.JOB_TICKET, badFieldValueMap);
        apiJobStore.save(badJobRow);

        jobPayloadBuilder = new DefaultJobPayloadBuilder()

        preResponseStore = new HashPreResponseStore();
        (1..2).each {
            preResponseStore.save("ticket$it", PreResponseTestingUtils.buildPreResponse("2016-04-2$it")).toBlocking().first()
        }

        ticket1PreResponse = PreResponseTestingUtils.buildPreResponse("2016-04-21")

        broadcastChannel = new SimpleBroadcastChannel<>(PublishSubject.create())
    }

    def "getJobViewObservable returns an Observable wrapping the job to be returned to the user"() {
        setup:
        TestSubscriber<Map<String, String>> getSubscriber = new TestSubscriber<>()
        Map<String, String> job = [
                query: "https://localhost:9998/v1/data/QUERY",
                results: "https://localhost:9998/v1/jobs/ticket1/results",
                syncResults: "https://localhost:9998/v1/jobs/ticket1/results?asyncAfter=never",
                self: "https://localhost:9998/v1/jobs/ticket1",
                status: "success",
                jobTicket: "ticket1",
                dateCreated: "2016-01-01"
        ]

        JobsApiRequest apiRequest = new JobsApiRequest(
                null,
                null,
                "",
                "",
                uriInfo,
                jobPayloadBuilder,
                apiJobStore,
                preResponseStore,
                broadcastChannel
        )

        when:
        apiRequest.getJobViewObservable("ticket1").subscribe(getSubscriber)

        then:
        getSubscriber.assertReceivedOnNext([job])
    }

    def "getJobViewObservable results in onError being called on its observers if the job ticket does not exist in the ApiJobStore"() {
        setup:
        TestSubscriber<Map<String, String>> errorSubscriber = new TestSubscriber<>()
        JobsApiRequest apiRequest = new JobsApiRequest(
                null,
                null,
                "",
                "",
                uriInfo,
                jobPayloadBuilder,
                apiJobStore,
                preResponseStore,
                broadcastChannel
        )

        when:
        apiRequest.getJobViewObservable("IDontExist").subscribe(errorSubscriber)

        then:
        errorSubscriber.assertError(JobNotFoundException.class)
        errorSubscriber.getOnErrorEvents().get(0).getMessage() == "No job found with job ticket IDontExist"
    }

    def "handleBroadcastChannelNotification returns an Observable that replays notification if it was sent before async timeout"() {
        setup:
        TestSubscriber<PreResponse> testSubscriber = new TestSubscriber<>()
        JobsApiRequest apiRequest = new JobsApiRequest(
                null,
                null,
                "",
                "",
                uriInfo,
                jobPayloadBuilder,
                apiJobStore,
                preResponseStore,
                broadcastChannel
        )

        when:
        Observable<PreResponse> preResponseObservable = apiRequest.handleBroadcastChannelNotification("ticket1")

        broadcastChannel.publish("ticket1")
        preResponseObservable.subscribe(testSubscriber)

        then:
        testSubscriber.assertReceivedOnNext([ticket1PreResponse])
    }

    def "handleBroadcastChannelNotification returns an Observable that filters notifications and only returns a single notification for the given ticket"() {
        setup:
        TestSubscriber<PreResponse> testSubscriber = new TestSubscriber<>()
        JobsApiRequest apiRequest = new JobsApiRequest(
                null,
                null,
                "",
                "",
                uriInfo,
                jobPayloadBuilder,
                apiJobStore,
                preResponseStore,
                broadcastChannel
        )

        when:
        Observable<PreResponse> preResponseObservable = apiRequest.handleBroadcastChannelNotification("ticket1")
        broadcastChannel.publish("ticket_before_connecting_is_dropped")
        broadcastChannel.publish("ticket1")
        broadcastChannel.publish("ticket2")
        broadcastChannel.publish("ticket1")
        preResponseObservable.subscribe(testSubscriber)

        then:
        testSubscriber.assertReceivedOnNext([ticket1PreResponse])
    }

    def "handleBroadcastChannelNotification returns an empty Observable if a timeout occurs before the notification is received"() {
        setup:
        TestSubscriber<PreResponse> testSubscriber = new TestSubscriber<>()
        JobsApiRequest apiRequest = new JobsApiRequest(
                null,
                "0",
                "",
                "",
                uriInfo,
                jobPayloadBuilder,
                apiJobStore,
                preResponseStore,
                broadcastChannel
        )

        when: "We start listening to the broadcastChannel and timeout occurs"
        Observable<PreResponse> preResponseObservable = apiRequest.handleBroadcastChannelNotification("ticket1")

        and: "we subscribe to the preResponseObservable"
        preResponseObservable.subscribe(testSubscriber)

        then: "preResponseObservable is empty (the chain is complete, and no values were sent)"
        ReactiveTestUtils.assertCompletedWithoutError(testSubscriber)
        testSubscriber.assertNoValues()
    }

    def "getJobViewObservable results in onError being called on it's observers if the JobRow cannot be correctly mapped to a Job payload"() {
        setup:
        TestSubscriber<Map<String, String>> errorSubscriber = new TestSubscriber<>()
        JobsApiRequest apiRequest = new JobsApiRequest(
                null,
                null,
                "",
                "",
                uriInfo,
                jobPayloadBuilder,
                apiJobStore,
                preResponseStore,
                broadcastChannel
        )

        when:
        apiRequest.getJobViewObservable("badTicket").subscribe(errorSubscriber)

        then:
        errorSubscriber.assertError(JobRequestFailedException.class)
        errorSubscriber.getOnErrorEvents().get(0).getMessage() == "Job with ticket badTicket cannot be retrieved due to internal error"
    }

    def "getJobViews results in onError being called on it's observers if any of the JobRows in the ApiJobStore cannot be correctly mapped to a Job payload"() {
        setup:
        TestSubscriber<Map<String, String>> errorSubscriber = new TestSubscriber<>()
        JobsApiRequest apiRequest = new JobsApiRequest(
                null,
                null,
                "",
                "",
                uriInfo,
                jobPayloadBuilder,
                apiJobStore,
                preResponseStore,
                broadcastChannel
        )

        when:
        apiRequest.getJobViews().subscribe(errorSubscriber)

        then:
        errorSubscriber.assertError(JobRequestFailedException.class)
        errorSubscriber.getOnErrorEvents().get(0).getMessage() == "Jobs cannot be retrieved successfully due to internal error"
    }
}
