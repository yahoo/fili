// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.data.HttpResponseMaker
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.async.jobs.stores.ApiJobStore
import com.yahoo.bard.webservice.async.broadcastchannels.BroadcastChannel
import com.yahoo.bard.webservice.async.preresponses.stores.HashPreResponseStore
import com.yahoo.bard.webservice.async.jobs.payloads.JobPayloadBuilder
import com.yahoo.bard.webservice.async.preresponses.stores.PreResponseStore
import com.yahoo.bard.webservice.async.preresponses.stores.PreResponseTestingUtils
import com.yahoo.bard.webservice.async.broadcastchannels.SimpleBroadcastChannel
import com.yahoo.bard.webservice.web.JobsApiRequest
import com.yahoo.bard.webservice.web.PreResponse
import com.yahoo.bard.webservice.web.RequestMapper

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ObjectWriter

import rx.Observable
import rx.observers.TestSubscriber
import rx.subjects.PublishSubject
import spock.lang.Specification
import spock.lang.Timeout

import java.util.concurrent.CountDownLatch

import javax.ws.rs.core.UriInfo

/**
 * Test for the reactive chain for getting a PreResponse
 */
@Timeout(30)
class JobsServletReactiveChainforResultsEndpointSpec extends Specification {
    JobsServlet jobsServlet
    BroadcastChannel<String> broadcastChannel
    HashPreResponseStore preResponseStore
    JobPayloadBuilder jobPayloadBuilder
    ApiJobStore apiJobStore
    UriInfo uriInfo
    ObjectMappersSuite objectMappersSuite
    DimensionDictionary dimensionDictionary
    RequestMapper requestMapper
    PreResponseStore mockPreResponseStore
    JobsServlet mockJobServlet
    HttpResponseMaker httpResponseMaker

    def setup() {
        objectMappersSuite = Mock(ObjectMappersSuite)
        ObjectMapper objectMapper = Mock(ObjectMapper)
        ObjectWriter objectWriter = Mock(ObjectWriter)
        objectMappersSuite.getMapper() >> objectMapper
        objectMapper.writer() >> objectWriter

        apiJobStore = Mock(ApiJobStore)
        jobPayloadBuilder = Mock(JobPayloadBuilder)
        uriInfo = Mock(UriInfo)
        dimensionDictionary = Mock(DimensionDictionary)
        requestMapper = Mock(RequestMapper)

        preResponseStore = new HashPreResponseStore()
        broadcastChannel = new SimpleBroadcastChannel<>(PublishSubject.create())
        httpResponseMaker = new HttpResponseMaker(objectMappersSuite, dimensionDictionary)

        jobsServlet = new JobsServlet(
                objectMappersSuite,
                apiJobStore,
                jobPayloadBuilder,
                preResponseStore,
                broadcastChannel,
                requestMapper,
                httpResponseMaker
        )

        //Mocked objects for interaction testing
        mockPreResponseStore = Mock(PreResponseStore)

        mockJobServlet = new JobsServlet(
                objectMappersSuite,
                apiJobStore,
                jobPayloadBuilder,
                mockPreResponseStore,
                broadcastChannel,
                requestMapper,
                httpResponseMaker
        )
    }

    def "getResults emits a PreResponse if it is available in the PreResponseStore even if the notification for the ticket is missed"() {
        setup:
        TestSubscriber<PreResponse> testSubscriber = new TestSubscriber<>()
        JobsApiRequest apiRequest = new JobsApiRequest(
                null,
                null,
                "",
                "",
                null,
                uriInfo,
                jobPayloadBuilder,
                apiJobStore
        )

        when: "PreResponse is stored in the PreResponsestore and then a notification is fired by BroadcastChannel"
        preResponseStore.save("ticket0", PreResponseTestingUtils.buildPreResponse("2016-04-20")).toBlocking().first()
        broadcastChannel.publish("ticket0")

        and: "We then subscribe to the reactive chain after the notification has already been fired"
        jobsServlet.getResults("ticket0", apiRequest.asyncAfter).subscribe(testSubscriber)

        then: "getResults returns the expected PreResponse"
        testSubscriber.assertReceivedOnNext([PreResponseTestingUtils.buildPreResponse("2016-04-20")])
    }

    def "getResults returns a preResponseObservable if it is available in the PreResponseStore even if the notification for the ticket is not received before the async timeout"() {
        setup:
        TestSubscriber<PreResponse> testSubscriber = new TestSubscriber<>()
        JobsApiRequest apiRequest = new JobsApiRequest(
                null,
                null,
                "",
                "",
                null,
                uriInfo,
                jobPayloadBuilder,
                apiJobStore
        )

        when: "PreResponse is stored in the PreResponsestore"
        preResponseStore.save("ticket1", PreResponseTestingUtils.buildPreResponse("2016-04-21")).subscribe()

        and: "We then subscribe to the reactive chain"
        jobsServlet.getResults("ticket1", apiRequest.asyncAfter).subscribe(testSubscriber)

        then: "getResults returns the expected PreResponse"
        testSubscriber.assertReceivedOnNext([PreResponseTestingUtils.buildPreResponse("2016-04-21")])
    }

    def "getResults returns a PreResponseObservable even if the PreResponse is not available in the PreResponseStore initially but a notification is received from the broadcastChannel before the async timeout"() {
        setup:
        TestSubscriber<PreResponse> testSubscriber = new TestSubscriber<>()
        JobsApiRequest apiRequest = new JobsApiRequest(
                null,
                null,
                "",
                "",
                null,
                uriInfo,
                jobPayloadBuilder,
                apiJobStore
        )

        when: "we start the async chain"
        jobsServlet.getResults("ticket2", apiRequest.asyncAfter).subscribe(testSubscriber)

        then: "The PreResponse is not available yet in the PreResponseStore"
        testSubscriber.assertNoValues()

        when: "PreResponse is stored in the PreResponseStore"
        preResponseStore.save("ticket2", PreResponseTestingUtils.buildPreResponse("2016-04-22")).subscribe()

        and: "We receive the notification from broadcastChannel"
        broadcastChannel.publish("ticket2")

        then: "getResults returns the expected PreResponse"
        testSubscriber.assertReceivedOnNext([PreResponseTestingUtils.buildPreResponse("2016-04-22")])
    }

    def "getResults returns an empty observable if the PreResponse is not available in the PreResponseStore before the async timeout"() {
        setup:
        TestSubscriber<PreResponse> testSubscriber = new TestSubscriber<>()
        JobsApiRequest apiRequest = new JobsApiRequest(null, "5", "", "", null, uriInfo, jobPayloadBuilder, apiJobStore)

        when: "we start the async chain"
        jobsServlet.getResults("ticket3", apiRequest.asyncAfter).subscribe(testSubscriber)
        Thread.sleep(7)

        then: "getResults returns an empty observable"
        testSubscriber.awaitTerminalEvent()
        testSubscriber.assertNoErrors()
        testSubscriber.assertCompleted()
        testSubscriber.assertNoValues()
    }

    def "If the PreResponse is available in the PreResponseStore and we miss the notification from broadcastChannel, we go to the PreResponseStore exactly once"() {
        setup:
        JobsApiRequest apiRequest1 = new JobsApiRequest(
                null,
                "5",
                "",
                "",
                null,
                uriInfo,
                jobPayloadBuilder,
                apiJobStore
        )

        and: "We miss the notification that the preResponse is stored in the PreResponseStore"
        broadcastChannel.publish("ticket4")

        when: "We start the async chain"
        mockJobServlet.getResults("ticket4", apiRequest1.asyncAfter).subscribe()

        then: "Then we go to the PreResponseStore exactly once to get the ticket"
        1 * mockPreResponseStore.get(_) >> Observable.empty()
    }

    def "When multiple results-are-ready notifications are sent, we only receive the results we care about"() {
        given: "A proper PreResponseStore that allows us to know when values appear in the store"
        CountDownLatch getTicket1Latch = new CountDownLatch(1)
        preResponseStore.addGetLatch("ticket1", getTicket1Latch)
        CountDownLatch saveTicket1Latch = new CountDownLatch(1)
        preResponseStore.addSaveLatch("ticket1", saveTicket1Latch)

        and:
        JobsApiRequest apiRequest = new JobsApiRequest(
                null,
                "never",
                "",
                "",
                null,
                uriInfo,
                jobPayloadBuilder,
                apiJobStore
        )

        and:
        TestSubscriber resultsSubscriber = new TestSubscriber()

        and:
        jobsServlet.getResults("ticket1", apiRequest.asyncAfter).subscribe(resultsSubscriber)

        and: "We wait for the first attempt to get results from the store to come up empty before we add fake results"
        getTicket1Latch.await()
        PreResponse results = Mock(PreResponse)
        preResponseStore.save("ticket1", results)

        and: "We wait for the results to be successfully stored before sending a ready notification"
        saveTicket1Latch.await()

        when: "We send a bunch of notifications through the broadcast channel, including multiples of ticket1"
        ["ticket2", "ticket1", "ticket3", "ticket1", "ticket4", "ticket1"].each { broadcastChannel.publish(it) }

        then: "The subscriber only gets the results for ticket1"
        resultsSubscriber.assertValue(results)

        cleanup:
        preResponseStore.clearGetLatches()
        preResponseStore.clearSaveLatches()
    }

    def "If the notification from broadcastChannel is not received within async timeout, we go to the PreResponseStore exactly once"() {
        setup:
        JobsApiRequest apiRequest1 = new JobsApiRequest(
                null,
                "2",
                "",
                "",
                null,
                uriInfo,
                jobPayloadBuilder,
                apiJobStore
        )

        when: "We start the async chain"
        mockJobServlet.getResults("ticket4", apiRequest1.asyncAfter)
        //The delay is to ensure that we get the notification after async timeout
        Thread.sleep(1000)

        and: "We receive the notification after async timeout"
        broadcastChannel.publish("ticket4")

        then: "then we go to the PreResponseStore exactly once for the initial check to see if the data is already there"
        1 * mockPreResponseStore.get(_) >> Observable.empty()
    }

    def "If the notification from broadcastChannel is received within the async timeout, we go to the PreResponsestore twice"() {
        setup:
        JobsApiRequest apiRequest1 = new JobsApiRequest(
                null,
                "never",
                "",
                "",
                null,
                uriInfo,
                jobPayloadBuilder,
                apiJobStore
        )

        when: "We start the async chain"
        mockJobServlet.getResults("ticket4", apiRequest1.asyncAfter).subscribe()

        and: "We receive the notification before the async timeout"
        broadcastChannel.publish("ticket4")

        then: "then we go to the PreResponseStore twice to get the ticket"
        2 * mockPreResponseStore.get(_) >> Observable.empty()
    }
}
