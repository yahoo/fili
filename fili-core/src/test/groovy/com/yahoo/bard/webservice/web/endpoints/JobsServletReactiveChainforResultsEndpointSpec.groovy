// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.application.ObjectMappersSuite
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

import javax.ws.rs.core.UriInfo

/**
 * Test for the reactive chain for getting a PreResponse
 */
class JobsServletReactiveChainforResultsEndpointSpec extends Specification {
    JobsServlet jobsServlet
    BroadcastChannel<String> broadcastChannel
    PreResponseStore preResponseStore
    JobPayloadBuilder jobPayloadBuilder
    ApiJobStore apiJobStore
    UriInfo uriInfo
    ObjectMappersSuite objectMappersSuite
    DimensionDictionary dimensionDictionary
    RequestMapper requestMapper
    PreResponseStore mockPreResponseStore
    JobsServlet mockJobServlet

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

        jobsServlet = new JobsServlet(
                objectMappersSuite,
                apiJobStore,
                jobPayloadBuilder,
                preResponseStore,
                broadcastChannel,
                dimensionDictionary,
                requestMapper
        )

        //Mocked objects for interaction testing
        mockPreResponseStore = Mock(PreResponseStore)
        mockPreResponseStore.save(_,_) >> Observable.just("blah")

        mockJobServlet = new JobsServlet(
                objectMappersSuite,
                apiJobStore,
                jobPayloadBuilder,
                mockPreResponseStore,
                broadcastChannel,
                dimensionDictionary,
                requestMapper
        )
    }

    def "getPreResponseObservable returns a preResponseObservable if it is available in the PreResponseStore even if the notification for the ticket is missed"() {
        setup:
        TestSubscriber<PreResponse> testSubscriber = new TestSubscriber<>()
        JobsApiRequest apiRequest = new JobsApiRequest(null, null, "", "", null, uriInfo, jobPayloadBuilder, apiJobStore, preResponseStore, broadcastChannel)

        when: "PreResponse is stored in the PreResponsestore and then a notification is fired by BroadcastChannel"
        preResponseStore.save("ticket0", PreResponseTestingUtils.buildPreResponse("2016-04-20")).toBlocking().first()
        broadcastChannel.publish("ticket0")

        and: "We then subscribe to the reactive chain after the notification has already been fired"
        jobsServlet.getPreResponseObservable("ticket0", apiRequest).subscribe(testSubscriber)

        then: "handleBroadcastChannelNotification returns the expected PreResponse"
        testSubscriber.assertReceivedOnNext([PreResponseTestingUtils.buildPreResponse("2016-04-20")])
    }

    def "getPreResponseObservable returns a preResponseObservable if it is available in the PreResponseStore even if the notification for the ticket is not received before the async timeout"() {
        setup:
        TestSubscriber<PreResponse> testSubscriber = new TestSubscriber<>()
        JobsApiRequest apiRequest = new JobsApiRequest(null, null, "", "", null, uriInfo, jobPayloadBuilder, apiJobStore, preResponseStore, broadcastChannel)

        when: "PreResponse is stored in the PreResponsestore"
        preResponseStore.save("ticket1", PreResponseTestingUtils.buildPreResponse("2016-04-21")).subscribe()

        and: "We then subscribe to the reactive chain"
        jobsServlet.getPreResponseObservable("ticket1", apiRequest).subscribe(testSubscriber)

        then: "handleBroadcastChannelNotification returns the expected PreResponse"
        testSubscriber.assertReceivedOnNext([PreResponseTestingUtils.buildPreResponse("2016-04-21")])
    }

    def "getPreResponseObservable returns a PreResponseObservable even if the PreResponse is not available in the PreResponseStore initially but a notification is received from the broadcastChannel before the async timeout"() {
        setup:
        TestSubscriber<PreResponse> testSubscriber = new TestSubscriber<>()
        JobsApiRequest apiRequest = new JobsApiRequest(null, null, "", "", null, uriInfo, jobPayloadBuilder, apiJobStore, preResponseStore, broadcastChannel)

        when: "we start the async chain"
        jobsServlet.getPreResponseObservable("ticket2", apiRequest).subscribe(testSubscriber)

        then: "The PreResponse is not available yet in the PreResponseStore"
        testSubscriber.assertNoValues()

        when: "PreResponse is stored in the PreResponseStore"
        preResponseStore.save("ticket2", PreResponseTestingUtils.buildPreResponse("2016-04-22")).subscribe()

        and: "We receive the notification from broadcastChannel"
        broadcastChannel.publish("ticket2")

        then: "handleBroadcastChannelNotification returns the expected PreResponse"
        testSubscriber.assertReceivedOnNext([PreResponseTestingUtils.buildPreResponse("2016-04-22")])
    }

    def "getPreResponseObservable returns an empty observable if the PreResponse is not available in the PreResponseStore before the async timeout"() {
        setup:
        TestSubscriber<PreResponse> testSubscriber = new TestSubscriber<>()
        JobsApiRequest apiRequest = new JobsApiRequest(null, "5", "", "", null, uriInfo, jobPayloadBuilder, apiJobStore, preResponseStore, broadcastChannel)

        when: "we start the async chain"
        jobsServlet.getPreResponseObservable("ticket3", apiRequest).subscribe(testSubscriber)
        Thread.sleep(7)

        then: "handleBroadcastChannelNotification returns an empty observable"
        testSubscriber.awaitTerminalEvent()
        testSubscriber.assertNoErrors()
        testSubscriber.assertCompleted()
        testSubscriber.assertNoValues()
    }

    def "If the PreResponse is available in the PreResponseStore and we miss the notification from broadcastChannel, we go to the PreResponseStore exactly once"() {
        setup:
        JobsApiRequest apiRequest1 = new JobsApiRequest(null, "5", "", "", null, uriInfo, jobPayloadBuilder, apiJobStore, mockPreResponseStore, broadcastChannel)

        when: "PreResponse is available in the PreResponseStore"
        mockPreResponseStore.save("ticket4", PreResponseTestingUtils.buildPreResponse("2016-04-24")).subscribe()

        and: "We miss the notification that the preResponse is stored in the PreResponseStore"
        broadcastChannel.publish("ticket4")

        and: "We start the async chain"
        mockJobServlet.getPreResponseObservable("ticket4", apiRequest1)
        Thread.sleep(1000)

        then: "then we go to the PreResponseStore exactly once to get the ticket"
        1 * mockPreResponseStore.get(_) >> Observable.just(Mock(PreResponse))
    }

    def "If the PreResponse is available in the PreResponsestore and the notification from broadcastChannel is not received within async timeout, we go to the PreResponseStore exactly once"() {
        setup:
        JobsApiRequest apiRequest1 = new JobsApiRequest(null, "2", "", "", null, uriInfo, jobPayloadBuilder, apiJobStore, mockPreResponseStore, broadcastChannel)

        when: "PreResponse is available in the PreResponsestore"
        mockPreResponseStore.save("ticket4", PreResponseTestingUtils.buildPreResponse("2016-04-24")).subscribe()

        and: "We start the async chain"
        mockJobServlet.getPreResponseObservable("ticket4", apiRequest1)
        //The delay is to ensure that we get the notification after async timeout
        Thread.sleep(1000)

        and: "We receive the notification after async timeout"
        broadcastChannel.publish("ticket4")

        then: "then we go to the PreResponseStore exactly once to get the ticket"
        1 * mockPreResponseStore.get(_) >> Observable.just(Mock(PreResponse))
    }

    def "If the PreResponse is available in the PreResponseStore and the notification from broadcastChannel is received within the async timeout, we go to the PreResponsestore twice"() {
        setup:
        JobsApiRequest apiRequest1 = new JobsApiRequest(null, null, "", "", null, uriInfo, jobPayloadBuilder, apiJobStore, mockPreResponseStore, broadcastChannel)

        when: "PreResponse is available in the PreResponsestore"
        mockPreResponseStore.save("ticket4", PreResponseTestingUtils.buildPreResponse("2016-04-24")).subscribe()

        and: "We start the async chain"
        mockJobServlet.getPreResponseObservable("ticket4", apiRequest1)

        and: "We receive the notification before the async timeout"
        broadcastChannel.publish("ticket4")

        then: "then we go to the PreResponseStore twice to get the ticket"
        2 * mockPreResponseStore.get(_) >> Observable.just(Mock(PreResponse))
    }

    def "If the PreResponse is not available in the PreResponseStore initially and the notification from broadcastChannel is received within the async timeout, we go to the PreResponsestore twice"() {
        setup:
        JobsApiRequest apiRequest1 = new JobsApiRequest(null, null, "", "", null, uriInfo, jobPayloadBuilder, apiJobStore, mockPreResponseStore, broadcastChannel)

        when: "We start the async chain"
        mockJobServlet.getPreResponseObservable("ticket4", apiRequest1)

        and: "PreResponse then becomes available in the PreResponsestore after some delay"
        Thread.sleep(1000)
        mockPreResponseStore.save("ticket4", PreResponseTestingUtils.buildPreResponse("2016-04-24")).subscribe()

        and: "We receive the notification before the async timeout"
        broadcastChannel.publish("ticket4")

        then: "then we go to the PreResponseStore again to get the ticket"
        2 * mockPreResponseStore.get(_) >> Observable.just(Mock(PreResponse))
    }
}
