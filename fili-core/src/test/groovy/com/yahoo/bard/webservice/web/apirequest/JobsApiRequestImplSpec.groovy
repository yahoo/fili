// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest

import com.yahoo.bard.webservice.async.broadcastchannels.BroadcastChannel
import com.yahoo.bard.webservice.async.broadcastchannels.SimpleBroadcastChannel
import com.yahoo.bard.webservice.async.jobs.JobTestUtils
import com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField
import com.yahoo.bard.webservice.async.jobs.jobrows.JobField
import com.yahoo.bard.webservice.async.jobs.jobrows.JobRow
import com.yahoo.bard.webservice.async.jobs.payloads.DefaultJobPayloadBuilder
import com.yahoo.bard.webservice.async.jobs.payloads.JobPayloadBuilder
import com.yahoo.bard.webservice.async.jobs.stores.ApiJobStore
import com.yahoo.bard.webservice.async.jobs.stores.HashJobStore
import com.yahoo.bard.webservice.async.jobs.stores.JobRowFilter
import com.yahoo.bard.webservice.async.preresponses.stores.HashPreResponseStore
import com.yahoo.bard.webservice.async.preresponses.stores.PreResponseStore
import com.yahoo.bard.webservice.async.preresponses.stores.PreResponseTestingUtils
import com.yahoo.bard.webservice.web.BadApiRequestException
import com.yahoo.bard.webservice.web.DefaultFilterOperation
import com.yahoo.bard.webservice.web.JobNotFoundException
import com.yahoo.bard.webservice.web.JobRequestFailedException
import com.yahoo.bard.webservice.web.PreResponse

import rx.observers.TestSubscriber
import rx.subjects.PublishSubject;
import spock.lang.Specification

import javax.ws.rs.core.UriBuilder
import javax.ws.rs.core.UriInfo

/**
 * Tests for JobsApiRequestImpl.
 */
class JobsApiRequestImplSpec extends Specification {
    BroadcastChannel<String> broadcastChannel
    UriInfo uriInfo
    JobPayloadBuilder jobPayloadBuilder
    ApiJobStore apiJobStore
    PreResponseStore preResponseStore
    PreResponse ticket1PreResponse
    JobsApiRequest defaultApiRequest

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

        defaultApiRequest = new JobsApiRequestImpl(
                null,
                null,
                "",
                "",
                null,
                uriInfo,
                jobPayloadBuilder,
                apiJobStore
        )
    }

    def "getJobViewObservable returns an Observable wrapping the job to be returned to the user"() {
        setup:
        TestSubscriber<Map<String, String>> getSubscriber = new TestSubscriber<>()
        Map<String, String> job = [
                query : "https://localhost:9998/v1/data/QUERY",
                results : "https://localhost:9998/v1/jobs/ticket1/results",
                syncResults : "https://localhost:9998/v1/jobs/ticket1/results?asyncAfter=never",
                self : "https://localhost:9998/v1/jobs/ticket1",
                status : "success",
                jobTicket : "ticket1",
                dateCreated : "2016-01-01",
                dateUpdated: "2016-01-01",
                userId : "momo"
        ]

        when:
        defaultApiRequest.getJobViewObservable("ticket1").subscribe(getSubscriber)

        then:
        getSubscriber.assertReceivedOnNext([job])
    }

    def "getJobViewObservable results in onError being called on its observers if the job ticket does not exist in the ApiJobStore"() {
        setup:
        TestSubscriber<Map<String, String>> errorSubscriber = new TestSubscriber<>()

        when:
        defaultApiRequest.getJobViewObservable("IDontExist").subscribe(errorSubscriber)

        then:
        errorSubscriber.assertError(JobNotFoundException.class)
        errorSubscriber.getOnErrorEvents().get(0).getMessage() == "No job found with job ticket IDontExist"
    }

    def "getJobViewObservable results in onError being called on it's observers if the JobRow cannot be correctly mapped to a Job payload"() {
        setup:
        TestSubscriber<Map<String, String>> errorSubscriber = new TestSubscriber<>()

        when:
        defaultApiRequest.getJobViewObservable("badTicket").subscribe(errorSubscriber)

        then:
        errorSubscriber.assertError(JobRequestFailedException.class)
        errorSubscriber.getOnErrorEvents().get(0).getMessage() == "Job with ticket badTicket cannot be retrieved due to internal error"
    }

    def "getJobViews results in onError being called on it's observers if any of the JobRows in the ApiJobStore cannot be correctly mapped to a Job payload"() {
        setup:
        TestSubscriber<Map<String, String>> errorSubscriber = new TestSubscriber<>()

        when:
        defaultApiRequest.getJobViews().subscribe(errorSubscriber)

        then:
        errorSubscriber.assertError(JobRequestFailedException.class)
        errorSubscriber.getOnErrorEvents().get(0).getMessage() == "Jobs cannot be retrieved successfully due to internal error"
    }

    def "buildJobStoreFilter correctly parses a single filter query and returns ApiJobStoreFilter"() {
        setup:
        String filterQuery = "userId-eq[foo]"

        when:
        Set<JobRowFilter> filters = defaultApiRequest.buildJobStoreFilter(filterQuery)

        then:
        filters.size() == 1
        JobRowFilter filter = filters[0]
        filter.jobField?.name == "userId"
        filter.operation == DefaultFilterOperation.fromString("eq")
        filter.values == ["foo"] as Set
    }

    def "buildJobStoreFilter correctly parses multiple filter query"() {
        setup:
        String filterQuery = "userId-eq[foo],status-eq[success]"

        when:
        Set<JobRowFilter> filters = defaultApiRequest.buildJobStoreFilter(filterQuery)

        then:
        filters.size() == 2

        JobRowFilter filter1 = filters[0]
        filter1.jobField?.name == "userId"
        filter1.operation == DefaultFilterOperation.fromString("eq")
        filter1.values == ["foo"] as Set

        JobRowFilter filter2 = filters[1]
        filter2.jobField?.name == "status"
        filter2.operation == DefaultFilterOperation.fromString("eq")
        filter2.values == ["success"] as Set
    }

    def "buildJobStoreFilter throws BadApiRequestException for bad filter query"() {
        setup:
        //Bad filter query without ApiJobStoreFilterOperation
        String badFilterQuery = "userId[foo]"

        when:
        defaultApiRequest.buildJobStoreFilter(badFilterQuery)

        then:
        BadApiRequestException exception = thrown()
        exception.message == "Filter expression 'userId[foo]' is invalid."
    }

    def "getFilteredJobViews returns JobRows that satisfy the given filter"() {
        setup:
        TestSubscriber<JobRow> testSubscriber = new TestSubscriber<>()

        HashJobStore apiJobStore = new HashJobStore()
        JobRow userFooJobRow1 = JobTestUtils.buildJobRow(1)
        JobRow userFooJobRow2 = JobTestUtils.buildJobRow(2)
        JobRow userFooJobRow3 = JobTestUtils.buildJobRow(3)

        apiJobStore.save(userFooJobRow1)
        apiJobStore.save(userFooJobRow2)
        apiJobStore.save(userFooJobRow3)

        JobsApiRequestImpl apiRequest = new JobsApiRequestImpl(
                null,
                null,
                "",
                "",
                "userId-eq[Number 1,Number 2]",
                uriInfo,
                jobPayloadBuilder,
                apiJobStore
        )

        Map<String, String> jobPayload1 = [
                jobTicket : "1",
                query : "https://host:port/v1/data/table/grain?metrics=1",
                results : "https://localhost:9998/v1/jobs/1/results",
                syncResults : "https://localhost:9998/v1/jobs/1/results?asyncAfter=never",
                self : "https://localhost:9998/v1/jobs/1",
                status : "pending",
                dateCreated : "0001-04-29T00:00:00.000Z",
                dateUpdated : "0001-04-30T00:00:00.000Z",
                userId : "Number 1"
        ]

        Map<String, String> jobPayload2 = [
                jobTicket : "2",
                query : "https://host:port/v1/data/table/grain?metrics=2",
                results : "https://localhost:9998/v1/jobs/2/results",
                syncResults : "https://localhost:9998/v1/jobs/2/results?asyncAfter=never",
                self : "https://localhost:9998/v1/jobs/2",
                status : "pending",
                dateCreated : "0002-04-29T00:00:00.000Z",
                dateUpdated : "0002-04-30T00:00:00.000Z",
                userId : "Number 2"
        ]

        when:
        apiRequest.getJobViews().subscribe(testSubscriber)

        then:
        testSubscriber.assertReceivedOnNext([jobPayload1, jobPayload2])
    }
}
