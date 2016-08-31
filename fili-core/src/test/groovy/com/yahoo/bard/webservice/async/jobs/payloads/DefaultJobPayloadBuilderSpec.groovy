// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.jobs.payloads

import com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField
import com.yahoo.bard.webservice.async.jobs.jobrows.JobField
import com.yahoo.bard.webservice.async.jobs.jobrows.JobRow
import com.yahoo.bard.webservice.web.JobRequestFailedException

import spock.lang.Specification

import javax.ws.rs.core.UriBuilder
import javax.ws.rs.core.UriInfo

/**
 * Tests for DefaultJobPayloadBuilder.
 */
class DefaultJobPayloadBuilderSpec extends Specification {
    UriInfo uriInfo = Stub(UriInfo)
    String baseUri = "https://localhost:9998/v1/"
    DefaultJobPayloadBuilder defaultJobPayloadBuilder

    def setup() {
        defaultJobPayloadBuilder = new DefaultJobPayloadBuilder()

        uriInfo.getBaseUriBuilder() >> {
            UriBuilder.fromPath(baseUri)
        }
    }

    def "JobRow is mapped correctly to the job to be displayed to the end user"() {
        setup:
        Map<JobField, String> fieldValueMap = [
                (DefaultJobField.JOB_TICKET):"ticket1",
                (DefaultJobField.DATE_CREATED): "2016-01-01",
                (DefaultJobField.DATE_UPDATED): "2016-01-01",
                (DefaultJobField.QUERY): "https://localhost:9998/v1/data/QUERY",
                (DefaultJobField.STATUS): "success",
                (DefaultJobField.USER_ID): "momo"
        ]
        JobRow jobRow = new JobRow(DefaultJobField.JOB_TICKET, fieldValueMap)

        Map<String, String> job = [
                query: "https://localhost:9998/v1/data/QUERY",
                results: "https://localhost:9998/v1/jobs/ticket1/results",
                syncResults: "https://localhost:9998/v1/jobs/ticket1/results?asyncAfter=never",
                self: "https://localhost:9998/v1/jobs/ticket1",
                status: "success",
                jobTicket: "ticket1",
                dateCreated: "2016-01-01",
                userId: "momo"
        ]

        expect:
        defaultJobPayloadBuilder.buildPayload(jobRow, uriInfo) == job
    }

    def "An exception is thrown if the fieldValueMap does not have all required field for the job to be returned to the user"() {
        setup: "build fieldValueMap without query field"
        Map<JobField, String> fieldValueMap = [
                (DefaultJobField.JOB_TICKET):"ticket1",
                (DefaultJobField.DATE_CREATED): "2016-01-01",
                (DefaultJobField.DATE_UPDATED): "2016-01-01",
                (DefaultJobField.STATUS): "success",
                (DefaultJobField.USER_ID): "momo"
        ]
        JobRow jobRow = new JobRow(DefaultJobField.JOB_TICKET, fieldValueMap)

        when:
        defaultJobPayloadBuilder.buildPayload(jobRow, uriInfo)

        then:
        JobRequestFailedException exception = thrown()
        exception.message == "Job with ticket ticket1 cannot be retrieved due to internal error"
    }
}
