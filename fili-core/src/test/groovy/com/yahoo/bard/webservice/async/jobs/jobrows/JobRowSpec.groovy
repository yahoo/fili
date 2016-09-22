// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.async.jobs.jobrows

import static com.yahoo.bard.webservice.async.jobs.JobTestUtils.DATE_CREATED_DATA
import static com.yahoo.bard.webservice.async.jobs.JobTestUtils.DATE_UPDATED_DATA
import static com.yahoo.bard.webservice.async.jobs.JobTestUtils.JOB_TICKET_DATA
import static com.yahoo.bard.webservice.async.jobs.JobTestUtils.QUERY_DATA
import static com.yahoo.bard.webservice.async.jobs.JobTestUtils.STATUS_DATA
import static com.yahoo.bard.webservice.async.jobs.JobTestUtils.USER_ID_DATA
import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.JOB_TICKET
import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.QUERY
import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.STATUS

import com.yahoo.bard.webservice.async.jobs.JobTestUtils

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper

import spock.lang.Specification

class JobRowSpec extends Specification {
    static final ObjectMapper MAPPER = new ObjectMapper()


    def "The JobRow constructor throws an IllegalArgumentException if the valuesMap does not contain the key field"() {
        when: "We construct a JobRow with a map without the key field"
        new JobRow(JOB_TICKET, [(STATUS): DefaultJobStatus.PENDING.getName()])

        then: "An exception is thrown"
        thrown IllegalArgumentException
    }

    def "getRowMap returns a correct representation of the row as a map with strings as keys instead of JobFields"() {
        given: "A JobRow with some data"

        and: "The expected map"
        Map<String, String> expectedMap = [
                jobTicket: JOB_TICKET_DATA,
                query: QUERY_DATA,
                status: STATUS_DATA,
                dateCreated: DATE_CREATED_DATA,
                dateUpdated: DATE_UPDATED_DATA,
                userId: USER_ID_DATA
        ]

        expect:
        JobTestUtils.buildJobRow().getRowMap() == expectedMap
    }

    def "A RowMap serializes correctly"() {
        given: "A JobRow with all the standard fields"
        JobRow row = JobTestUtils.buildJobRow()

        and: "The expected JSON"
        JsonNode expected = MAPPER.readTree(
                """{
                        "query": "$QUERY_DATA",
                        "status": "$STATUS_DATA",
                        "jobTicket": "$JOB_TICKET_DATA",
                        "dateCreated": "$DATE_CREATED_DATA",
                        "dateUpdated": "$DATE_UPDATED_DATA",
                        "userId": "$USER_ID_DATA"
                }"""
        )

        expect:
        MAPPER.valueToTree(row) == expected

    }

    def "equal returns #truefalse for #row1 and #row2"() {
        expect:
        equalityOperation(row1, row2)

        where:
        truefalse | row1                        | row2
        true      | JobTestUtils.buildJobRow()  | JobTestUtils.buildJobRow()
        //Everything is different
        false     | JobTestUtils.buildJobRow(1) | JobTestUtils.buildJobRow(2)
        // Different id fields
        false     | JobTestUtils.buildJobRow()  | JobTestUtils.buildJobRow([(JOB_TICKET): JOB_TICKET_DATA + "!"])
        // Different non-id fields
        false     | JobTestUtils.buildJobRow()  | JobTestUtils.buildJobRow([(QUERY): QUERY_DATA + ",aMetric"])

        equalityOperation = truefalse ? {row1, row2 -> row1 == row2} : {row1, row2 -> row1 != row2}
    }

    def "A job with id #id has hashcode #hashcode"() {
        expect: "A JobRow with the specified id has the specified hashcode"
        JobTestUtils.buildJobRow([(JOB_TICKET): id]).hashCode() == id.hashCode()

        where:
        id << ["1", "An id", "89fa"]
    }
}
