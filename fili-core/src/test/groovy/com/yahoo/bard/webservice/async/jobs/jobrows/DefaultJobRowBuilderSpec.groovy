// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.jobs.jobrows

import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.DATE_CREATED
import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.DATE_UPDATED
import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.JOB_TICKET
import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.QUERY
import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.STATUS
import static com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField.USER_ID

import org.joda.time.DateTime

import spock.lang.Specification
import java.security.Principal
import java.time.Clock
import java.time.Instant
import java.time.ZoneId

import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.core.SecurityContext
import javax.ws.rs.core.UriInfo


class DefaultJobRowBuilderSpec extends Specification {

    static final String QUERY_STRING = "https://host:4443/v1/data/someTable/day/gender?metrics=pageViews&" +
            "dateTime=2014-09-01/2014-09-08"

    static final String USER_NAME = "greg"

    static final String UUID = "this_id_is_unique_just_like_everybody_else"

    def "A DefaultJobRow is built correctly"() {
        given: "UriInfo and RequestContext that will be used to populate the jobMetadata"
        UriInfo request = Stub(UriInfo) {
            getRequestUri() >> new URI(QUERY_STRING)
        }
        Instant testTime = Instant.now()

        ContainerRequestContext requestContext = Stub(ContainerRequestContext) {
            getSecurityContext() >> Stub(SecurityContext) {
                getUserPrincipal() >> Stub(Principal) {
                    getName() >> USER_NAME
                }
            }
        }

        when: "We build the job jobMetadata"
        JobRow jobMetadata = new DefaultJobRowBuilder(
                {it[USER_ID] + UUID},
                {USER_NAME},
                Clock.fixed(testTime, ZoneId.systemDefault())
        )
                .buildJobRow(request, requestContext)

        then: "The JobRow is built correctly"
        jobMetadata.get(QUERY) == QUERY_STRING
        jobMetadata.get(STATUS) == DefaultJobStatus.PENDING.name
        jobMetadata.get(USER_ID) == USER_NAME
        jobMetadata.get(DATE_CREATED) == new DateTime(testTime.toEpochMilli()).toDateTimeISO().toString()
        jobMetadata.get(DATE_CREATED) == jobMetadata.get(DATE_UPDATED)
        jobMetadata.get(JOB_TICKET) == USER_NAME + UUID
    }
}
