// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.async.jobs.payloads

import spock.lang.Specification

import javax.ws.rs.core.UriBuilder
import javax.ws.rs.core.UriInfo

/**
 * Tests for DefaultJobRowMapper.
 */
class JobPayLoadBuilderSpec extends Specification {
    UriInfo uriInfo = Stub(UriInfo)
    String baseUri = "https://localhost:9998/v1/"

    def setup() {
        uriInfo.getBaseUriBuilder() >> {
            UriBuilder.fromPath(baseUri)
        }
    }

    def "We are able to get the correct results url"() {
        expect:
        JobPayloadBuilder.getResultsUrl("ticket1", uriInfo) == "https://localhost:9998/v1/jobs/ticket1/results"
    }

    def "We are able to get the correct syncResults url"() {
        expect:
        JobPayloadBuilder.getSyncResultsUrl("ticket1", uriInfo) == "https://localhost:9998/v1/jobs/ticket1/results?asyncAfter=never"
    }

    def "We are able to get the correct self url"() {
        expect:
        JobPayloadBuilder.getSelfUrl("ticket1", uriInfo) == "https://localhost:9998/v1/jobs/ticket1"
    }
}
