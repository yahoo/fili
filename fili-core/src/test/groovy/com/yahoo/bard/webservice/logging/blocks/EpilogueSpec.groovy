// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging.blocks

import static javax.ws.rs.core.Response.Status.OK

import com.yahoo.bard.webservice.util.CacheLastObserver
import com.yahoo.bard.webservice.util.GroovyTestUtils

import com.fasterxml.jackson.databind.ObjectMapper

import spock.lang.Specification
import spock.lang.Unroll

import rx.Observable


class EpilogueSpec extends Specification {

    static final ObjectMapper JSON_SERIALIZER = new ObjectMapper()

    @Unroll
    def "When the responseLengthObserver receives #messages and #errorType Epilogue serializes correctly"() {
        given: "The Observer that acts as a push-based future for the response length"
        CacheLastObserver<Long> responseLengthObserver = new CacheLastObserver<>()

        and: "The Epilogue under test"
        Epilogue epilogue = new Epilogue("Help I'm trapped in a lumbermill!", OK, responseLengthObserver)

        and: "The expected serialization of Epilogue"
        String expectedSerialization = """{
                "status": "$OK.reasonPhrase",
                "code": $OK.statusCode,
                "logMessage": "Help I'm trapped in a lumbermill!",
                "responseLength": ${messages ? messages[-1] : Epilogue.LENGTH_UNKNOWN},
                "connectionClosedPrematurely": $connectionClosedPrematurely
        }"""

        when: "We fire some messages into the response length observer"
        Observable.create { subscriber ->
                messages.each {subscriber.onNext(it)}
                if (error != null) {
                    subscriber.onError(error)
                }
        }
        .subscribe(responseLengthObserver)

        then: "The epilogue serializes appropriately"
        GroovyTestUtils.compareJson(JSON_SERIALIZER.writeValueAsString(epilogue), expectedSerialization)

        where:
        messages      | error                   | connectionClosedPrematurely
        []            | null                    | false
        []            | new RuntimeException()  | false
        []            | new EOFException()      | true

        [1l]          | null                    | false
        [1l]          | new RuntimeException()  | false
        [1l]          | new EOFException()      | true

        [5l, 8l, 13l] | null                    | false
        [5l, 8l, 13l] | new RuntimeException()  | false
        [5l, 8l, 13l] | new EOFException()      | true

        errorType = error ? error.getClass() : "no error"
    }
}
