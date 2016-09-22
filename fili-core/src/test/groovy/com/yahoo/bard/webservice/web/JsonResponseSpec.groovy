// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.util.GroovyTestUtils
import com.yahoo.bard.webservice.util.JsonSlurper
import com.yahoo.bard.webservice.util.JsonSortStrategy
import com.yahoo.bard.webservice.util.Pagination

import spock.lang.Specification
import spock.lang.Unroll

import javax.ws.rs.core.UriBuilder
import javax.ws.rs.core.UriInfo

class JsonResponseSpec extends Specification {
    private static final ObjectMappersSuite MAPPERS = new ObjectMappersSuite()

    static final int PAGE = 2
    static final int PER_PAGE = 2

    static final GString METADATA_BLOCK = """{
                        "pagination": {
                            "paginationLinks": {
                                "first":"http://localhost:9998/v1/metrics/?page=1",
                                "last":"http://localhost:9998/v1/metrics/?page=3",
                                "next":"http://localhost:9998/v1/metrics/?page=3",
                                "previous":"http://localhost:9998/v1/metrics/?page=1"
                            },
                            "currentPage": $PAGE,
                            "rowsPerPage": $PER_PAGE,
                            "numberOfResults": 6
                        }
                    }"""

    String metricRowsString = """{
        "metrics": [
            {"category": "General", "name":"metricA", "longName": "metricA", "uri":"http://localhost:9998/metrics/metricA"},
            {"category": "General", "name":"metricB", "longName": "metricB", "uri":"http://localhost:9998/metrics/metricB"},
            {"category": "General", "name":"metricC", "longName": "metricC", "uri":"http://localhost:9998/metrics/metricC"}
        ]
    }"""

    Set<Map<String, String>> metricRows = [
            ["category": "General", "name":"metricA", "longName": "metricA", "uri":"http://localhost:9998/metrics/metricA"],
            ["category": "General", "name":"metricB", "longName": "metricB", "uri":"http://localhost:9998/metrics/metricB"],
            ["category": "General", "name":"metricC", "longName": "metricC", "uri":"http://localhost:9998/metrics/metricC"],
        ]

    UriInfo uriInfo

    def setup() {
        uriInfo = Mock(UriInfo)
        uriInfo.getRequestUri() >> { new URI("http://localhost:9998/v1/metrics/") }
        uriInfo.getRequestUriBuilder() >> { UriBuilder.fromUri(new URI("http://localhost:9998/v1/metrics/")) }
    }

    Pagination stubPagination() {
        Stub(Pagination) {
            getFirstPage() >> OptionalInt.of(1)
            getLastPage() >> OptionalInt.of(3)
            getNextPage() >> OptionalInt.of(3)
            getPreviousPage() >> OptionalInt.of(1)
            getPage() >> PAGE
            getNumResults() >> 6
            getPerPage() >> PER_PAGE
        }
    }

    @Unroll
    def "The JsonResponse correctly serializes (#notincluding a metadata block) with #pages for pagination"() {
        given: "A JsonResponse with the appropriate Pagination"

        JsonResponse jsonResponse = new JsonResponse<>(
                metricRows.stream(),
                pages,
                uriInfo,
                "metrics",
                MAPPERS
        )

        and: "the equivalent expected metablock"
        String expectedJson = withMetaObject(metricRowsString, metaBlock)

        and: "An output stream to write the reponse to"
        ByteArrayOutputStream baos = new ByteArrayOutputStream()

        when: "The Json response is created and writes to the output stream"
        jsonResponse.write(baos)

        then: "The JSON written to the stream matches what we expected"
        GroovyTestUtils.compareJson(baos.toString(), expectedJson)

        where:
        not      | pages            | metaBlock
        "not "   | null             | GString.EMPTY
        ""       | stubPagination() | METADATA_BLOCK
    }

    /**
     * Adds {@code metaBlock} to {@code jsonString} with the {@code meta} label.
     * If the meta block is falsy, the original string is returned unmodified.
     *
     * @param jsonString  The JSON to add the meta block to
     * @param metaBlock  The block to add to the JSON string with the {@code meta} label
     *
     * @return A JSON string with a new JSON object called {@code meta} that maps to to the passed in {@code metaBlock},
     * or the original String if {@code metaBlock} is falsy.
     */
    String withMetaObject(String jsonString, GString metaBlock) {
        if (!metaBlock) {
            return jsonString
        }
        JsonSlurper jsonSlurper = new JsonSlurper(JsonSortStrategy.SORT_NONE)
        def baseJson = jsonSlurper.parseText(jsonString) as Map
        def metaJson = jsonSlurper.parseText(metaBlock)
        baseJson.put("meta", metaJson)
        MAPPERS.getMapper().writeValueAsString(baseJson)
    }
}
