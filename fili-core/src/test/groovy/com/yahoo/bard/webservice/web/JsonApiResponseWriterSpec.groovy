// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import com.yahoo.bard.webservice.util.GroovyTestUtils
import com.yahoo.bard.webservice.util.JsonSortStrategy
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import spock.lang.Unroll

class JsonApiResponseWriterSpec extends ResponseWriterSpec {

    @Unroll
    def "JSONApi response is correct for a known result set with link names #linkNames"() {
        setup:
        apiRequest.getFormat() >> DefaultResponseFormatType.JSONAPI
        formattedDateTime = dateTime.toString(getDefaultFormat())
        GString metaBlock = """{
                        "pagination": {
                            $bodyLinksAsJson,
                            "currentPage": $PAGE,
                            "rowsPerPage": $PER_PAGE,
                            "numberOfResults": 6
                        }
                    }"""

        response = new ResponseData(
                resultSet,
                apiRequest,
                new SimplifiedIntervalList(),
                volatileIntervals,
                pagination,
                bodyLinks
        )

        String expectedJson = withMetaObject(defaultJsonApiFormat, metaBlock)

        ByteArrayOutputStream os = new ByteArrayOutputStream()
        jsonApiResponseWriter = new JsonApiResponseWriter(MAPPERS)
        jsonApiResponseWriter.write(apiRequest, response, os)

        expect:
        GroovyTestUtils.compareJson(os.toString(), expectedJson, JsonSortStrategy.SORT_BOTH)

        where:
        linkNames << LINK_NAMES_LIST
        bodyLinks << BODY_LINKS_LIST
        bodyLinksAsJson << BODY_LINKS_AS_JSON_LIST
    }
}
