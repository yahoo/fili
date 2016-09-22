// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.bard.webservice.data.time.StandardGranularityParser
import com.yahoo.bard.webservice.data.time.GranularityParser
import com.yahoo.bard.webservice.druid.model.query.AllGranularity

import spock.lang.Specification
import spock.lang.Unroll

class ApiRequestSpec extends Specification {

    GranularityParser granularityParser = new  StandardGranularityParser()

    class ConcreteApiRequest extends ApiRequest {}

    def "check valid parsing generateFormat"() {

        expect:
        responseFormat == expectedFormat

        where:
        responseFormat          | expectedFormat
        ResponseFormatType.JSON | new ConcreteApiRequest().generateAcceptFormat(null)
        ResponseFormatType.JSON | new ConcreteApiRequest().generateAcceptFormat("json")
        ResponseFormatType.CSV  | new ConcreteApiRequest().generateAcceptFormat("csv")
    }

    def "check invalid parsing generateFormat"() {
        when:
        new ConcreteApiRequest().generateAcceptFormat("bad")

        then:
        thrown BadApiRequestException
    }

    @Unroll
    def "check valid granularity name #name parses to granularity #expected"() {
        expect:
        new ConcreteApiRequest().generateGranularity(name, granularityParser) == expected

        where:
        name  | expected
        "day" | DAY
        "all" | AllGranularity.INSTANCE
    }

    def "check invalid granularity creates error"() {
        setup: "Define an improper granularity name"
        String timeGrainName = "seldom"
        String expectedMessage = ErrorMessageFormat.UNKNOWN_GRANULARITY.format(timeGrainName)

        when:
        new ConcreteApiRequest().generateGranularity(timeGrainName, new StandardGranularityParser())

        then:
        Exception e = thrown(BadApiRequestException)
        e.getMessage() == expectedMessage
    }
}
