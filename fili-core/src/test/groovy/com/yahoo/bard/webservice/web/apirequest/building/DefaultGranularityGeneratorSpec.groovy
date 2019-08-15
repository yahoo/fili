package com.yahoo.bard.webservice.web.apirequest.building

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.bard.webservice.data.time.AllGranularity
import com.yahoo.bard.webservice.data.time.GranularityParser
import com.yahoo.bard.webservice.web.BadApiRequestException
import com.yahoo.bard.webservice.web.ErrorMessageFormat

import spock.lang.Specification
import spock.lang.Unroll

class DefaultGranularityGeneratorSpec extends Specification {
    @Unroll
    def "check valid granularity name #name parses to granularity #expected"() {
        expect:
        GranularityGenerator.DEFAULT_GRANULARITY_GENERATOR.generateGranularity(name, GranularityParser.DEFAULT_PARSER) == expected

        where:
        name    | expected
        "day"   | DAY
        "all"   | AllGranularity.INSTANCE
    }

    def "check invalid granularity creates error"() {
        setup: "Define an improper granularity name"
        String timeGrainName = "seldom"
        String expectedMessage = ErrorMessageFormat.UNKNOWN_GRANULARITY.format(timeGrainName)

        when:
        GranularityGenerator.DEFAULT_GRANULARITY_GENERATOR.generateGranularity(timeGrainName, GranularityParser.DEFAULT_PARSER)

        then:
        Exception e = thrown(BadApiRequestException)
        e.getMessage() == expectedMessage
    }
}
