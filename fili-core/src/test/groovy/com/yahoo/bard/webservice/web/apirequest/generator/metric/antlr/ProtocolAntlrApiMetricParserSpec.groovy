// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.metric.antlr

import com.yahoo.bard.webservice.web.apirequest.metrics.ApiMetric

import spock.lang.Specification
import spock.lang.Unroll

class ProtocolAntlrApiMetricParserSpec extends Specification {

    ProtocolAntlrApiMetricParser generator = new ProtocolAntlrApiMetricParser()

    def "Parser produces correct value for list of metrics"() {
        setup:
        def expected = []
        String key
        key = "one(bar=baz)"
        ApiMetric expectedMetric = new ApiMetric(key.replace(" ", ""), "one", ["bar": "baz"])
        expected.add(expectedMetric)
        key = "two(  )"
        expectedMetric = new ApiMetric(key.replace(" ", ""), "two", [:])
        expected.add(expectedMetric)
        key = " three "
        expectedMetric = new ApiMetric(key.replace(" ", ""), "three", [:])
        expected.add(expectedMetric)
        key = "four(bar=baz, one = two)"
        expectedMetric = new ApiMetric(key.replace(" ", ""), "four", ["bar": "baz", "one": "two"])
        expected.add(expectedMetric)
        def query = expected.collect {it.rawName}.join(",")

        expect:
        generator.apply(query) == expected
    }

    // Ignorable whitespace used in many expamples.
    @Unroll
    def "Parser produces #metric and #params from text: #text"() {
        expect:
        //generator.apply(text) == [new ApiMetric(text, metric, params)]
        generator.apply(text) == [new ApiMetric(text.replace(" ", ""), metric, params)]


        where:
        text                              | metric   | params
        // unquoted values

        "two ( )"                         | "two"    | [:]
        "one(bar=baz)"                    | "one"    | ["bar": "baz"]
        "three"                           | "three"  | [:]
        "four(bar=baz, one=two)"          | "four"   | ["bar": "baz", "one": "two"]
        "five( bar=baz, one=three )"      | "five"   | ["bar": "baz", "one": "three"]
        "six(bar=baz, 1=2)"               | "six"    | ["bar": "baz", "1": "2"]
        "seven(bar=baz , 1=7)"            | "seven"  | ["bar": "baz", "1": "7"]
        "eight(bar|baz=bazz, 1=7)"        | "eight"  | ["bar|baz": "bazz", "1": "7"]

        // escaped values
        "eight( bar=(baz), one=(two) )"   | "eight"  | ["bar": "baz", "one": "two"]

        "nine(bar=(baz),1=2)"             | "nine"   | ["bar": "baz", "1": "2"]
        "ten(bar=(|,😃,|))"               | "ten"    | ["bar": "|,😃,|"]
        "ten(bar=(\\\\)))"                | "ten"    | ["bar": "\\)"]
        "eleven(bar=())"                  | "eleven" | ["bar": ""]
        "twelve(bar=(\"hello\"\\)world))" | "twelve" | ["bar": "\"hello\")world"]
    }

    // We're not injecting ignorable whitespace here because it's hard to frame the test to ignore
    // whitespace in escaped expression.  Ignorable whitespace is tested above.
    @Unroll
    def "Parser correctly escapes whitespace for  #metric and #params from text: #text"() {
        expect:
        generator.apply(text) == [new ApiMetric(text, metric, params)]
        where:
        text                              | metric   | params
        // unquoted values

        "two()"                           | "two"    | [:]
        // escaped values
        "eight(bar=(ba z),one=(two))"     | "eight"  | ["bar": "ba z", "one": "two"]

        "nine(bar=(baz  ),1=2)"           | "nine"   | ["bar": "baz  ", "1": "2"]
        "ten(bar=(|,😃,|))"               | "ten"    | ["bar": "|,😃,|"]
        "ten(bar=(\\\\)))"                | "ten"    | ["bar": "\\)"]
        "eleven(bar=('  '))"              | "eleven" | ["bar": "'  '"]
        "twelve(bar=(\"hello\"\\)world))" | "twelve" | ["bar": "\"hello\")world"]
    }
}
