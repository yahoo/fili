// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging.blocks

import com.yahoo.bard.webservice.web.ErrorMessageFormat
import spock.lang.Specification
import spock.lang.Unroll

class BardQueryInfoSpec extends Specification {
    BardQueryInfo bardQueryInfo

    def setup() {
        bardQueryInfo = BardQueryInfoUtils.initializeBardQueryInfo()
    }

    def cleanup() {
        BardQueryInfoUtils.resetBardQueryInfo()
    }

    def "getBardQueryInfo() returns registered BardQueryInfo instance"() {
        expect:
        BardQueryInfo.getBardQueryInfo() == bardQueryInfo
    }

    @Unroll
    def "incrementCountFor(#queryType) increment count of #queryType by 1"() {
        expect: "count for #queryType is 0"
        BardQueryInfo.QUERY_COUNTER.get(queryType).get() == 0

        when: "calling incrementCountFor(#queryType)"
        BardQueryInfo.incrementCountFor(queryType)

        then:
        BardQueryInfo.QUERY_COUNTER.get(queryType).get() == 1

        where:
        queryType                          | _
        BardQueryInfo.WEIGHT_CHECK         | _
        BardQueryInfo.FACT_QUERIES         | _
        BardQueryInfo.FACT_QUERY_CACHE_HIT | _
    }

    def "incrementCountFor(String) throws IllegalArgumentException on non-existing query type"() {
        when:
        BardQueryInfo.incrementCountFor("nonExistingQueryType")

        then:
        IllegalArgumentException illegalArgumentException = thrown()
        illegalArgumentException.message == ErrorMessageFormat.RESOURCE_RETRIEVAL_FAILURE.format("nonExistingQueryType")
    }

    def "incrementCount*() methods increment their corresponding query counts by 1"() {
        expect: "all query counts are 0"
        BardQueryInfo.QUERY_COUNTER.get(BardQueryInfo.WEIGHT_CHECK).get() == 0
        BardQueryInfo.QUERY_COUNTER.get(BardQueryInfo.FACT_QUERIES).get() == 0
        BardQueryInfo.QUERY_COUNTER.get(BardQueryInfo.FACT_QUERY_CACHE_HIT).get() == 0

        when:
        BardQueryInfo.incrementCountWeightCheck()
        BardQueryInfo.incrementCountFactHits()
        BardQueryInfo.incrementCountCacheHits()

        then:
        BardQueryInfo.QUERY_COUNTER.get(BardQueryInfo.WEIGHT_CHECK).get() == 1
        BardQueryInfo.QUERY_COUNTER.get(BardQueryInfo.FACT_QUERIES).get() == 1
        BardQueryInfo.QUERY_COUNTER.get(BardQueryInfo.FACT_QUERY_CACHE_HIT).get() == 1
    }
}
