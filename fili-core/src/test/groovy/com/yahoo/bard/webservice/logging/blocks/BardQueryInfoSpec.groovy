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
        bardQueryInfo.getBardQueryInfo() == bardQueryInfo
    }

    @Unroll
    def "incrementCountFor(#queryType) increments count of #queryType by 1"() {
        expect: "count for #queryType is 0"
        bardQueryInfo.queryCounter.get(queryType).get() == 0

        when: "calling incrementCountFor(#queryType)"
        bardQueryInfo.incrementCountFor(queryType)

        then: "count of #queryType is incremented by 1"
        bardQueryInfo.queryCounter.get(queryType).get() == 1

        where:
        queryType                          | _
        BardQueryInfo.WEIGHT_CHECK         | _
        BardQueryInfo.FACT_QUERIES         | _
        BardQueryInfo.FACT_QUERY_CACHE_HIT | _
    }

    def "incrementCountFor(String) throws IllegalArgumentException on non-existing query type"() {
        when: "BardQueryInfo is given an unknown query type"
        bardQueryInfo.incrementCountFor("nonExistingQueryType")

        then: "IllegalArgumentException is thrown with exception message"
        IllegalArgumentException illegalArgumentException = thrown()
        illegalArgumentException.message == ErrorMessageFormat.RESOURCE_RETRIEVAL_FAILURE.format("nonExistingQueryType")
    }

    def "incrementCount*() methods increment their corresponding query type counts by 1"() {
        expect: "all query counts are 0"
        bardQueryInfo.queryCounter.get(BardQueryInfo.WEIGHT_CHECK).get() == 0
        bardQueryInfo.queryCounter.get(BardQueryInfo.FACT_QUERIES).get() == 0
        bardQueryInfo.queryCounter.get(BardQueryInfo.FACT_QUERY_CACHE_HIT).get() == 0

        when: "calling incrementCount*() methods for all query types"
        bardQueryInfo.incrementCountWeightCheck()
        bardQueryInfo.incrementCountFactHits()
        bardQueryInfo.incrementCountCacheHits()

        then: "counts of all query types are incremented by 1"
        bardQueryInfo.queryCounter.get(BardQueryInfo.WEIGHT_CHECK).get() == 1
        bardQueryInfo.queryCounter.get(BardQueryInfo.FACT_QUERIES).get() == 1
        bardQueryInfo.queryCounter.get(BardQueryInfo.FACT_QUERY_CACHE_HIT).get() == 1
    }
}
