// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging.blocks

import com.yahoo.bard.webservice.application.ObjectMappersSuite

import spock.lang.Specification
import spock.lang.Unroll

import java.util.concurrent.atomic.AtomicInteger

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
    def "Validate cachePutFailures LogInfo is serialized correctly"() {
        when:
        bardQueryInfo.addPutFailureInfo("test", new BardCacheInfo("setFailure", 10, "test",100))

        then:
        new ObjectMappersSuite().jsonMapper.writeValueAsString(
                bardQueryInfo
        ) == """{"type":"test","queryCounter":{"factQueryCount":0,"weightCheckQueries":0,"factCachePutErrors":0,"factCachePutTimeouts":0,"factCacheHits":0},"cachePutFailures":[{"opType":"setFailure","cacheKeyCksum":"test","cacheKeyLen":10,"cacheValLen":100}],"cacheReadFailures":[]}"""
    }

    @Unroll
    def "increment Count For #queryType increments counter by 1"() {
        setup:
        AtomicInteger counter = BardQueryInfo.bardQueryInfo.queryCounter.get(queryType);

        expect: "count for #queryType is 0"
        counter.get() == 0

        when: "calling incrementCountFor(#queryType)"
        incrementor()

        then: "count of #queryType is incremented by 1"
        counter.get() == 1

        where:
        queryType                          | incrementor
        BardQueryInfo.WEIGHT_CHECK         | BardQueryInfo.&incrementCountWeightCheck
        BardQueryInfo.FACT_QUERIES         | BardQueryInfo.&incrementCountFactHits
        BardQueryInfo.FACT_QUERY_CACHE_HIT | BardQueryInfo.&incrementCountCacheHits
        BardQueryInfo.FACT_PUT_ERRORS      | BardQueryInfo.&incrementCountCacheSetFailures
        BardQueryInfo.FACT_PUT_TIMEOUTS    | BardQueryInfo.&incrementCountCacheSetTimeoutFailures
    }

    def "Object serializes with type and map"() {
        expect:
        new ObjectMappersSuite().jsonMapper.writeValueAsString(
                bardQueryInfo
        ) == """{"type":"test","queryCounter":{"factQueryCount":0,"weightCheckQueries":0,"factCachePutErrors":0,"factCachePutTimeouts":0,"factCacheHits":0},"cachePutFailures":[],"cacheReadFailures":[]}"""
    }
}
