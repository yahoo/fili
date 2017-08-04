// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.cache

import static com.yahoo.bard.webservice.config.BardFeatureFlag.DRUID_CACHE
import static com.yahoo.bard.webservice.config.BardFeatureFlag.DRUID_CACHE_V2

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.metadata.SegmentIntervalsHashIdGenerator
import com.yahoo.bard.webservice.table.availability.AvailabilityTestingUtils
import com.yahoo.bard.webservice.util.GroovyTestUtils
import com.yahoo.bard.webservice.web.endpoints.DataServlet

import org.joda.time.Interval

import net.spy.memcached.MemcachedClient
import spock.lang.IgnoreIf
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

@IgnoreIf({!DRUID_CACHE.isOn()})
class MemTupleDataCacheSpec extends Specification {
    JerseyTestBinder jtb
    @Shared boolean cacheV2Status
    long defaultCheckSum = -1234L
    long expectedCheckSum = -1234L
    @Shared TupleDataCache<String, Long, String> cache

    def setup() {
        cacheV2Status = DRUID_CACHE_V2.isOn();
        DRUID_CACHE_V2.setOn(true)
        // Create the test web container to test the resources
        jtb = new JerseyTestBinder(false, DataServlet.class)
        if (System.getenv("BUILD_NUMBER") != null) {
            // Only use real memcached client if in the CI environment
            jtb.dataCache = new MemTupleDataCache<String>()
        }
        jtb.querySigningService = Mock(SegmentIntervalsHashIdGenerator)
        jtb.querySigningService.getSegmentSetId(_) >> { Optional.of(defaultCheckSum) }
        jtb.start()

        Interval interval = new Interval("2010-01-01/2500-12-31")
        AvailabilityTestingUtils.populatePhysicalTableCacheIntervals(jtb, interval)
        cache = (TupleDataCache<String, Long, String>) jtb.dataCache
    }

    def cleanup() {
        DRUID_CACHE_V2.setOn(cacheV2Status)
        // Release the test web container
        assert jtb != null
        jtb.tearDown()
    }

    def "cache misses on error without invalidating"() {
        setup:  "Give the cache a client that will throw an error in the middle of hits"
        MemcachedClient client = Mock(MemcachedClient)
        MemTupleDataCache.DataEntry<String> tuple = new MemTupleDataCache.DataEntry<String>(
                "key",
                expectedCheckSum,
                "value"
        );

        client.get(_) >> tuple >> { throw new RuntimeException() } >> tuple

        MemTupleDataCache localCache = new MemTupleDataCache<>(client)

        when: "get a healthy object"
        String cacheValue = localCache.getDataValue("key")

        then:
        cacheValue == tuple.getValue()

        when: "get an exception"
        cacheValue = localCache.get("key")

        then: "return null (a cache miss)"
        cacheValue == null

        when: "get a good hit"
        cacheValue =  localCache.getDataValue("key")

        then: "hits continue"
        cacheValue == tuple.getValue()
    }

    @Unroll
    def "cache set and get #key1 and #key2"() {
        when: "set"
        cache.set(key1, checkSum1, value1)
        cache.set(key2, checkSum2, value2)

        then: "get"
        cache.getDataValue(key1) == value1
        cache.getDataValue(key2) == value2

        when: "update"
        cache.set(key1, checkSum2, value2)
        cache.set(key2, checkSum2, value1)

        then: "get"
        cache.getDataValue(key1) == value2
        cache.getDataValue(key2) == value1

        where:
        key1          | key2          | checkSum1  | checkSum2  | value1         | value2
        "key"         | "key2"        | 1234L      | -8765L     | "value"        | "value2"
        "key1\ufe01@" | "key2\ufe02@" | -4321L     | 5678L      | "value\ufe01#" | "value\ufe02#"
    }

    @Unroll
    def "cache throws #exception.simpleName because #reason"() {
        when:
        cache.set(key, checkSum, value)

        then:
        thrown(exception)

        where:
        key  | checkSum  | value | exception             | reason
        null | 0L        | null  | NullPointerException  | "cache key is null"
    }

    def "servlet caching"() {
        setup:
        assert jtb != null
        String response =
            """[
              {
                "version" : "v1",
                "timestamp" : "2014-06-10T00:00:00.000Z",
                "event" : {
                  "color" : "Baz",
                  "width" : 9
                }
              }
            ]"""

       String expected =
       """{
           "rows":[{"dateTime":"2014-06-10 00:00:00.000","color|id":"Baz","color|desc":"","width":9}]
           }"""

        when: "initial request"
        jtb.druidWebService.jsonResponse = {response}

        String result = jtb.getHarness().target("data/shapes/day/color")
            .queryParam("metrics","width")
            .queryParam("dateTime","2014-06-10%2F2014-06-11")
            .request().get(String.class)

        then: "result should match"
        GroovyTestUtils.compareJson(result, expected)

        when: "subsequent request"
        jtb.druidWebService.jsonResponse = {response}

        result = jtb.getHarness().target("data/shapes/day/color")
            .queryParam("metrics","width")
            .queryParam("dateTime","2014-06-10%2F2014-06-11")
            .request().get(String.class)

        then: "result should match due to a cache hit"
        GroovyTestUtils.compareJson(result, expected)

        when: "druid result changes only value"
        jtb.druidWebService.jsonResponse = {"[]"}

        result = jtb.getHarness().target("data/shapes/day/color")
            .queryParam("metrics","width")
            .queryParam("dateTime","2014-06-10%2F2014-06-11")
            .request().get(String.class)

        then: "should read old result from cache"
        GroovyTestUtils.compareJson(result, expected)

        when: "druid result changes both value and segment metadata"
        defaultCheckSum = ~expectedCheckSum
        jtb.druidWebService.jsonResponse = {"[]"}
        expected = """{ "rows":[] }"""

        result = jtb.getHarness().target("data/shapes/day/color")
            .queryParam("metrics","width")
            .queryParam("dateTime","2014-06-10%2F2014-06-11")
            .request().get(String.class)

        then: "should be detected and result to a cache miss that will return the new druid result"
        GroovyTestUtils.compareJson(result, expected)

        when: "subsequent queries after change and first cache miss"
        expectedCheckSum = ~expectedCheckSum
        defaultCheckSum = expectedCheckSum
        jtb.druidWebService.jsonResponse = {"[]"}
        expected = """{ "rows":[] }"""

        result = jtb.getHarness().target("data/shapes/day/color")
            .queryParam("metrics","width")
            .queryParam("dateTime","2014-06-10%2F2014-06-11")
            .request().get(String.class)

        then: "should hit the cache"
        GroovyTestUtils.compareJson(result, expected)

        when: "force cache bypass"
        jtb.druidWebService.jsonResponse = {"[]"}

        result = jtb.getHarness().target("data/shapes/day/color")
            .queryParam("metrics","width")
            .queryParam("dateTime","2014-06-10%2F2014-06-11")
            .queryParam("_cache","FALSE")
            .request().get(String.class)

        then: "should read new result"
        GroovyTestUtils.compareJson(result, expected)
    }
}
