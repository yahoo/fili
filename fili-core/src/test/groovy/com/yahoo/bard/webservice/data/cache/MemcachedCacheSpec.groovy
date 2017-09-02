// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.cache

import static com.yahoo.bard.webservice.config.BardFeatureFlag.DRUID_CACHE
import static com.yahoo.bard.webservice.config.BardFeatureFlag.DRUID_CACHE_V2

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.table.availability.AvailabilityTestingUtils
import com.yahoo.bard.webservice.util.GroovyTestUtils
import com.yahoo.bard.webservice.web.endpoints.DataServlet

import org.joda.time.Interval

import net.spy.memcached.MemcachedClient
import spock.lang.IgnoreIf
import spock.lang.Specification
import spock.lang.Unroll

@IgnoreIf({!DRUID_CACHE.isOn()})
class MemcachedCacheSpec extends Specification {
    JerseyTestBinder jtb
    boolean cacheStatus
    boolean cacheV2Status

    def setup() {
        cacheStatus = DRUID_CACHE.isOn()
        cacheV2Status = DRUID_CACHE_V2.isOn()
        DRUID_CACHE.setOn(true)
        DRUID_CACHE_V2.setOn(false)

        // Create the test web container to test the resources
        jtb = new JerseyTestBinder(false, DataServlet.class)
        if (System.getenv("BUILD_NUMBER") != null) {
            // Only use real memcached client if in the CI environment
            jtb.dataCache = new HashDataCache<>(new MemDataCache<HashDataCache.Pair<String, String>>())
        }
        jtb.start()

        Interval interval = new Interval("2010-01-01/2500-12-31")
        AvailabilityTestingUtils.populatePhysicalTableCacheIntervals(jtb, interval)
    }

    def cleanup() {
        DRUID_CACHE.setOn(cacheStatus)
        DRUID_CACHE_V2.setOn(cacheV2Status)
        // Release the test web container
        assert jtb != null
        jtb.tearDown()
    }

    def "cache misses on error without invalidating"() {
        setup:  "Give the cache a client that will throw an error in the middle of hits"
        MemcachedClient client = Mock(MemcachedClient)
        HashDataCache.Pair<String, String> pair = new HashDataCache.Pair<String, String>("key", "value");

        client.get(_) >> pair >> { throw new RuntimeException() } >> pair
        HashDataCache cache = new HashDataCache<>(new MemDataCache<HashDataCache.Pair<String, String>>(client))

        when: "get a healthy object"
        String cacheValue = cache.get("key")

        then:
        cacheValue == pair.value

        when: "get an exception"
        cacheValue = cache.get("key")

        then: "return null (a cache miss)"
        cacheValue == null

        when: "get a good hit"
        cacheValue =  cache.get("key")

        then: "hits continue"
        cacheValue == pair.value
    }

    @Unroll
    def "cache set and get #key"() {
        when: "set"
        DataCache<String> cache = (DataCache<String>) jtb.dataCache
        cache.set(key, value)
        cache.set(key2, value2)

        then: "get"
        cache.get(key) == value
        cache.get(key2) == value2

        when: "update"
        cache.set(key, value2)
        cache.set(key2, value)

        then: "get"
        cache.get(key) == value2
        cache.get(key2) == value

        where:
        key          | key2         | value          | value2
        "key"        | "key2"       | "value"        | "value2"
        "key\ufe01@" | "key\ufe02@" | "value\ufe01#" | "value\ufe02#"
    }

    @Unroll
    def "cache throws #exception.simpleName because #reason"() {
        setup:
        DataCache<String> cache = (DataCache<String>) jtb.dataCache
        cache.set(key, value)

        when:
        cache.get(key)

        then:
        thrown(exception)

        where:
        key   | value   | exception | reason
        null  | null    | Exception | "cache key is null"
        null  | "value" | Exception | "cache key is null"
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

        when: "Change druid result"
        jtb.druidWebService.jsonResponse = {"[]"}

        result = jtb.getHarness().target("data/shapes/day/color")
            .queryParam("metrics","width")
            .queryParam("dateTime","2014-06-10%2F2014-06-11")
            .request().get(String.class)

        then: "should read old result from cache"
        GroovyTestUtils.compareJson(result, expected)

        when: "force cache bypass"
        jtb.druidWebService.jsonResponse = {"[]"}

        expected = """{ "rows":[] }"""


        result = jtb.getHarness().target("data/shapes/day/color")
            .queryParam("metrics","width")
            .queryParam("dateTime","2014-06-10%2F2014-06-11")
            .queryParam("_cache","FALSE")
            .request().get(String.class)

        then: "should read new result"
        GroovyTestUtils.compareJson(result, expected)
    }
}
