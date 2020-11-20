// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.cache


import static com.yahoo.bard.webservice.data.cache.MemDataCache.SYSTEM_CONFIG


import net.spy.memcached.MemcachedClient
import net.spy.memcached.internal.OperationFuture
import spock.lang.Specification

import java.util.concurrent.TimeoutException

class MemDataCacheSpec extends Specification {
    MemDataCache memDataCache;

    void setup() {
        SYSTEM_CONFIG.setProperty(MemDataCache.OPERATION_TIMEOUT_CONFIG_KEY, "12345")
        SYSTEM_CONFIG.setProperty(MemDataCache.EXPIRATION_KEY, "1234")
    }

    void cleanup() {
        SYSTEM_CONFIG.clearProperty(MemDataCache.OPERATION_TIMEOUT_CONFIG_KEY)
        SYSTEM_CONFIG.clearProperty(MemDataCache.EXPIRATION_KEY)
        SYSTEM_CONFIG.clearProperty(MemDataCache.WAIT_FOR_FUTURE)
    }

    def "Constructor has correct expiration and timeout"() {
        given: // will throw errors because no remote socket is open
        memDataCache = new MemDataCache();

        expect:
        memDataCache.client.operationTimeout == 12345
    }

    def "Set uses the configured expiration and doesn't block anymore"() {
        given:
        OperationFuture future = Mock(OperationFuture)
        MemcachedClient client = Mock(MemcachedClient)
        memDataCache = new MemDataCache(client);

        when:
        boolean result = memDataCache.set("key", "value")

        then:
        1 * client.set("key", 1234, "value") >> future
        0 * future.get()
        result
    }

    def "check #waitForFuture feature flag behaves correctly and doesn't wait for future when set to true"() {

        SYSTEM_CONFIG.setProperty(MemDataCache.WAIT_FOR_FUTURE, "false")
        OperationFuture future = Mock(OperationFuture)
        MemcachedClient client = Mock(MemcachedClient)
        memDataCache = new MemDataCache(client);
        when:
        boolean result = memDataCache.setInSeconds("key", "value",1000)

        then: "doesn't block anymore"
        1 * client.set("key", 1000, "value") >> future
        0 * future.get()
        result

        SYSTEM_CONFIG.setProperty(MemDataCache.WAIT_FOR_FUTURE, "true")
        when:
        boolean result1 = memDataCache.setInSeconds("key", "value",1000)

        then:
        1 * client.set("key", 1000, "value") >> {throw new TimeoutException()} >> future
        thrown(IllegalStateException)
        !result1

    }
}
